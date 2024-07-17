/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.archive.pruning.AggPruningResult;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.archive.schemaevolution.OrcColumnManager;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ORCReaderWithAggTask extends UnPushableORCReaderTask {

    // used for statistics

    private AggPruningResult aggPruningResult;
    private Iterator<Long> indexIterator;
    private long index;
    private List<DataType> dataTypeList;
    private List<AggregateCall> aggCalls;
    private List<RelColumnOrigin> aggColumns;

    public ORCReaderWithAggTask(OSSReadOption ossReadOption, String tableFileName, FileMeta fileMeta,
                                PruningResult pruningResult, ExecutionContext context,
                                List<DataType> dataTypeList, List<AggregateCall> aggCalls,
                                List<RelColumnOrigin> aggColumns) {
        super(ossReadOption, tableFileName, fileMeta, pruningResult, context);
        Preconditions.checkArgument(aggCalls != null, "ORCReaderWithAggTask must have agg");
        this.dataTypeList = dataTypeList;
        this.aggCalls = aggCalls;
        this.aggColumns = aggColumns;
        this.aggPruningResult = (AggPruningResult) pruningResult;
    }

    public void init() {
        try {
            startTime = System.nanoTime() / 1000_000;
            // fetch file footer
            this.reader = OrcFile.createReader(new Path(ossFileUri),
                OrcFile.readerOptions(configuration).filesystem(fileSystem).orcTail(fileMeta.getOrcTail()));

            if (aggPruningResult.noscan()) {
                closeRecordReader();
                return;
            }

            // reader filter options
            Reader.Options readerOptions = createOption();
            if (aggPruningResult.pass()) {
                this.recordReader = reader.rows(readerOptions);
                return;
            }
            if (aggPruningResult.part()) {
                indexIterator = aggPruningResult.getStripeMap().keySet().stream().sorted(Long::compareTo).collect(
                        Collectors.toList())
                    .listIterator();
                index = indexIterator.next();
                if (!aggPruningResult.stat(index)) {
                    StripeColumnMeta stripeColumnMeta = aggPruningResult.getStripeMap().get(index);
                    readerOptions =
                        readerOptions.range(stripeColumnMeta.getStripeOffset(), stripeColumnMeta.getStripeLength());
                    this.recordReader = reader.rows(readerOptions);
                } else {
                    closeRecordReader();
                }
            }

        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    public ORCReadResult next(VectorizedRowBatch buffer, SessionProperties sessionProperties) {
        try {
            // Use statistics
            if (recordReader == null) {
                return fetchStatistics(sessionProperties);
            }
            return super.next(buffer, sessionProperties);
        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    private ORCReadResult fetchStatistics(SessionProperties sessionProperties) {
        int resultRows = 0;
        ORCReadResult readResult = null;
        long s = System.currentTimeMillis();
        if (index != -1) {
            BlockBuilder[] blockBuilders = new BlockBuilder[aggCalls.size()];
            int colIndex = 0;
            for (AggregateCall call : aggCalls) {
                // for each agg functions, fetch the statistics from orc && write the result to block builder.
                SqlKind kind = call.getAggregation().getKind();
                blockBuilders[colIndex] = fetchStatistics(sessionProperties, colIndex, kind);
                colIndex++;
            }
            // move to next stripe
            nextStripe();
            // build the result chunk
            Block[] blocks = blocksFrom(blockBuilders);
            readResult = new ORCReadResult(
                tableFileName,
                System.currentTimeMillis() - s,
                ++resultRows,
                fileSystem,
                new Chunk(blocks)
            );
        }

        if (readResult == null) {
            readResult = new ORCReadResult(
                tableFileName,
                System.currentTimeMillis() - s,
                resultRows,
                fileSystem
            );
        }
        count += resultRows;
        return readResult;
    }

    @NotNull
    private Block[] blocksFrom(BlockBuilder[] blockBuilders) {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return blocks;
    }

    private BlockBuilder fetchStatistics(SessionProperties sessionProperties, int colIndex,
                                         SqlKind kind) {
        BlockBuilder blockBuilder;
        if (kind == SqlKind.COUNT) {
            // imprecise data type
            blockBuilder = BlockBuilders.create(dataTypeList.get(colIndex), context, 1);

            RelColumnOrigin columnOrigin = aggColumns.get(colIndex);
            if (columnOrigin != null) {
                OSSColumnTransformer ossColumnTransformer = ossReadOption.getOssColumnTransformer();
                TypeComparison ossColumnCompare =
                    ossColumnTransformer.compare(columnOrigin.getColumnName());
                // fill in default value
                if (TypeComparison.isMissing(ossColumnCompare)) {
                    ColumnMeta targetColumnMeta =
                        ossColumnTransformer.getTargetColumnMeta(columnOrigin.getColumnName());
                    blockBuilder.writeLong(
                        targetColumnMeta.getField().getDefault() == null ? 0 : fileMeta.getTableRows());
                    index = -1;
                    return blockBuilder;
                }
            }
            // No need to orc column statistics for count agg
            blockBuilder.writeLong(fileMeta.getTableRows());
            index = -1;
            return blockBuilder;
        }
        if (kind == SqlKind.CHECK_SUM) {
            // imprecise data type
            blockBuilder = BlockBuilders.create(dataTypeList.get(colIndex), context, 1);
            // No need to orc column statistics for checksum agg
            blockBuilder.writeLong(fileMeta.getFileHash());
            // check the existence of files
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(fileMeta.getEngine());
            Preconditions.checkArgument(fileSystemGroup != null);
            try {
                if (!fileSystemGroup.exists(fileMeta.getFileName(), context.getFinalPlan().isUseColumnar())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS,
                        "File " + fileMeta.getFileName() + " doesn't exits");
                }
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
            index = -1;
            return blockBuilder;
        }
        {
            // sum/min/max
            RelColumnOrigin columnOrigin = aggColumns.get(colIndex);

            OSSColumnTransformer ossColumnTransformer = ossReadOption.getOssColumnTransformer();
            TypeComparison ossColumnCompare =
                ossColumnTransformer.compare(columnOrigin.getColumnName());
            if (TypeComparison.isMissing(ossColumnCompare)) {
                Preconditions.checkArgument(kind != SqlKind.SUM,
                    "currently missing column can't handle sum");
            }
            int rank = ossColumnTransformer.getTargetColumnRank(columnOrigin.getColumnName());
            ColumnMeta sourceColumnMeta = ossColumnTransformer.getSourceColumnMeta(rank);
            ColumnMeta targetColumnMeta = ossColumnTransformer.getTargetColumnMeta(rank);

            // prepare the block builder (precise data type)
            TableMeta tm = fileMeta.getTableMeta(context);
            String fieldId = tm.getColumnFieldId(columnOrigin.getColumnName());
            Preconditions.checkArgument(fieldId != null, "fix the case");
            ColumnMeta columnMeta;
            if (tm.isOldFileStorage()) {
                columnMeta = tm.getColumn(columnOrigin.getColumnName());
            } else {
                columnMeta = OrcColumnManager.getHistory(fieldId, fileMeta);
            }

            DataType aggResultType = DataTypeUtil.aggResultTypeOf(columnMeta.getDataType(), kind);
            blockBuilder = BlockBuilders.create(aggResultType, context, 1);

            ColumnStatistics statistics;
            if (aggPruningResult.noscan()) {
                statistics = fileMeta.getStatisticsMap().get(fieldId);
            } else {
                statistics = fileMeta.getStripeColumnMetas(fieldId)
                    .get(index).getColumnStatistics();
            }

            ColumnProvider<?> columnProvider = ColumnProviders.getProvider(columnMeta);

            // fetch type-specific handle
            columnProvider.fetchStatistics(
                statistics, kind, blockBuilder, aggResultType, sessionProperties
            );
            return blockBuilder;
        }
    }

    /**
     * get the next stripe in the file
     *
     * @return true if there is more stripe to read
     */
    @Override
    protected boolean nextStripe() {
        // close
        closeRecordReader();
        // for noscan, there is only one stripe: the whole file
        if (aggPruningResult.noscan() || aggPruningResult.pass()) {
            index = -1L;
            return false;
        }
        index = indexIterator.hasNext() ? indexIterator.next() : -1;
        // end of stripes
        if (index == -1) {
            return false;
        }
        if (!aggPruningResult.stat(index)) {
            StripeColumnMeta stripeColumnMeta = aggPruningResult.getStripeMap().get(index);
            Reader.Options readerOptions = createOption();
            readerOptions.range(stripeColumnMeta.getStripeOffset(), stripeColumnMeta.getStripeLength());
            try {
                this.recordReader = reader.rows(readerOptions);
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
        return true;
    }
}
