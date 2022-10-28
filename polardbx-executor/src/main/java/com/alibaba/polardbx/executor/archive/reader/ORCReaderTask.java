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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.google.common.collect.Range;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ORCReaderTask {

    private OSSReadOption ossReadOption;
    public String tableFileName;

    private RecordReader recordReader;

    private FileSystem fileSystem;
    private Reader reader;
    private URI ossFileUri;

    private OSSOrcFileMeta fileMeta;

    private PruningResult pruningResult;

    private List<AggregateCall> aggCalls;

    private List<RelColumnOrigin> aggColumns;

    private ListIterator<Range<Long>> listIterator;

    private Configuration configuration;

    private final AtomicBoolean closed;

    private volatile long stamp;

    private ExecutionContext context;

    private long startTime;

    private long count;

    private Iterator<Long> indexIterator;

    private long index;

    private Iterator<Chunk> chunkIterator;

    List<DataType> dataTypeList;

    public ORCReaderTask(OSSReadOption ossReadOption, String tableFileName, FileMeta fileMeta,
                         PruningResult pruningResult, List<AggregateCall> aggCalls, List<RelColumnOrigin> aggColumns,
                         List<DataType> dataTypes, ExecutionContext context) {
        this.ossReadOption = ossReadOption;
        this.tableFileName = tableFileName;
        this.closed = new AtomicBoolean(false);
        this.stamp = FileSystemManager.readLockWithTimeOut(ossReadOption.getEngine());
        this.fileSystem = FileSystemManager.getFileSystemGroup(ossReadOption.getEngine()).getMaster();
        String orcPath = FileSystemUtils.buildUri(this.fileSystem, tableFileName);
        this.ossFileUri = URI.create(orcPath);
        this.fileMeta = (OSSOrcFileMeta) fileMeta;
        this.pruningResult = pruningResult;
        this.aggCalls = aggCalls;
        this.aggColumns = aggColumns;
        this.configuration = new Configuration(false);
        this.configuration.setLong(OrcConf.MAX_MERGE_DISTANCE.getAttribute(), ossReadOption.getMaxMergeDistance());
        this.count = 0;
        this.index = 0;
        this.indexIterator = null;
        this.dataTypeList = dataTypes;
        this.context = context;
    }

    public void init() {
        try {
            startTime = System.nanoTime() / 1000_000;

            if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_BUFFER_POOL)) {
                String[] columns = new String[ossReadOption.getColumnMetas().size()];
                for (int i = 0; i < columns.length; i++) {
                    columns[i] = ossReadOption.getColumnMetas().get(i).getName();
                }
                if (withAgg() && pruningResult.pass()) {
                    return;
                }
                List<Chunk> chunkList = BufferPoolManager.getInstance().get(fileMeta, columns, ossReadOption, context);
                chunkIterator = chunkList.iterator();
                return;
            }

            // fetch file footer
            this.reader = OrcFile.createReader(new Path(ossFileUri),
                OrcFile.readerOptions(configuration).filesystem(fileSystem).orcTail(fileMeta.getOrcTail()));

            if (withAgg() && pruningResult.pass()) {
                closeRecordReader();
                return;
            }
            // reader filter options
            Reader.Options readerOptions = createOption();

            if (pruningResult.pass()) {
                this.recordReader = reader.rows(readerOptions);
            }
            if (pruningResult.part()) {
                if (withAgg()) {
                    indexIterator = pruningResult.getStripeMap().keySet().stream().sorted(Long::compareTo)
                        .collect(Collectors.toList())
                        .listIterator();
                    index = indexIterator.next();
                    if (!pruningResult.stat(index)) {
                        StripeColumnMeta stripeColumnMeta = pruningResult.getStripeMap().get(index);
                        readerOptions =
                            readerOptions.range(stripeColumnMeta.getStripeOffset(), stripeColumnMeta.getStripeLength());
                        this.recordReader = reader.rows(readerOptions);
                    } else {
                        closeRecordReader();
                    }
                } else {
                    Iterator<Range<Long>> descendingIterator =
                        pruningResult.getRangeSet().asDescendingSetOfRanges().iterator();
                    List<Range<Long>> rangeList = new ArrayList<>();
                    while (descendingIterator.hasNext()) {
                        rangeList.add(descendingIterator.next());
                    }

                    // sequential access file
                    listIterator = rangeList.listIterator(rangeList.size());
                    Range<Long> range = listIterator.previous();
                    readerOptions =
                        readerOptions.range(range.lowerEndpoint(), range.upperEndpoint() - range.lowerEndpoint());
                    this.recordReader = reader.rows(readerOptions);
                }
            }

        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    public Chunk nextFromBufferPool(SessionProperties sessionProperties) {
        if (chunkIterator == null) {
            return fetchStatistics(sessionProperties).getChunk();
        }
        if (chunkIterator.hasNext()) {
            return chunkIterator.next();
        } else {
            return null;
        }
    }

    /**
     * When enable buffer pool, the result chunk still may come from statistics. The function checks whether
     * we use buffer pool or statistics
     *
     * @return true if it does come from buffer pool
     */
    public boolean comesFromBufferPool() {
        return chunkIterator != null;
    }

    private void fetchStatistics(BlockBuilder[] blockBuilders, SessionProperties sessionProperties, int colIndex,
                                 SqlKind kind) {
        if (kind == SqlKind.COUNT) {
            blockBuilders[colIndex] = BlockBuilders.create(dataTypeList.get(colIndex), context, 1);
            // No need to orc column statistics for count agg
            blockBuilders[colIndex].writeLong(fileMeta.getTableRows());
            index = -1;
        } else {
            RelColumnOrigin columnOrigin = aggColumns.get(colIndex);

            // prepare the block builder (precise data type)
            ColumnMeta columnMeta = fileMeta.getColumnMetaMap().get(columnOrigin.getColumnName());
            DataType aggResultType = DataTypeUtil.aggResultTypeOf(columnMeta.getDataType(), kind);
            blockBuilders[colIndex] = BlockBuilders.create(aggResultType, context, 1);

            ColumnStatistics statistics;
            if (pruningResult.pass()) {
                statistics = fileMeta.getStatisticsMap().get(columnOrigin.getColumnName());
            } else {
                statistics = fileMeta.getStripeColumnMetas(columnOrigin.getColumnName())
                    .get(index).getColumnStatistics();
            }
            ColumnProvider<?> columnProvider = ColumnProviders.getProvider(columnMeta);

            // fetch type-specific handle
            columnProvider.fetchStatistics(
                statistics, kind, blockBuilders[colIndex], aggResultType, sessionProperties
            );
        }
    }

    public ORCReadResult fetchStatistics(SessionProperties sessionProperties) {
        int resultRows = 0;
        long s = System.currentTimeMillis();
        BlockBuilder[] blockBuilders = null;
        if (index != -1) {
            int colIndex = 0;
            blockBuilders = new BlockBuilder[aggCalls.size()];
            for (AggregateCall call : aggCalls) {
                // for each agg functions, fetch the statistics from orc && write the result to block builder.
                SqlKind kind = call.getAggregation().getKind();
                fetchStatistics(blockBuilders, sessionProperties, colIndex, kind);
                colIndex++;
            }
            resultRows++;
            // move to next stripe for non-count agg.
            nextStripe();
        }

        count += resultRows;
        return new ORCReadResult(
            tableFileName,
            System.currentTimeMillis() - s,
            resultRows,
            fileSystem,
            blockBuilders == null ? null : new Chunk(blocksFrom(blockBuilders)));
    }

    @NotNull
    private Block[] blocksFrom(BlockBuilder[] blockBuilders) {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        return blocks;
    }

    public ORCReadResult next(VectorizedRowBatch buffer, SessionProperties sessionProperties) {
        try {
            // use statistics
            if (recordReader == null) {
                return fetchStatistics(sessionProperties);
            }

            // read orc file
            buffer.size = 0;
            long s = System.currentTimeMillis();
            long resultRows = 0;

            if (this.recordReader.nextBatch(buffer)) {
                resultRows += buffer.size;
            }

            if (resultRows == 0) {
                if (pruningResult.part()) {
                    if (nextStripe()) {
                        return next(buffer, sessionProperties);
                    }
                }
            }
            count += resultRows;
            ORCReadResult readResult = new ORCReadResult(
                tableFileName,
                System.currentTimeMillis() - s,
                resultRows,
                fileSystem
            );
            return readResult;
        } catch (Throwable t) {
            close();
            throw GeneralUtil.nestedException(t);
        }
    }

    /**
     * get the next stripe in the file
     *
     * @return true if there is more stripe to read
     */
    private boolean nextStripe() {
        // close
        closeRecordReader();
        // for pass, there is only one stripe: the whole file
        if (pruningResult.pass()) {
            index = -1L;
            return false;
        }
        if (withAgg()) {
            index = indexIterator.hasNext() ? indexIterator.next() : -1;
            // end of stripes
            if (index == -1) {
                return false;
            }
            if (!pruningResult.stat(index)) {
                StripeColumnMeta stripeColumnMeta = pruningResult.getStripeMap().get(index);
                Reader.Options readerOptions = createOption();
                readerOptions.range(stripeColumnMeta.getStripeOffset(), stripeColumnMeta.getStripeLength());
                try {
                    this.recordReader = reader.rows(readerOptions);
                } catch (Throwable e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
            return true;
        } else {
            if (listIterator.hasPrevious()) {
                Range<Long> range = listIterator.previous();
                // reader filter options
                Reader.Options readerOptions = createOption()
                    .range(range.lowerEndpoint(), range.upperEndpoint() - range.lowerEndpoint());
                try {
                    this.recordReader = reader.rows(readerOptions);
                } catch (Throwable e) {
                    throw GeneralUtil.nestedException(e);
                }
                return true;
            }
        }
        return false;
    }

    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (context.isEnableTrace()) {
                long now = System.nanoTime() / 1000_000;
                ExecuteSQLOperation op = new ExecuteSQLOperation(
                    fileMeta.getPhysicalTableSchema(),
                    tableFileName,
                    "pruning result:" + pruningResult.toString() + " predicate :" + ossReadOption.getSearchArgument()
                        .toString(),
                    now);
                // Update trace time.
                op.setThreadName(Thread.currentThread().getName());
                op.setTimeCost(now - startTime);
                op.setGetConnectionTimeCost(0);
                op.setRowsCount(count);
                op.setTotalTimeCost(now - op.getTimestamp());
                op.setPhysicalCloseCost(0);
                context.getTracer().trace(op);
            }

            if (this.recordReader != null) {
                this.recordReader.close();
                this.recordReader = null;
            }
            if (this.reader != null) {
                this.reader.close();
                this.reader = null;
            }

            this.chunkIterator = null;
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            FileSystemManager.unlockRead(ossReadOption.getEngine(), stamp);
        }
    }

    /**
     * whether using column statistics instead of reading the real orc file
     *
     * @return true if we can skip
     */
    public boolean pass() {
        return recordReader == null;
    }

    private boolean withAgg() {
        return aggCalls != null;
    }

    private void closeRecordReader() {
        try {
            if (this.recordReader != null) {
                this.recordReader.close();
                this.recordReader = null;
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private Reader.Options createOption() {
        return new Reader.Options(configuration)
            .schema(this.ossReadOption.getReadSchema())
            .searchArgument(
                ossReadOption.getSearchArgument(),
                ossReadOption.getColumns()
            );
    }
}
