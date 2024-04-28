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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class OSSPhysicalTableReadResult extends SimpleOSSPhysicalTableReadResult {
    private List<ORCReaderTask> orcReaderTaskList;
    private OSSReadOption readOption;
    private VectorizedRowBatch buffer;
    private int chunkLimit;
    private ExecutionContext context;

    private int taskIndex;
    private volatile boolean isFinished;
    private TimeZone timeZone;

    public OSSPhysicalTableReadResult(OSSReadOption readOption, ExecutionContext executionContext,
                                      List<AggregateCall> aggCalls, List<RelColumnOrigin> aggColumns,
                                      ImmutableBitSet groupSet, RelDataType dataType) {
        super();
        List<ORCReaderTask> taskList = new ArrayList<>();
        for (int i = 0; i < readOption.getTableFileList().size(); i++) {
            String tableFile = readOption.getTableFileList().get(i);
            FileMeta fileMeta = readOption.getPhyTableFileMetas().get(i);
            PruningResult pruningResult = readOption.getPruningResultList().get(i);

            ORCReaderTask task = new ORCReaderTask(readOption, tableFile, fileMeta, pruningResult,
                aggCalls, aggColumns, dataType == null ? null : CalciteUtils.getTypes(dataType), executionContext);
            taskList.add(task);
        }

        this.orcReaderTaskList = taskList;
        this.readOption = readOption;
        this.context = executionContext;

        this.chunkLimit = (int) context.getParamManager().getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE);
        this.buffer = this.readOption.getReadSchema().createRowBatch(chunkLimit);

        this.taskIndex = 0;
        this.isFinished = false;

        this.columnProviders = readOption.getOssColumnTransformer().getTargetColumnProvides();
        this.sessionProperties = SessionProperties.fromExecutionContext(context);
        this.timeZone = TimestampUtils.getTimeZone(context);
    }

    public void init() {
        for (ORCReaderTask task : orcReaderTaskList) {
            task.init();
        }
    }

    public Chunk next(List<DataType<?>> inProjectDataTypeList, BlockBuilder[] blockBuilders,
                      VectorizedExpression condition, MutableChunk preAllocatedChunk, int[] filterBitmap,
                      int[] outProject, ExecutionContext context) {
        for (; taskIndex < orcReaderTaskList.size(); taskIndex++) {
            while (true) {
                ORCReaderTask task = orcReaderTaskList.get(taskIndex);

                // get the next stripe
                ORCReadResult readResult = task.next(buffer, sessionProperties);
                // the stripe uses statistics
                if (readResult.isStatistics()) {
                    return readResult.getChunk();
                }
                if ((readResult.getResultRows()) == 0) {
                    // nothing to read, go next orc read task.
                    task.close();
                    break;
                }

                Chunk result = next(buffer,
                    readOption.getOssColumnTransformer(),
                    inProjectDataTypeList,
                    blockBuilders,
                    condition,
                    preAllocatedChunk,
                    filterBitmap,
                    outProject,
                    context, 0, null);
                if (result == null) {
                    continue;
                }
                return result;
            }
        }
        this.isFinished = true;
        return null;
    }

    public Chunk nextChunkFromBufferPool(List<DataType<?>> inProjectDataTypeList, BlockBuilder[] blockBuilders,
                                         VectorizedExpression condition, MutableChunk preAllocatedChunk,
                                         int[] filterBitmap,
                                         int[] outProject, ExecutionContext context) {
        boolean delayMaterialization =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_DELAY_MATERIALIZATION);
        boolean zeroCopy =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_ZERO_COPY);
        boolean compatible =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        boolean useSelection =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_SELECTION);

        long resultRows;
        RandomAccessBlock[] blocksForCompute = new RandomAccessBlock[filterBitmap.length];

        for (; taskIndex < orcReaderTaskList.size(); taskIndex++) {
            while (true) {
                ORCReaderTask task = orcReaderTaskList.get(taskIndex);
                Chunk chunk = task.nextFromBufferPool(sessionProperties);
                if (chunk == null) {
                    // nothing to read, go next orc read task.
                    task.close();
                    break;
                } else {
                    resultRows = chunk.getPositionCount();
                }
                if (!task.comesFromBufferPool()) {
                    return chunk;
                }

                int inProjectCount = inProjectDataTypeList.size();
                // make block for pre-filter
                if (zeroCopy) {
                    for (int i = 0; i < inProjectCount; i++) {
                        if (filterBitmap[i] == 1) {
                            blocksForCompute[i] = (RandomAccessBlock) chunk.getBlock(i);
                        }
                    }
                } else {
                    // make block for pre-filter
                    for (int i = 0; i < inProjectCount; i++) {
                        if (filterBitmap[i] == 1) {
                            DataType dataType = inProjectDataTypeList.get(i);
                            BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);

                            Block cachedBlock = chunk.getBlock(i);

                            for (int j = 0; j < chunk.getPositionCount(); j++) {
                                cachedBlock.writePositionTo(j, blockBuilder);
                            }

                            blocksForCompute[i] = (RandomAccessBlock) blockBuilder.build();
                        }
                    }
                }

                // pre-filter
                Pair<Integer, int[]> sel =
                    preFilter(condition, preAllocatedChunk, filterBitmap, context,
                        (int) resultRows, blocksForCompute, inProjectCount);

                int selSize = sel.getKey();
                int[] selection = sel.getValue();
                if (selSize == 0) {
                    continue;
                }

                // buffer to block builders
                Block[] blocks = new Block[outProject.length];
                for (int i = 0; i < outProject.length; i++) {
                    DataType dataType = inProjectDataTypeList.get(outProject[i]);
                    BlockBuilder blockBuilder = BlockBuilders.create(dataType, context);
                    Block cachedBlock = chunk.getBlock(outProject[i]);

                    if (filterBitmap[outProject[i]] == 1 && selSize == chunkLimit) {
                        // case 1. use block from bitmap
                        blocks[i] = (Block) blocksForCompute[outProject[i]];
                    } else if (selSize == resultRows && zeroCopy) {
                        // case 2. use block from cacheBlock
                        blocks[i] = cachedBlock;
                    } else if (delayMaterialization && cachedBlock instanceof DecimalBlock) {

                        // case 3. decimal block delay materialization
                        DecimalBlock decimalBlock = cachedBlock.cast(DecimalBlock.class);
                        blocks[i] = DecimalBlock.from(decimalBlock, selSize, selection, useSelection);

                    } else if (delayMaterialization && cachedBlock instanceof SliceBlock) {
                        // case 4. slice block delay materialization
                        blocks[i] = SliceBlock.from(cachedBlock.cast(SliceBlock.class), selSize, selection, compatible,
                            useSelection);

                    } else if (delayMaterialization && cachedBlock instanceof DateBlock) {
                        // case 5. date block delay materialization
                        DateBlock dateBlock = cachedBlock.cast(DateBlock.class);
                        blocks[i] = DateBlock.from(dateBlock, selSize, selection, useSelection);

                    } else if (delayMaterialization && cachedBlock instanceof TimestampBlock) {
                        // case 5. date block delay materialization
                        TimestampBlock timestampBlock = cachedBlock.cast(TimestampBlock.class);
                        blocks[i] = TimestampBlock.from(timestampBlock, selSize, selection, useSelection, timeZone);

                    } else if (delayMaterialization && cachedBlock instanceof IntegerBlock) {

                        // case 6. integer block delay materialization
                        IntegerBlock integerBlock = cachedBlock.cast(IntegerBlock.class);
                        blocks[i] = IntegerBlock.from(integerBlock, selSize, selection, useSelection);

                    } else {
                        // case 7. normal
                        for (int j = 0; j < selSize; j++) {
                            int idx = selection[j];
                            cachedBlock.writePositionTo(idx, blockBuilder);
                        }
                        blocks[i] = blockBuilder.build();
                    }
                }

                // partial agg for buffer pool
                if (withAgg() && task.comesFromBufferPool()) {
                    return aggExec(new Chunk(blocks), inProjectDataTypeList, outProject, context);
                }
                return new Chunk(blocks);
            }
        }

        this.isFinished = true;
        return null;
    }

    public Chunk next(List<DataType<?>> inProjectDataTypeList, BlockBuilder[] blockBuilders) {
        for (; taskIndex < orcReaderTaskList.size(); taskIndex++) {
            ORCReaderTask task = orcReaderTaskList.get(taskIndex);

            ORCReadResult readResult = task.next(buffer, sessionProperties);
            if ((readResult.getResultRows()) == 0) {
                // nothing to read, go next orc read task.
                task.close();
                continue;
            }
            if (readResult.isStatistics()) {
                return readResult.getChunk();
            }

            Chunk result = next(buffer, readOption.getOssColumnTransformer(),
                inProjectDataTypeList, blockBuilders, context, 0, null);
            if (result == null) {
                continue;
            }
            return result;
        }

        this.isFinished = true;
        return null;
    }

    public Chunk nextChunkFromBufferPool(BlockBuilder[] blockBuilders, SessionProperties sessionProperties,
                                         List<DataType<?>> inProjectDataTypeList, int[] outProject) {
        final int columns = blockBuilders.length;
        boolean delayMaterialization =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_DELAY_MATERIALIZATION);
        for (; taskIndex < orcReaderTaskList.size(); taskIndex++) {
            ORCReaderTask task = orcReaderTaskList.get(taskIndex);
            Chunk chunk = task.nextFromBufferPool(sessionProperties);
            if (chunk == null) {
                // nothing to read, go next orc read task.
                task.close();
                continue;
            }
            if (!task.comesFromBufferPool()) {
                return chunk;
            }

            // partial agg for chunk from buffer pool
            if (withAgg()) {
                return aggExec(chunk, inProjectDataTypeList, outProject, context);
            }
            // buffer to block builders
            Block[] blocks = new Block[columns];
            if (delayMaterialization) {
                for (int i = 0; i < columns; i++) {
                    blocks[i] = chunk.getBlock(i);
                }
            } else {
                // buffer to block builders
                for (int i = 0; i < blockBuilders.length; i++) {
                    BlockBuilder blockBuilder = blockBuilders[i];
                    for (int j = 0; j < chunk.getPositionCount(); j++) {
                        chunk.getBlock(i).writePositionTo(j, blockBuilder);
                    }
                    blocks[i] = blockBuilder.build();
                }
            }
            return new Chunk(blocks);
        }

        this.isFinished = true;
        return null;
    }

    /**
     * check whether the tableScan has aggregators
     *
     * @return true if it has aggregators
     */
    private boolean withAgg() {
        return aggCalls != null;
    }

    public void close() {
        for (ORCReaderTask task : orcReaderTaskList) {
            task.close();
        }
    }

    public boolean isFinished() {
        return this.isFinished;
    }
}
