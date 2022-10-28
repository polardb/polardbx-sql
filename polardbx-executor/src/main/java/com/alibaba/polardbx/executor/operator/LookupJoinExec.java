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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.util.BufferInputBatchQueue;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.AbstractHashJoinExec.LIST_END;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

/**
 * Lookup Join (aka. BKA Join)
 * <p>
 * Note: outer 有可能比较复杂，inner 一定是view
 */
public class LookupJoinExec extends AbstractBufferedJoinExec implements ResumeExec {

    int batchSize;
    boolean innerIsOpen;
    LookupTableExec lookupTableExec;
    ResumeExec outerResumeExec;
    final ChunkConverter allOuterKeyChunkGetter;

    BufferInputBatchQueue batchQueue;
    MemoryAllocatorCtx bufferMemoryAllocator;
    Chunk savePopChunk;
    ListenableFuture<?> blocked;
    boolean isFinish;

    ConcurrentRawHashTable hashTable;
    int[] positionLinks;

    boolean beingConsumeOuter;

    boolean shouldSuspend;

    boolean outerNoMoreData;

    /**
     * 允许多条读连接流式执行
     */
    boolean allowMultiReadConnStreaming;

    public LookupJoinExec(Executor outerInput, Executor innerInput,
                          JoinRelType joinType, boolean maxOneRow,
                          List<EquiJoinKey> joinKeys, List<EquiJoinKey> allJoinKeys, IExpression otherCondition,
                          ExecutionContext context,
                          int shardCount,
                          int parallelism,
                          boolean allowMultiReadConnStreaming) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, null, null, context);
        getInnerLookupTableExec();
        getOuterExec();
        initBatchSize(shardCount, parallelism);

        this.blocked = ProducerExecutor.NOT_BLOCKED;

        DataType[] keyColumnTypes = allJoinKeys.stream().map(t -> t.getUnifiedType()).toArray(DataType[]::new);
        int[] outerKeyColumns = allJoinKeys.stream().mapToInt(t -> t.getOuterIndex()).toArray();
        this.allOuterKeyChunkGetter =
            Converters.createChunkConverter(
                outerInput.getDataTypes(), outerKeyColumns, keyColumnTypes, context);

        this.streamJoin = true;
        this.allowMultiReadConnStreaming = allowMultiReadConnStreaming;
    }

    /**
     * 初始化每批lookup in values的数量
     */
    private void initBatchSize(int shardCount, int parallelism) {
        parallelism = Math.max(parallelism, 1);
        int batchSize;
        int maxTotalLookupSize = context.getParamManager().getInt(ConnectionParams.LOOKUP_JOIN_MAX_BATCH_SIZE);
        if (lookupTableExec.shardEnabled() && shardCount > 0) {
            batchSize = shardCount *
                context.getParamManager().getInt(ConnectionParams.LOOKUP_JOIN_BLOCK_SIZE_PER_SHARD);
            // 防止裁剪后存在数据倾斜 设置上限
            batchSize = Math.min(maxTotalLookupSize, batchSize);
        } else {
            // 不能裁剪的情况根据分片数与并行度调整batchSize
            batchSize = context.getParamManager().getInt(ConnectionParams.JOIN_BLOCK_SIZE);
            if (shardCount > 0) {
                int maxPerShard = maxTotalLookupSize / (shardCount * parallelism);
                int minPerShard = context.getParamManager().getInt(ConnectionParams.LOOKUP_JOIN_MIN_BATCH_SIZE);
                batchSize = Math.min(batchSize, maxPerShard);
                batchSize = Math.max(batchSize, minPerShard);
            }
        }
        this.batchSize = batchSize;
    }

    private void getOuterExec() {
        Executor outerChild = outerInput;
        while (outerChild != null) {
            if (outerChild instanceof ResumeExec) {
                outerResumeExec = (ResumeExec) outerChild;
                break;
            } else if (outerChild instanceof ProjectExec || outerChild instanceof VectorizedProjectExec) {
                outerChild = outerChild.getInputs().get(0);
            } else {
                break;
            }
        }
    }

    private void getInnerLookupTableExec() {
        Executor innerChild = innerInput;
        while (innerChild != null) {
            if (innerChild instanceof LookupTableExec) {
                lookupTableExec = (LookupTableExec) innerChild;
                break;
            } else {
                innerChild = innerChild.getInputs().get(0);
            }
        }
    }

    @Override
    public void doOpen() {
        createBlockBuilders();
        this.memoryPool = MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        this.memoryAllocator = memoryPool.getMemoryAllocatorCtx();
        this.lookupTableExec.setMemoryAllocator(memoryAllocator);
        this.batchQueue = new BufferInputBatchQueue(batchSize, outerInput.getDataTypes(), memoryAllocator, context);
        this.beingConsumeOuter = true;
        this.outerInput.open();
    }

    protected boolean resumeAfterSuspend() {
        doSuspend();
        shouldSuspend = false;
        beingConsumeOuter = true;
        return resume();
    }

    @Override
    Chunk doNextChunk() {

        if (isFinish) {
            return null;
        } else if (shouldSuspend) {
            if (!resumeAfterSuspend()) {
                this.isFinish = true;
                return null;
            }
        }

        if (beingConsumeOuter) {
            Chunk outerChunk = outerInput.nextChunk();
            if (outerChunk != null) {
                batchQueue.addChunk(outerChunk);
                if (allowMultiReadConnStreaming && batchQueue.getTotalRowCount().get() > batchSize) {
                    // 流式情况下攒够一批数据就去lookup
                    beingConsumeOuter = false;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            } else {
                if (outerResumeExec != null) {
                    if (outerResumeExec.shouldSuspend()) {
                        outerResumeExec.doSuspend();
                        beingConsumeOuter = false;
                        blocked = ProducerExecutor.NOT_BLOCKED;
                    } else if (outerInput.produceIsFinished()) {
                        beingConsumeOuter = false;
                        blocked = ProducerExecutor.NOT_BLOCKED;
                    } else {
                        blocked = outerInput.produceIsBlocked();
                    }
                } else {
                    if (outerInput.produceIsFinished()) {
                        outerInput.close();
                        beingConsumeOuter = false;
                        blocked = ProducerExecutor.NOT_BLOCKED;
                    } else {
                        blocked = outerInput.produceIsBlocked();
                    }
                }
            }
            return null;
        } else {
            Chunk ret = super.doNextChunk();
            if (ret == null && outerNoMoreData) {
                if (outerResumeExec != null) {
                    //complete the look-up stage.
                    shouldSuspend = true;
                } else {
                    isFinish = true;
                }
            }
            return ret;
        }
    }

    @Override
    public boolean resume() {
        outerNoMoreData = false;
        this.batchQueue = new BufferInputBatchQueue(batchSize, outerInput.getDataTypes(), memoryAllocator, context);
        this.hashTable = null;
        this.positionLinks = null;
        this.beingConsumeOuter = true;
        if (bufferMemoryAllocator != null) {
            this.bufferMemoryAllocator.releaseReservedMemory(bufferMemoryAllocator.getReservedAllocated(), true);
        }
        if (outerResumeExec instanceof TableScanExec) {
            return outerResumeExec.resume();
        } else {
            return !outerInput.produceIsFinished();
        }
    }

    protected LookupTableExec getLookupTableExec() {
        return lookupTableExec;
    }

    @Override
    public boolean shouldSuspend() {
        return shouldSuspend;
    }

    @Override
    public void doSuspend() {
        getLookupTableExec().doSuspend();
    }

    @Override
    Chunk nextProbeChunk() {
        if (this.savePopChunk == null) {
            buildChunks = new ChunksIndex();
            buildKeyChunks = new ChunksIndex();
            savePopChunk = batchQueue.pop();
            if (savePopChunk != null) {
                getLookupTableExec().releaseConditionMemory();
                Chunk allJoinKeys = allOuterKeyChunkGetter.apply(savePopChunk);
                getLookupTableExec().updateLookupPredicate(allJoinKeys);
                if (innerIsOpen) {
                    getLookupTableExec().resume();
                } else {
                    innerInput.open();
                    innerIsOpen = true;
                }
            } else {
                // 再尝试拉取outer数据
                if (allowMultiReadConnStreaming && !beingConsumeOuter && !outerInput.produceIsFinished()) {
                    beingConsumeOuter = true;
                    return null;
                }
                outerNoMoreData = true;
                return null;
            }
        }
        Chunk result = innerInput.nextChunk();
        if (result != null) {
            buildChunks.addChunk(result);
            buildKeyChunks.addChunk(innerKeyChunkGetter.apply(result));
            this.blocked = ProducerExecutor.NOT_BLOCKED;
            return null;
        } else {
            if (getLookupTableExec().produceIsFinished()) {
                if (bufferMemoryAllocator != null) {
                    bufferMemoryAllocator.releaseReservedMemory(bufferMemoryAllocator.getReservedAllocated(), true);
                } else {
                    bufferMemoryAllocator = memoryPool.getMemoryAllocatorCtx();
                }
                buildHashTable();
                // Allocate memory for hash-table
                bufferMemoryAllocator.allocateReservedMemory(hashTable.estimateSize());
                bufferMemoryAllocator.allocateReservedMemory(SizeOf.sizeOf(positionLinks));
                Chunk ret = savePopChunk;
                this.savePopChunk = null;
                this.blocked = ProducerExecutor.NOT_BLOCKED;
                return ret;
            } else {
                this.blocked = getLookupTableExec().produceIsBlocked();
                return null;
            }
        }
    }

    void buildHashTable() {
        final int size = buildKeyChunks.getPositionCount();
        hashTable = new ConcurrentRawHashTable(size);
        positionLinks = new int[size];
        Arrays.fill(positionLinks, LIST_END);
        int position = 0;
        for (int chunkId = 0; chunkId < buildKeyChunks.getChunkCount(); ++chunkId) {
            final Chunk keyChunk = buildKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks, null, getIgnoreNullsInJoinKey());
            position += keyChunk.getPositionCount();
        }
        assert position == size;
    }

    @Override
    int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
        int hashCode = hashCodes[position];

        int matchedPosition = hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    boolean matchValid(int current) {
        return current != LIST_END;
    }

    @Override
    void doClose() {
        // Hash Join
        hashTable = null;
        positionLinks = null;
        buildChunks = null;
        buildKeyChunks = null;

        innerInput.close();
        outerInput.close();
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
            memoryPool = null;
        }
        batchQueue = null;
        bufferMemoryAllocator = null;
        isFinish = true;
        memoryPool = null;
        savePopChunk = null;
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}
