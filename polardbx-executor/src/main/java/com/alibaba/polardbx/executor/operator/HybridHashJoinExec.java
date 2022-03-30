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
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.LocalBucketPartitionFunction;
import com.alibaba.polardbx.executor.mpp.operator.PartitionFunction;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SingleStreamSpiller;
import com.alibaba.polardbx.executor.operator.spill.SingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.common.utils.bloomfilter.FastIntBloomFilter;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.alibaba.polardbx.executor.operator.AbstractHashJoinExec.LIST_END;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

// TODO: spillCnt应该是一直使用最外层的，但现在迭代生成新的spillCnt，统计失效。
public class HybridHashJoinExec extends AbstractJoinExec implements MemoryRevoker, ConsumerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HybridHashJoinExec.class);

    //FIXME: anti Join with null key value unexpected, need get information from Smp exchanger/Mpp Sync
    // Special flag for anti Join, for all logical build inner, should include in other driver or MppNode.
    // but not;
    static class AntiJoinContext {
        public boolean innerHasNull = false;
        public boolean innerIsEmpty = true;

        void consumeChunk(Chunk keyInputChunk) {
            innerIsEmpty = false;
            if (innerHasNull || checkContainsNull(keyInputChunk)) {
                if (keyInputChunk.getBlockCount() != 1) {
                    throw new UnsupportedOperationException(
                        getClass().getName() + "not support anti-join with multi key columns containing Null value");
                }
                innerHasNull = true;
                return;
            }
        }
    }

    ;
    AntiJoinContext antiJoinContext;

    public enum BucketState {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT(0),

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. all buckets of Operator accepts input and spills it
         */
        SPILLING_INPUT(1),

        /**
         * Partial LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT(2),

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED(3),

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING(4),

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT(5),

        /**
         * No longer needed
         */
        DISPOSED(6);

        private final int value;

        BucketState(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    static class RecursionParams {
        public MemoryPool hybridMemoryPool;
        public OperatorMemoryAllocatorCtx memoryContext;
        public int depth;
        public boolean useRecursion;
        public AntiJoinContext antiJoinContext;
        private String bucketNamePrefix;
        private HybridHashJoinExec rootExec;

        public RecursionParams(MemoryPool hybridMemoryPool,
                               OperatorMemoryAllocatorCtx memoryContext, int depth, boolean useRecursion,
                               String bucketNamePrefix, AntiJoinContext antiJoinContext, HybridHashJoinExec rootExec) {
            this.hybridMemoryPool = hybridMemoryPool;
            this.memoryContext = memoryContext;
            this.depth = depth;
            this.useRecursion = useRecursion;
            this.bucketNamePrefix = bucketNamePrefix;
            this.antiJoinContext = antiJoinContext;
            this.rootExec = rootExec;
        }
    }

    private final HybridHashJoinExec rootExec;
    private final boolean isRootExec;
    private final String bucketNamePrefix;
    private final int recursionDepth;
    private final boolean useRecursion;

    private RecursionParams getRecursionParams(int bucketIndex) {
        String newBucketNamePrefix =
            bucketNamePrefix + String.format("bucketI=%d(total%d)-", bucketIndex, bucketCount);
        return new RecursionParams(hybridMemoryPool, memoryContext, recursionDepth + 1, useRecursion,
            newBucketNamePrefix, antiJoinContext, rootExec);
    }

    private final int partitionCount;
    private final int partitionIndex;
    private final int bucketCount;
    private final BucketArea[] bucketAreas;

    MemoryPool hybridMemoryPool;

    // Special mode only for semi/anti-join
    private final boolean specialAntiJoin;
    private boolean passThrough;
    private boolean passNothing;

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();
    private SingleStreamSpillerFactory streamSpillerFactory;
    private List<DataType> buildSideTypes = new ArrayList<>();
    private List<DataType> probeSideTypes = new ArrayList<>();

    private Optional<Integer> unspilledBucketIndex = Optional.empty();

    private PartitionFunction bucketPartitionFunction;

    private boolean build = false;

    private Chunk saveProbeChunk = null;
    private int saveProbePosition = 0;
    private Chunk saveProbeKeyChunk = null;
    private int[] saveProbeHashCodes = null;
//  private IntArrayList[] spillPartitionAssigment;

    private boolean finished = false;

    //earlyStopInputs make a early termination.
    private boolean outerIsEmpty = false;

    private ListenableFuture<?> produceBlocked;

    private OperatorMemoryAllocatorCtx memoryContext;

    private ListenableFuture<?> spillInProgress = immediateFuture(null);

    private ListenableFuture<?> consumeIsBlocked = immediateFuture(null);

    private boolean openConsumed = false;
    ChunkBufferFromExtraBlockBuilder resultChunkBuffer;

    // TODO: refactor
    public HybridHashJoinExec(Executor outerInput,
                              Executor innerInput,
                              JoinRelType joinType,
                              boolean maxOneRow,
                              List<EquiJoinKey> joinKeys,
                              IExpression otherCondition,
                              List<IExpression> antiJoinOperands,
                              ExecutionContext context,
                              int partitionCount,
                              int partitionIndex,
                              int bucketCount,
                              SingleStreamSpillerFactory singleStreamSpillerFactory,
                              RecursionParams recursionParams) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null, context);
        createBlockBuilders();
        this.partitionCount = partitionCount;
        this.partitionIndex = partitionIndex;
        this.bucketCount = bucketCount;
        this.bucketAreas = new BucketArea[bucketCount];
        //The only difference with original constructor
        this.isRootExec = false;
        rootExec = recursionParams.rootExec;
        this.hybridMemoryPool = recursionParams.hybridMemoryPool;
        this.memoryContext = recursionParams.memoryContext;
        this.recursionDepth = recursionParams.depth;
        this.bucketNamePrefix = recursionParams.bucketNamePrefix;
        this.antiJoinContext = recursionParams.antiJoinContext;
        int maxDepth = context.getParamManager().getInt(ConnectionParams.HYBRID_HASH_JOIN_MAX_RECURSIVE_DEPTH);
        this.useRecursion = recursionDepth < maxDepth && recursionParams.useRecursion;
        //************

        for (DataType dataType : innerInput.getDataTypes()) {
            this.buildSideTypes.add(dataType);
        }

        for (DataType dataType : outerInput.getDataTypes()) {
            this.probeSideTypes.add(dataType);
        }

        specialAntiJoin = joinType == JoinRelType.ANTI && antiJoinOperands != null;
        this.streamSpillerFactory = singleStreamSpillerFactory;
        this.produceBlocked = ProducerExecutor.NOT_BLOCKED;

        this.resultChunkBuffer =
            new ChunkBufferFromExtraBlockBuilder(this.blockBuilders, this.chunkLimit);

        bucketPartitionFunction =
            new LocalBucketPartitionFunction(bucketCount, partitionCount, partitionIndex);
    }

    public HybridHashJoinExec(Executor outerInput,
                              Executor innerInput,
                              JoinRelType joinType,
                              boolean maxOneRow,
                              List<EquiJoinKey> joinKeys,
                              IExpression otherCondition,
                              List<IExpression> antiJoinOperands,
                              ExecutionContext context,
                              int partitionCount,
                              int partitionIndex,
                              int bucketCount,
                              SingleStreamSpillerFactory singleStreamSpillerFactory) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null, context);
        this.recursionDepth = 0;
        if (partitionCount == 1 && bucketCount == 1) {
            useRecursion = false;
        } else {
            useRecursion = true;
        }
        antiJoinContext = new AntiJoinContext();
        createBlockBuilders();
        this.isRootExec = true;
        rootExec = this;
        this.partitionCount = partitionCount;
        this.partitionIndex = partitionIndex;
        this.bucketCount = bucketCount;
        this.bucketAreas = new BucketArea[bucketCount];

        this.hybridMemoryPool = MemoryPoolUtils.createOperatorTmpTablePool(
            getExecutorName(), context.getMemoryPool());

        this.memoryContext = new OperatorMemoryAllocatorCtx(hybridMemoryPool, singleStreamSpillerFactory != null);

        for (DataType dataType : innerInput.getDataTypes()) {
            this.buildSideTypes.add(dataType);
        }

        for (DataType dataType : outerInput.getDataTypes()) {
            this.probeSideTypes.add(dataType);
        }
        specialAntiJoin = joinType == JoinRelType.ANTI && antiJoinOperands != null;
        this.streamSpillerFactory = singleStreamSpillerFactory;
        this.produceBlocked = ProducerExecutor.NOT_BLOCKED;

        this.resultChunkBuffer =
            new ChunkBufferFromExtraBlockBuilder(this.blockBuilders, this.chunkLimit);

        bucketPartitionFunction =
            new LocalBucketPartitionFunction(bucketCount, partitionCount, partitionIndex);

        this.bucketNamePrefix = String.format("partition %d-", partitionIndex);
    }

    @Override
    void doClose() {
        if (logger.isDebugEnabled()) {
            logger.debug(this + " doClose:" + build);
        }
        outerInput.close();
        closeConsume(true);
    }

    @Override
    public void openConsume() {
        for (int i = 0; i < bucketAreas.length; i++) {
            this.bucketAreas[i] = new BucketArea(i);
        }
        openConsumed = true;
    }

    @Override
    public void closeConsume(boolean force) {
        if (logger.isDebugEnabled()) {
            logger.debug(this + " closeConsume:" + openConsumed + ",force=" + force);
        }
        if (openConsumed || force) {
            synchronized (bucketAreas) {
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    bucketAreas[bucket].close();
                }
            }
            if (isRootExec && hybridMemoryPool != null) {
                collectMemoryUsage(hybridMemoryPool);
                hybridMemoryPool.destroy();
            }
        }
    }

    @Override
    public void doOpen() {
        outerInput.open();
    }

    private int getBucket(Chunk inputKeyChunk) {
        if (bucketCount == 1) {
            return 0;
        }
        int bucket = getPartitionGenerator().getPartition(inputKeyChunk, 0);
        checkState(bucket >= 0, "bucket:%s is negative", bucket);
        return bucket;
    }

    private PartitionFunction getPartitionGenerator() {
        return bucketPartitionFunction;
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        Chunk keyInputChunk = innerKeyChunkGetter.apply(inputChunk);
        if (specialAntiJoin) {
            antiJoinContext.consumeChunk(keyInputChunk);
        }
        int bucketIndex = getBucket(keyInputChunk);
        bucketAreas[bucketIndex].consumeInnerChunk(inputChunk, keyInputChunk);
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        return consumeIsBlocked;
    }

    @Override
    public boolean needsInput() {
        return consumeIsBlocked.isDone();
    }

    @Override
    public void buildConsume() {
        if (openConsumed) {
            if (logger.isDebugEnabled()) {
                logger.debug(this + " buildConsume:" + build);
            }
            if (!build) {
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    bucketAreas[bucket].buildInner();
                }
                if (specialAntiJoin) {
                    if (antiJoinContext.innerIsEmpty) {
                        passThrough = true;
                    }
                    if (antiJoinContext.innerHasNull) {
                        passNothing = true;
                    }
                } else {
                    passThrough = Arrays.stream(bucketAreas).allMatch(t -> t.directOutputProbe == true);
                    passNothing = Arrays.stream(bucketAreas).allMatch(t -> t.noJoinChunkOutput == true);
                }
                build = true;
            }
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return produceBlocked;
    }

    Chunk doNextChunkOuterEmpty() {
        if (!unspilledBucketIndex.isPresent() || bucketAreas[unspilledBucketIndex.get()].unspillFinished()) {
            //上一个unspill 分区已经没有数据了，尝试选择一个分区做unspill
            if (unspilledBucketIndex.isPresent()) {
                //free the useless bucket's memory in advance
                bucketAreas[unspilledBucketIndex.get()].reset();
                bucketAreas[unspilledBucketIndex.get()].close();
                unspilledBucketIndex = Optional.empty();
            }
            int bucketIndex = getNextUnspillBucket();
            if (bucketIndex >= 0) {
                if (!tryUnspillNext(bucketIndex)) {
                    return null;
                }
            } else {
                finished = true;
                saveProbeKeyChunk = null;
                saveProbeHashCodes = null;
                resultChunkBuffer.flushToBuffer(true);
                return resultChunkBuffer.nextChunk();
            }
        }
        int bucketIndex = unspilledBucketIndex.get();
        bucketAreas[bucketIndex].tryProduce();
        return resultChunkBuffer.nextChunk();
    }

    @Override
    Chunk doNextChunk() {
        if (!produceBlocked.isDone()) {
            return null;
        }
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            Chunk ret = outerInput.nextChunk();
            if (ret == null) {
                if (outerInput.produceIsFinished()) {
                    this.finished = true;
                }
                produceBlocked = outerInput.produceIsBlocked();
            }
            return ret;
        } else if (passNothing) {
            this.finished = true;
            return null;
        }
        Chunk ret = resultChunkBuffer.nextChunk();
        if (ret != null) {
            return ret;
        }
        if (outerIsEmpty) {
            return doNextChunkOuterEmpty();
        }

        if (saveProbeChunk == null || saveProbePosition == saveProbeChunk.getPositionCount()) {
            saveProbeChunk = outerInput.nextChunk();

            if (saveProbeChunk == null) {
                if (!outerIsEmpty && !outerInput.produceIsFinished()) {
                    //outer input可能还有数据，这里暂时退出来
                    finished = false;
                    produceBlocked = outerInput.produceIsBlocked();
                    return null;
                }

                //outer input已经没有数据了，可以释放之前所有build好的分区内存
                this.outerIsEmpty = true;
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    bucketAreas[bucket].buildOuter();
                }
                return doNextChunkOuterEmpty();
            }
            saveProbeKeyChunk = outerKeyChunkGetter.apply(saveProbeChunk);
            saveProbeHashCodes = saveProbeKeyChunk.hashCodeVector();
            saveProbePosition = 0;
        }

        checkState(saveProbeChunk != null);
        int bucketIndex = getBucket(saveProbeKeyChunk);
        bucketAreas[bucketIndex].consumeOuterChunkAndTryProduce();

        return resultChunkBuffer.nextChunk();
    }

    @Override
    public boolean produceIsFinished() {
        if (!resultChunkBuffer.produceIsFinished()) {
            return false;
        }
        return passNothing || finished;
    }

    private boolean tryUnspillNext(int bucketIndex) {
        boolean success = bucketAreas[bucketIndex].spillHandler.tryOpenUnspill();
        // TODO: need add retry count?
        if (!success) {
            if (partitionCount == 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "HybridHashJoinExec unspill failed and can't recursive spill, because the executor need hold collation");
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "HybridHashJoinExec unspill failed and recursion" + String.format(" %d depth!", recursionDepth));
            }
//            return false;
        }
        bucketAreas[bucketIndex].state = BucketState.INPUT_UNSPILLING;
        unspilledBucketIndex = Optional.of(bucketIndex);
        return true;
    }

    private int getNextUnspillBucket() {
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            if (bucketAreas[bucket].state == BucketState.INPUT_SPILLED) {
                return bucket;
            }
        }
        return -1;
    }

    private int getNextConsumingBucket() {
        int retBucket = -1;
        long revocableMemory = -1L;
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            if (bucketAreas[bucket].state == BucketState.CONSUMING_INPUT
                && bucketAreas[bucket].inMemoryAllocateMemory > revocableMemory) {
                retBucket = bucket;
                revocableMemory = bucketAreas[bucket].inMemoryAllocateMemory;
            }
        }
        return retBucket;
    }

    private int getNextBuiltBucket() {
        int retBucket = -1;
        long revocableMemory = -1L;
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            if (bucketAreas[bucket].state == BucketState.LOOKUP_SOURCE_BUILT
                && bucketAreas[bucket].inMemoryAllocateMemory > revocableMemory) {
                retBucket = bucket;
                revocableMemory = bucketAreas[bucket].inMemoryAllocateMemory;
            }
        }
        return retBucket;
    }

    private int getBucketToSpill() {
        // not unspill phase yet, just spill
        if (!outerIsEmpty) {
            return build ? getNextBuiltBucket() : getNextConsumingBucket();
        }
        if (unspilledBucketIndex.isPresent()) {
            if (finished || !useRecursion) {
                return -1;
            }
            return unspilledBucketIndex.get();
        }
        return -1;
    }

    public ListenableFuture<?> cannotRevoke() {
        if (memoryContext.getRevocableAllocated() != 0) {
            logger.warn(String.format(bucketNamePrefix + "memory pool statistics probably leak %d",
                memoryContext.getRevocableAllocated()));
        }
        finishMemoryRevoke = Optional.of(() -> {
            memoryContext.releaseRevocableMemory(memoryContext.getRevocableAllocated(), true);
        });
        spillInProgress = ProducerExecutor.NOT_BLOCKED;
        return spillInProgress;

    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        checkState(spillInProgress.isDone());
        int bucketToSpill = getBucketToSpill();
        if (bucketToSpill < 0) {
            return cannotRevoke();
        }
        spillInProgress = bucketAreas[bucketToSpill].startMemoryRevoke();
        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke() {
        checkState(spillInProgress.isDone());
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryContext;
    }

    private class BucketArea {
        ChunksIndex innerChunks;
        ChunksIndex innerKeyChunks;
        BucketState state;
        int bucketIndex;

        //build
        ConcurrentRawHashTable hashTable;
        int[] positionLinks;
        FastIntBloomFilter bloomFilter;

        // Special mode only for semi/anti-join
        private boolean directOutputProbe;
        private boolean noJoinChunkOutput;
        boolean alreadyBuild;

        private long needAllocateMemory = 0;
        private long inMemoryAllocateMemory = 0;

        private SpillHandler spillHandler;

        public BucketArea(int bucketIndex) {
            this.innerChunks = new ChunksIndex();
            this.innerKeyChunks = new ChunksIndex();
            this.state = BucketState.CONSUMING_INPUT;
            this.bucketIndex = bucketIndex;
        }

        public void consumeInnerChunk(Chunk inputChunk, Chunk keyInputChunk) {
            if (state == BucketState.SPILLING_INPUT) {
                //TODO: memory statistic is only for the bottom of the recursion
                needAllocateMemory += inputChunk.estimateSize();
                needAllocateMemory += keyInputChunk.estimateSize();
                spillHandler.buildSpillerExec.spillChunk(inputChunk);
                consumeIsBlocked = spillHandler.buildSpillerExec.spillIsBlocked();
            } else {
                checkState(state == BucketState.CONSUMING_INPUT);
                addChunk(inputChunk, keyInputChunk, true);
            }
        }

        public void buildInner() {
            if (state == BucketState.CONSUMING_INPUT) {
                doBuildTable();
                state = BucketState.LOOKUP_SOURCE_BUILT;
            } else if (state == BucketState.SPILLING_INPUT) {
                state = BucketState.INPUT_SPILLED;
            }
        }

        public void tryProduce() {
            if (state == BucketState.INPUT_UNSPILLING) {
                spillHandler.tryBuild();
            } else {
                checkState(state == BucketState.INPUT_UNSPILLED_AND_BUILT);
                spillHandler.tryProduce();
            }

        }

        public void consumeOuterChunkAndTryProduce() {
            final int positionCount = saveProbeChunk.getPositionCount();

            if (this.directOutputProbe) {
                for (; saveProbePosition < positionCount; saveProbePosition++) {
                    if (!antiJoinContext.innerIsEmpty) {
                        boolean allIsNull = true;
                        for (int i = 0; i < saveProbeKeyChunk.getBlockCount(); i++) {
                            allIsNull = allIsNull && saveProbeKeyChunk.getBlock(i).isNull(saveProbePosition);
                        }
                        if (allIsNull) {
                            continue;
                        }
                    }
                    for (int i = 0; i < saveProbeChunk.getBlockCount(); i++) {
                        saveProbeChunk.getBlock(i).writePositionTo(saveProbePosition, blockBuilders[i]);
                    }
                }
            } else if (this.noJoinChunkOutput) {
                // saveProbePosition的目的是告诉当前saveProbeChunk已经消费完成
                saveProbePosition = positionCount;
            } else {
                if (this.state == BucketState.INPUT_SPILLED) {
                    boolean needBloomfilter = true;
                    if (semiJoin && joinType == JoinRelType.ANTI) {
                        needBloomfilter = false;
                    }
                    if (this.bloomFilter != null && needBloomfilter) {
                        for (; saveProbePosition < positionCount; saveProbePosition++) {
                            int hashCode = saveProbeHashCodes[saveProbePosition];
                            if (this.bloomFilter.mightContain(hashCode)) {
                                this.spillHandler.getProbeSpillerExec()
                                    .addRowToSpill(saveProbeChunk, saveProbePosition);
                            } else {
                                if (outerJoin) {
                                    if (joinType != JoinRelType.RIGHT) {
                                        buildLeftNullRow(saveProbeChunk, saveProbePosition);
                                    } else {
                                        buildRightNullRow(saveProbeChunk, saveProbePosition);
                                    }
                                    resultChunkBuffer.flushToBuffer(false);
                                }
                                if (semiJoin) {
                                    if (joinType == JoinRelType.ANTI
                                        && checkAntiJoinOperands(saveProbeChunk.rowAt(saveProbePosition))) {
                                        buildSemiJoinRow(saveProbeChunk, saveProbePosition);
                                    }
                                    resultChunkBuffer.flushToBuffer(false);
                                }
                            }
                        }
                        this.spillHandler.getProbeSpillerExec().spillRows(false);
                    } else {
                        //BloomFilter 还未生成，所以只能将收到的probe数据全量spill
                        this.spillHandler.getProbeSpillerExec().spillChunk(saveProbeChunk);
                        // saveProbePosition的目的是告诉当前saveProbeChunk已经消费完成
                        saveProbePosition = positionCount;
                    }
                    produceBlocked = this.spillHandler.getProbeSpillerExec().spillIsBlocked();
                } else {
                    while (saveProbePosition < positionCount) {
                        int hashCode = saveProbeHashCodes[saveProbePosition];
                        this.join(saveProbeChunk, saveProbeKeyChunk, hashCode);
                        saveProbePosition++;
                        if (resultChunkBuffer.hasNextChunk()) {
                            break;
                        }
                    }
                }
            }

        }

        void buildOuter() {
            if (this.state == BucketState.LOOKUP_SOURCE_BUILT) {
                this.reset();
                this.state = BucketState.DISPOSED;
                return;
            }
        }

        public ListenableFuture<?> startMemoryRevoke() {
            if (state == BucketState.INPUT_UNSPILLING ||
                state == BucketState.INPUT_UNSPILLED_AND_BUILT) {
                return spillHandler.startMemoryRevoke();
            }
            finishMemoryRevoke = Optional.of(() -> {
                bucketAreas[bucketIndex].reset();
            });
            if (innerChunks.getPositionCount() == 0) {
                // not only optimize, to ensure call doSpecialCheckForSemiJoin when innerChunk is empty
                return ProducerExecutor.NOT_BLOCKED;
            }
            if (state == BucketState.CONSUMING_INPUT) {
                state = BucketState.SPILLING_INPUT;
            } else {
                checkState(state == BucketState.LOOKUP_SOURCE_BUILT);
                state = BucketState.INPUT_SPILLED;
            }

            rootExec.addSpillCnt(1);
            logger.info(String.format(bucketNamePrefix + "start spill the bucket-%d, and it will release %d memory",
                bucketIndex, inMemoryAllocateMemory));

            synchronized (bucketAreas) {
                if (useRecursion) {
                    spillHandler = new SubJoinSpillHandler();
                } else {
                    spillHandler = new RebuildSpillHandler();
                }
            }
            spillHandler.getBuildSpillerExec().spillChunksIterator(innerChunks.getChunksAndDeleteAfterRead());
            return spillHandler.getBuildSpillerExec().spillIsBlocked();
        }

        void reset() {
            this.innerChunks = new ChunksIndex();
            this.innerKeyChunks = new ChunksIndex();
            this.hashTable = null;
            this.positionLinks = null;
            //释放内存
            if (state == BucketState.INPUT_UNSPILLED_AND_BUILT) {
                //因为unSpill是从reserved申请的，所以这里需要单独释放UnSpill分区的内存
                memoryContext.releaseReservedMemory(inMemoryAllocateMemory, true);
            } else {
                if (memoryContext.isRevocable()) {
                    memoryContext.releaseRevocableMemory(inMemoryAllocateMemory, true);
                } else {
                    memoryContext.releaseReservedMemory(inMemoryAllocateMemory, true);
                }
            }
            inMemoryAllocateMemory = 0;
        }

        void close() {
            if (logger.isDebugEnabled()) {
                logger.debug("close BucketArea:" + bucketIndex);
            }
            this.innerChunks = null;
            this.innerKeyChunks = null;
            this.hashTable = null;
            this.positionLinks = null;
            this.state = BucketState.DISPOSED;
            if (spillHandler != null) {
                spillHandler.close();
            }
            inMemoryAllocateMemory = 0;

        }

        void addChunk(Chunk chunk, Chunk keyInputChunk, boolean recordMem) {
            innerChunks.addChunk(chunk);
            innerKeyChunks.addChunk(keyInputChunk);
            if (recordMem) {
                long allocateMem = keyInputChunk.estimateSize() + chunk.estimateSize();
                needAllocateMemory += allocateMem;
                inMemoryAllocateMemory += allocateMem;
                if (memoryContext.isRevocable()) {
                    memoryContext.allocateRevocableMemory(allocateMem);
                } else {
                    memoryContext.allocateReservedMemory(allocateMem);
                }
            }
        }

        /**
         * For semi/anti-join, enter pass-through or pass-nothing mode in some cases
         */
        void doSpecialCheckForSemiJoin() {
            directOutputProbe = false;
            noJoinChunkOutput = false;
            if (innerChunks.isEmpty() && semiJoin) {
                if (joinType == JoinRelType.SEMI) {
                    noJoinChunkOutput = true;
                } else if (joinType == JoinRelType.ANTI) {
                    // Note that even for 'NOT IN' anti-join, we should not check operator anymore
                    directOutputProbe = true;
                } else {
                    throw new AssertionError();
                }
            }
        }

        void doBuildTable() {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "complete fetching the " + bucketNamePrefix + bucketIndex + " bucket index rows ... total count = "
                        + innerChunks
                        .getPositionCount());
            }
            if (innerChunks.isEmpty() && joinType == JoinRelType.INNER) {
                noJoinChunkOutput = true;
            }
            if (semiJoin) {
                doSpecialCheckForSemiJoin();
            }

            final int size = innerChunks.getPositionCount();
            this.hashTable = new ConcurrentRawHashTable(size);

            this.positionLinks = new int[size];
            Arrays.fill(positionLinks, LIST_END);

            if (!alreadyBuild) {
                if (size <= BLOOM_FILTER_ROWS_LIMIT && size > 0) {
                    this.bloomFilter = FastIntBloomFilter.create(size);
                }
            }

            int position = 0;
            for (int chunkId = 0; chunkId < innerChunks.getChunkCount(); ++chunkId) {
                Chunk keyChunk = innerKeyChunks.getChunk(chunkId);
                if (alreadyBuild) {
                    buildOneChunk(keyChunk, position, hashTable, positionLinks, null);
                } else {
                    buildOneChunk(keyChunk, position, hashTable, positionLinks, bloomFilter);
                }

                position += keyChunk.getPositionCount();
            }
            alreadyBuild = true;
            //TODO 这里计算HashTable的内存开销，主要考虑到tryUnSpill过程中无法预知HashTable的内存消耗
//            memoryAllocator.allocateReservedMemory(hashTable.estimateSize());
//            memoryAllocator.allocateReservedMemory(SizeOf.sizeOf(positionLinks));
        }

        int matchInit(Chunk keyChunk, int position, int hashCode) {
            if (bloomFilter != null && !bloomFilter.mightContain(hashCode)) {
                return LIST_END;
            }

            int matchedPosition = hashTable.get(hashCode);
            while (matchedPosition != LIST_END) {
                if (innerKeyChunks.equals(matchedPosition, keyChunk, position)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        int matchNext(int current, Chunk keyChunk, int position) {
            int matchedPosition = positionLinks[current];
            while (matchedPosition != LIST_END) {
                if (innerKeyChunks.equals(matchedPosition, keyChunk, position)) {
                    break;
                }
                matchedPosition = positionLinks[matchedPosition];
            }
            return matchedPosition;
        }

        boolean matchValid(int current) {
            return current != LIST_END;
        }

        private void buildSemiJoinRow(Chunk inputChunk, int position) {
            // outer side only
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                inputChunk.getBlock(i).writePositionTo(position, blockBuilders[i]);
            }
        }

        void join(Chunk probeChunk, Chunk probeKeyChunk, int hashCode) {
            int matchedPosition = matchInit(probeKeyChunk, saveProbePosition, hashCode);
            boolean matched = false;
            for (; matchValid(matchedPosition); matchedPosition = matchNext(matchedPosition, probeKeyChunk,
                saveProbePosition)) {
                if (!checkJoinCondition(innerChunks, probeChunk, saveProbePosition, matchedPosition)) {
                    continue;
                }

                if (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT) {
                    buildJoinRow(innerChunks, probeChunk, saveProbePosition, matchedPosition);
                } else if (joinType == JoinRelType.RIGHT) {
                    buildRightJoinRow(innerChunks, probeChunk, saveProbePosition, matchedPosition);
                }
                resultChunkBuffer.flushToBuffer(false);
                // checks max1row generated from scalar subquery
                if ((!ConfigDataMode.isFastMock()) && singleJoin && matched) {
                    throw new TddlRuntimeException(ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW);
                }

                // set matched flag
                matched = true;

                // semi/anti-joins do not care multiple matches
                if (semiJoin) {
                    break;
                }
            }

            // generates a null result while using outer join
            if (outerJoin && !matched) {
                if (joinType != JoinRelType.RIGHT) {
                    buildLeftNullRow(probeChunk, saveProbePosition);
                } else {
                    buildRightNullRow(probeChunk, saveProbePosition);
                }
                resultChunkBuffer.flushToBuffer(false);
            }

            // generates a semi-row result while using semi or anti-semi join
            if (semiJoin) {
                if (joinType == JoinRelType.SEMI && matched) {
                    buildSemiJoinRow(probeChunk, saveProbePosition);
                } else if (joinType == JoinRelType.ANTI && !matched
                    && checkAntiJoinOperands(probeChunk.rowAt(saveProbePosition))) {
                    buildSemiJoinRow(probeChunk, saveProbePosition);
                }
                resultChunkBuffer.flushToBuffer(false);
            }
        }

        boolean unspillFinished() {
            return spillHandler.produceIsFinished();
        }

        private abstract class SpillHandler {
            protected SpillerExec buildSpillerExec;
            protected SpillerExec probeSpillerExec;

            public SpillerExec getBuildSpillerExec() {
                return buildSpillerExec;
            }

            public SpillerExec getProbeSpillerExec() {
                return probeSpillerExec;
            }

            // should synchronized (bucketAreas) by caller, for closeConsume
            SpillHandler() {
                String buildSpillerName = context.getTraceId() + "-" + partitionIndex + "-Bucket-" + bucketIndex;
                String probeSpillerName =
                    context.getTraceId() + "-" + partitionIndex + "-ProbePartition-" + bucketIndex;
                SingleStreamSpiller buildSpiller =
                    context.getQuerySpillSpaceMonitor().register(streamSpillerFactory.create(buildSpillerName,
                        buildSideTypes, context.getQuerySpillSpaceMonitor().newLocalSpillMonitor(), null));
                SingleStreamSpiller probeSpiller =
                    context.getQuerySpillSpaceMonitor().register(streamSpillerFactory.create(probeSpillerName,
                        probeSideTypes, context.getQuerySpillSpaceMonitor().newLocalSpillMonitor(), null));
                this.buildSpillerExec =
                    new SpillerExec(buildSpiller, new ChunkBuilder(buildSideTypes, chunkLimit, context));
                this.probeSpillerExec =
                    new SpillerExec(probeSpiller, new ChunkBuilder(probeSideTypes, chunkLimit, context));
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        HybridHashJoinExec.this + " bucketId=" + bucketIndex + "Spill handler create. buildSpiller="
                            + buildSpiller + ", probeSpiller=" + probeSpiller);
                }
            }

            void close() {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        HybridHashJoinExec.this + " bucketId=" + bucketIndex + "Spill handler close. buildSpiller="
                            + buildSpillerExec + ", probeSpiller=" + probeSpillerExec);
                }
                buildSpillerExec.close();
                probeSpillerExec.close();
            }

            boolean tryOpenUnspill() {
                buildSpillerExec.buildUnspill();
                probeSpillerExec.buildUnspill();
                return true;
            }

            abstract void tryBuild();

            void buildOver() {
                state = BucketState.INPUT_UNSPILLED_AND_BUILT;
            }

            abstract void tryProduce();

            abstract boolean produceIsFinished();

            abstract public ListenableFuture<?> startMemoryRevoke();
        }

        private class SubJoinSpillHandler extends SpillHandler {
            private AbstractJoinExec subJoinExec = null;
            private ConsumerExecutor subJoinConsumer;
            private BucketDivideChunkBuffer buildBucketDivider;
            private BucketDivideChunkBuffer probeBucketDivider;

            @Override
            void close() {
                super.close();
                if (subJoinExec != null) {
                    subJoinExec.doClose();
                }
            }

            @Override
            boolean tryOpenUnspill() {
                super.tryOpenUnspill();
                int subPartitionCount = partitionCount * bucketCount;
                int subPartitionIndex = bucketIndex * partitionCount + partitionIndex;
                int subBucketCount =
                    context.getParamManager().getInt(ConnectionParams.HYBRID_HASH_JOIN_RECURSIVE_BUCKET_NUM);
                buildBucketDivider = new BucketDivideChunkBuffer(subBucketCount, subPartitionCount, subPartitionIndex,
                    innerKeyChunkGetter, buildSideTypes, chunkLimit, context);
                probeBucketDivider = new BucketDivideChunkBuffer(subBucketCount, subPartitionCount, subPartitionIndex,
                    outerKeyChunkGetter, probeSideTypes, chunkLimit, context);

                HybridHashJoinExec join = new HybridHashJoinExec(probeBucketDivider, buildBucketDivider,
                    joinType, singleJoin, joinKeys, condition, antiJoinOperands, context,
                    subPartitionCount,
                    subPartitionIndex,
                    subBucketCount,
                    streamSpillerFactory, getRecursionParams(bucketIndex));
                subJoinConsumer = join;
                subJoinExec = join;
                subJoinConsumer.openConsume();
                return true;
            }

            @Override
            void tryBuild() {
                while (!buildBucketDivider.hasNextChunk()) {
                    Chunk chunk = buildSpillerExec.nextChunk();
                    if (chunk == null) {
                        if (buildSpillerExec.produceIsFinished()) {
                            buildBucketDivider.flushToBuffer(true);
                            break;
                        } else {
                            continue;
                        }
                    }
                    buildBucketDivider.addChunk(chunk);
                }
                Chunk chunk = buildBucketDivider.nextChunk();

                if (chunk != null) {
                    subJoinConsumer.consumeChunk(chunk);
                    produceBlocked = subJoinConsumer.consumeIsBlocked();
                } else {
                    checkState(buildBucketDivider.produceIsFinished());
                    checkState(buildSpillerExec.produceIsFinished());
                    buildOver();
                }
            }

            @Override
            void tryProduce() {
                while (!probeBucketDivider.hasNextChunk()) {
                    Chunk chunk = probeSpillerExec.nextChunk();
                    if (chunk == null) {
                        if (probeSpillerExec.produceIsFinished()) {
                            probeBucketDivider.flushToBuffer(true);
                            break;
                        } else {
                            continue;
                        }
                    }
                    probeBucketDivider.addChunk(chunk);
                }
                Chunk chunk = subJoinExec.doNextChunk();
                if (chunk == null) {
                    produceBlocked = subJoinExec.produceIsBlocked();
                } else {
                    resultChunkBuffer.addChunk(chunk);
                }
            }

            @Override
            boolean produceIsFinished() {
                return subJoinExec.produceIsFinished();
            }

            @Override
            public ListenableFuture<?> startMemoryRevoke() {
                if (subJoinExec instanceof MemoryRevoker) {
                    spillInProgress = ((MemoryRevoker) subJoinExec).startMemoryRevoke();
                    finishMemoryRevoke = Optional.of(() -> {
                        ((MemoryRevoker) subJoinExec).finishMemoryRevoke();
                    });
                    return spillInProgress;
                } else {
                    return cannotRevoke();
                }
            }

            @Override
            void buildOver() {
                subJoinConsumer.buildConsume();
                subJoinExec.doOpen();
                super.buildOver();
            }
        }

        private class RebuildSpillHandler extends SpillHandler {
            boolean finished = false;

            boolean tryAllocateUnspillMemory() {
                boolean ret = memoryContext.tryAllocateReservedMemory(needAllocateMemory);
                if (ret) {
                    inMemoryAllocateMemory = needAllocateMemory;
                }
                return ret;
            }

            @Override
            boolean tryOpenUnspill() {
                boolean success = tryAllocateUnspillMemory();
                if (!success) {
                    return false;
                }
                super.tryOpenUnspill();
                return true;
            }

            @Override
            void tryBuild() {
                Chunk buildChunk = buildSpillerExec.nextChunk();
                if (buildChunk == null) {
                    checkState(buildSpillerExec.produceIsFinished());
                    buildOver();
                    return;
                }
                Chunk keyChunk = innerKeyChunkGetter.apply(buildChunk);
                addChunk(buildChunk, keyChunk, false);
            }

            @Override
            void buildOver() {
                super.buildOver();
                doBuildTable();
            }

            @Override
            void tryProduce() {
                if (saveProbeChunk == null || saveProbePosition == saveProbeChunk.getPositionCount()) {
                    saveProbeChunk = probeSpillerExec.nextChunk();
                    if (saveProbeChunk == null) {
                        finished = true;
                        return;
                    }
                    saveProbeKeyChunk = outerKeyChunkGetter.apply(saveProbeChunk);
                    saveProbeHashCodes = saveProbeKeyChunk.hashCodeVector();
                    saveProbePosition = 0;
                }
                consumeOuterChunkAndTryProduce();
            }

            @Override
            boolean produceIsFinished() {
                return finished;
            }

            @Override
            public ListenableFuture<?> startMemoryRevoke() {
                throw new UnsupportedOperationException(getClass().getName() + ".startMemoryRevoke");
            }
        }

    }
}

