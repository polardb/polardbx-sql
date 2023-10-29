package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector.HashBucketFunction;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitioningExchangerSIMD extends LocalExchanger {
    protected int[] totalLengthsPerPartition;
    protected int[] batchLengthsPerPartition;
    protected int[] destScatterMapStartOffset;
    private int partitionNum;

    private final HashBucketFunction partitionGenerator;
    private final List<Integer> partitionChannels;
    private List<DataType> types;
    private final List<AtomicBoolean> consumings;

    private BlockBuilder[] blockBuilders;
    private ChunkConverter keyConverter;
    private ExecutionContext context;

    protected ChunkBuilder[] chunkBuilders;
    private final ScatterMemoryContext scatterMemoryContext;

    public PartitioningExchangerSIMD(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                                     LocalExchangersStatus status,
                                     boolean asyncConsume,
                                     List<DataType> types,
                                     List<Integer> partitionChannels,
                                     List<DataType> keyTargetTypes,
                                     ExecutionContext context) {
        super(bufferMemoryManager, executors, status, asyncConsume);
        scatterMemoryContext = new ScatterMemoryContext();
        this.types = types;
        this.context = context;
        this.partitionGenerator = new HashBucketFunction(executors.size());
        this.partitionChannels = partitionChannels;
        this.consumings = status.getConsumings();
        if (keyTargetTypes.isEmpty()) {
            this.keyConverter = null;
        } else {
            int[] columnIndex = new int[partitionChannels.size()];
            for (int i = 0; i < partitionChannels.size(); i++) {
                columnIndex[i] = partitionChannels.get(i);
            }
            this.keyConverter = Converters.createChunkConverter(columnIndex, types, keyTargetTypes, context);
        }
        this.partitionNum = executors.size();

        this.blockBuilders = new BlockBuilder[partitionNum]; //预留block builder
        this.batchLengthsPerPartition = new int[partitionNum]; //每一个partitionb中batch的length
        this.totalLengthsPerPartition = new int[partitionNum]; //每一个partition的总大小
        this.destScatterMapStartOffset = new int[partitionNum]; //每一个partition scatter的起始地址
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        this.chunkBuilders = new ChunkBuilder[partitionNum];
        for (int i = 0; i < partitionNum; i++) {
//            BatchBlockWriter.create(
            this.chunkBuilders[i] = new ChunkBuilder(types, 1024, context);
        }
        scatterPartitionNoneBucketPage(chunk);
    }

    private void scatterPartitionNoneBucketPage(Chunk chunk) {
        Chunk keyChunk;
        if (keyConverter == null) {
            keyChunk = getPartitionFunctionArguments(chunk);
        } else {
            keyChunk = keyConverter.apply(chunk);
        }
        scatterMemoryContext.init(chunk.getBlocksDirectly()); //初始化scatter内存管理上下文

        Arrays.fill(totalLengthsPerPartition, 0);
        int pageSize = chunk.getPositionCount();
        int batchSize = 1024;
        int positionStart = 0;
        for (; positionStart <= pageSize - batchSize; positionStart += batchSize) {
            scatterPartitionNoneBucketPageInBatch(keyChunk, chunk, positionStart, batchSize);
        }

        if (positionStart < pageSize) { //处理剩余的数据
            scatterPartitionNoneBucketPageInBatch(keyChunk, chunk, positionStart, pageSize - positionStart);
        }

        // build a page for each partition
        Map<Integer, Chunk> partitionChunks = new HashMap<>();
        for (int partition = 0; partition < partitionNum; partition++) { //枚举每个partition
            int lengthPerPartition = totalLengthsPerPartition[partition]; //获取到该partition的所有数据
            if (lengthPerPartition > 0) {
                ChunkBuilder partitionPageBuilder = chunkBuilders[partition]; //获取到page Builder
                partitionPageBuilder.declarePosition(lengthPerPartition); //该partition内的数量
                Chunk partitionedChunk = partitionPageBuilder.build();
                partitionChunks.put(partition, partitionedChunk);
            }
        }
        if (asyncConsume) {
            for (Map.Entry<Integer, Chunk> entry : partitionChunks.entrySet()) {
                int partition = entry.getKey();
                executors.get(partition).consumeChunk(entry.getValue());
            }
        } else {
            for (Map.Entry<Integer, Chunk> entry : partitionChunks.entrySet()) {
                int partition = entry.getKey();
                AtomicBoolean consuming = consumings.get(partition);
                while (true) {
                    if (consuming.compareAndSet(false, true)) {
                        try {
                            executors.get(partition).consumeChunk(entry.getValue());
                        } finally {
                            consuming.set(false);
                        }
                        break;
                    }
                }
            }
        }

    }

    private void scatterPartitionNoneBucketPageInBatch(Chunk keyChunk, Chunk chunk, int positionStart, int batchSize) {
        //最终得到destScatterMap，其中保存了每个元素scatter之后的元素的位置
        scatterAssignmentNoneBucketPageInBatch(keyChunk, positionStart, batchSize); //计算partition的逻辑
        scatterCopyNoneBucketPageInBatch(chunk, positionStart, batchSize); //核心的copy逻辑
    }

    private void scatterAssignmentNoneBucketPageInBatch(Chunk keyChunk, int positionStart, int batchSize) {
        Arrays.fill(batchLengthsPerPartition, 0);
        int[] partitions = scatterMemoryContext.getPartitionsBuffer();
        for (int i = 0; i < batchSize; i++) {
            int position = i + positionStart;
            int partition = partitionGenerator.getPartition(keyChunk, position);
            partitions[i] = partition;
            ++batchLengthsPerPartition[partition]; //更新每个partition中的batch数量
        }

        int[] destScatterMap = scatterMemoryContext.getDestScatterMapBuffer();
        prepareLengthsPerPartitionInBatch(batchSize, partitions, destScatterMap);
    }

    //destScatterMapStartOffset存储了每个partition对应开始位置的offset
    private void prepareDestScatterMapStartOffset() {
        int startOffset = 0;
        for (int i = 0; i < partitionNum; i++) {
            destScatterMapStartOffset[i] = startOffset; //统计出每个batch scatter之后对应的map的偏移量
            startOffset += batchLengthsPerPartition[i];
        }
    }

    protected void prepareLengthsPerPartitionInBatch(int batchSize, int[] partitions, int[] destScatterMap) {
        prepareDestScatterMapStartOffset();
        for (int i = 0; i < batchSize; ++i) {
            destScatterMap[i] = destScatterMapStartOffset[partitions[i]]++; //获取每个位置scatter之后对应的内存地址
        }
    }

    protected void scatterCopyNoneBucketPageInBatch(Chunk keyChunk, int positionStart, int batchSize) {
        Block[] sourceBlocks = keyChunk.getBlocksDirectly(); //获取到所有的block
        for (int col = 0; col < types.size(); col++) { //枚举所有列
            Block sourceBlock = sourceBlocks[col]; //获取到block
            scatterMemoryContext.setBatchSize(batchSize);
            scatterMemoryContext.setSrcStartPosition(positionStart);
            scatterMemoryContext.setDestLengthsPerPartitionInBatch(batchLengthsPerPartition);
            for (int i = 0; i < chunkBuilders.length; i++) {
                blockBuilders[i] = chunkBuilders[i].getBlockBuilder(col); //获取到所有partition的pageBuilder
            }
            vectorizedSIMDScatterAppendTo(sourceBlock, scatterMemoryContext, blockBuilders);
        }
        for (int i = 0; i < partitionNum; i++) {
            totalLengthsPerPartition[i] += batchLengthsPerPartition[i];
        }
    }

    void vectorizedSIMDScatterAppendTo(Block block, ScatterMemoryContext scatterMemoryContext,
                                       BlockBuilder[] blockBuilders) {
        block.copyPositions_scatter_simd(scatterMemoryContext, blockBuilders);
    }

    private Chunk getPartitionFunctionArguments(Chunk page) {
        Block[] blocks = new Block[partitionChannels.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(partitionChannels.get(i));
        }
        return new Chunk(page.getPositionCount(), blocks);
    }

}
