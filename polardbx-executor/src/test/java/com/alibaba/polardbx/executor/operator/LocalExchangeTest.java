package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.EmptyMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.LocalHashBucketFunction;
import com.alibaba.polardbx.executor.mpp.operator.PartitionFunction;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalExchangeConsumerFactory;
import com.alibaba.polardbx.executor.mpp.planner.LocalExchange;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LocalExchangeTest extends BaseExecTest {

    private long localBufferSize;
    private Executor notificationExecutor;

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);

        // open vectorization implementation o f join probing and rows building.
        connectionMap.put(ConnectionParams.ENABLE_EXCHANGE_PARTITION_OPTIMIZATION.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_LOCAL_EXCHANGE_BATCH.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));

        localBufferSize = context.getParamManager().getLong(ConnectionParams.MPP_TASK_LOCAL_MAX_BUFFER_SIZE);
        notificationExecutor = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testPartitionExchanger() {
        // consistent data types during exchange.
        final List<DataType> exchangeDataTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType);

        // parallelism for consumer.
        final int parallelism = 4;

        // which column index to calculate hash.
        final List<Integer> partitionChannels = ImmutableList.of(0);

        // exchange mode.
        LocalExchange.LocalExchangeMode exchangeMode = LocalExchange.LocalExchangeMode.PARTITION;

        OutputBufferMemoryManager outputBufferMemoryManager =
            new OutputBufferMemoryManager(localBufferSize, new EmptyMemSystemListener(), notificationExecutor);

        ExecutorFactory parentExecutorFactory =
            new LocalBufferExecutorFactory(outputBufferMemoryManager, exchangeDataTypes, parallelism);

        LocalExchange localExchange = new LocalExchange(exchangeDataTypes, partitionChannels, exchangeMode, true);

        LocalExchangeConsumerFactory factory =
            new LocalExchangeConsumerFactory(parentExecutorFactory, outputBufferMemoryManager, localExchange);

        // consumer
        List<ExecTestDriver.ConsumeTask> consumeTaskList = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            ConsumerExecutor consumer = factory.createExecutor(context, i);

            List<Chunk> inputChunks = ImmutableList.of(
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                ),
                new Chunk(
                    randomIntegerBlock(0, 1000, 1000, 10),
                    randomLongBlock(0, 10000, 1000, 10),
                    randomIntegerBlock(1000, 5000, 1000, 10)
                )
            );

            ExecTestDriver.ConsumeTask consumeTask = new ExecTestDriver.SerialConsumeTask(consumer, inputChunks);
            consumeTaskList.add(consumeTask);
        }

        // producer
        List<ExecTestDriver.ProduceTask> produceTaskList = new ArrayList<>();
        List<ConsumerExecutor> localBufferExecutors = factory.getConsumerExecutors();
        for (int i = 0; i < parallelism; i++) {
            com.alibaba.polardbx.executor.operator.Executor producer =
                (com.alibaba.polardbx.executor.operator.Executor) localBufferExecutors.get(i);

            ExecTestDriver.ProduceTask produceTask = new ExecTestDriver.SerialProduceTask(producer);
            produceTaskList.add(produceTask);
        }

        // execute partition exchanger
        for (ExecTestDriver.ConsumeTask consumeTask : consumeTaskList) {
            consumeTask.exec();
        }

        // Get partition function to check with results of producer.
        PartitionFunction partitionFunction = new LocalHashBucketFunction(parallelism);

        for (int i = 0; i < parallelism; i++) {
            ExecTestDriver.ProduceTask produceTask = produceTaskList.get(i);
            produceTask.exec();
            List<Chunk> results = produceTask.result();

            for (Chunk chunk : results) {

                Block[] blocks = new Block[partitionChannels.size()];
                for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                    blocks[blockIndex] = chunk.getBlock(partitionChannels.get(blockIndex));
                }
                Chunk keyChunk = new Chunk(chunk.getPositionCount(), blocks);

                for (int position = 0; position < keyChunk.getPositionCount(); position++) {
                    int partition = partitionFunction.getPartition(keyChunk, position);

                    // The partition of elements in key chunk must be equal to parallelism
                    Assert.assertEquals(i, partition);

                }
            }
        }

    }
}
