package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.operator.HashAggExec;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.executor.operator.SingleExecTest;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupByNullTest extends BaseExecTest {
    private int aggHashTableSize = 2;
    private boolean compatible = false;

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);

        // open vectorization implementation of agg.
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_OSS_COMPATIBLE.getName(), compatible);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testGroupByIntNull() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
                IntegerBlock.of(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 2, 0, null),
            IntegerBlock.of(1, 5, 9, 21)
        )), false);
    }

    @Test
    public void testGroupByIntIntNull() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 0, 1, 2, 0, 1, 0),
                IntegerBlock.of(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 0, 1, 2, 0, 1),
            IntegerBlock.of(1, 1, 2, 2, 0, 0, null, null),
            IntegerBlock.of(0, 1, 2, 0, 1, 2, 0, 1)
        )), false);
    }

    @Test
    public void testGroupByIntNullIntNull() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, null, 2, null, 1, 2, 0, 1, 0),
                IntegerBlock.of(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, null, 2, null, 1, 2, 0, 1),
            IntegerBlock.of(1, 1, 2, 2, 0, 0, null, null),
            IntegerBlock.of(0, null, 2, null, 1, 2, 0, 1)
        )), false);
    }

    @Test
    public void testGroupByLongNull() {
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.ofInt(0, 1, 2, 3, 4, 5, 6, 7, 8),
                LongBlock.ofInt(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.ofInt(1, 2, 0, null),
            LongBlock.ofInt(1, 5, 9, 21)
        )), false);
    }

    @Test
    public void testGroupByLongLongNull() {
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.ofInt(0, 1, 2, 0, 1, 2, 0, 1, 0),
                LongBlock.ofInt(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.ofInt(0, 1, 2, 0, 1, 2, 0, 1),
            LongBlock.ofInt(1, 1, 2, 2, 0, 0, null, null),
            LongBlock.ofInt(0, 1, 2, 0, 1, 2, 0, 1)
        )), false);
    }

    @Test
    public void testGroupByLongNullLongNull() {
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.ofInt(0, null, 2, null, 1, 2, 0, 1, 0),
                LongBlock.ofInt(1, 1, 2, 2, 0, 0, null, null, null))
            )
            .build();

        // group key int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(0, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, aggHashTableSize, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> results = test.result();
        for (Chunk result : results) {
            for (int pos = 0; pos < result.getPositionCount(); pos++) {
                System.out.println(stringify(result.rowAt(pos)));
            }
        }

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.ofInt(0, null, 2, null, 1, 2, 0, 1),
            LongBlock.ofInt(1, 1, 2, 2, 0, 0, null, null),
            LongBlock.ofInt(0, null, 2, null, 1, 2, 0, 1)
        )), false);
    }
}
