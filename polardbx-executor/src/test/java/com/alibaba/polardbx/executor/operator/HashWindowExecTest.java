package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class HashWindowExecTest extends BaseExecTest {

    private static String TABLE_NAME = "MOCK_HASH_AGG_TABLE";

    private static String COLUMN_PREFIX = "MOCK_HASH_AGG_COLUMN_";

    private static int DEFAULT_AGG_HASH_TABLE_SIZE = 1024;

    @Test
    public void testHashWindowSimpleAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);

        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals = {
            new BigDecimal("4.0000"), new BigDecimal("3.5000"), new BigDecimal("8.5000"),
            new BigDecimal("4.0000"), new BigDecimal("4.0000"), new BigDecimal("3.5000"), new BigDecimal("8.5000"),
            new BigDecimal("4.0000")};
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal);

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            decimalBlockBuilder.build()
        )), false);

    }

    @Test
    public void testHashWindowSimpleCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6),
                IntegerBlock.of(5, 3, 8, 1, 2, 3, 4)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l, 4l, 5l, 6l),
            LongBlock.of(3l, 4l, 9l, 7l, 5l, 3l, 8l, 1l, 2l, 3l, 4l),
            LongBlock.of(2l, 2l, 2l, 2l, 2l, 2l, 2l, 2l, 1l, 1l, 1l)
        )), false);
    }

    @Test
    public void testHashWindowMultColCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6),
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6),
                IntegerBlock.of(5, 3, 8, 1, 2, 3, 4)))
            .build();
        /** groups */
        int[] groups = {0, 1};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {2};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l, 4l, 5l, 6l),
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l, 4l, 5l, 6l),
            LongBlock.of(3l, 4l, 9l, 7l, 5l, 3l, 8l, 1l, 2l, 3l, 4l),
            LongBlock.of(2l, 2l, 2l, 2l, 2l, 2l, 2l, 2l, 1l, 1l, 1l)
        )), false);
    }

    protected void prepareParams(boolean enableVecAgg) {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1600);

        // open vectorization implementation of aggregator
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), enableVecAgg);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testHashWindowCountDistinct() {
        // vec agg not support distinct
        prepareParams(false);
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6),
                IntegerBlock.of(5, 4, null, 1, null, 3, 4)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l, 4l, 5l, 6l),
            LongBlock.of(3l, 4l, 9l, 7l, 5l, 4l, null, 1l, null, 3l, 4l),
            LongBlock.of(2l, 1l, 1l, 2l, 2l, 1l, 1l, 2l, 0l, 1l, 1l)
        )), false);
    }

    @Test
    public void testHashWindowSimpleSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4),
                IntegerBlock.of(3, 4, 9, 7, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 5),
                IntegerBlock.of(5, 3, 8, 1, 5)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 0, 1, 2, 3, 5),
            IntegerBlock.of(3, 4, 9, 7, null, 5, 3, 8, 1, 5),
            IntegerBlock.of(8, 7, 17, 8, null, 8, 7, 17, 8, 5)
        )), false);
    }

    @Test
    public void testHashWindowLargeChunkSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
                IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1000).toArray())))
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1024).toArray()),
                IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray())))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> result = new ArrayList<>();
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(1000, 1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(24).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(24).toArray())
        ));
        assertExecResultByRow(test.result(), result, false);
    }

    private boolean[] getBooleanArray(int size, Predicate<Integer> filter) {
        boolean[] result = new boolean[size];
        for (int i = 0; i < size; ++i) {
            result[i] = filter.test(i);
        }
        return result;
    }

    @Test
    public void testHashWindowLargeChunkSumWithNull() {
        prepareParams(false);
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
                IntegerBlock.wrapWithNull(IntStream.generate(() -> 1).limit(1000).toArray(),
                    getBooleanArray(1000, i -> i % 2 == 0))
            ))
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
                IntegerBlock.wrapWithNull(IntStream.generate(() -> 1).limit(1000).toArray(),
                    getBooleanArray(1000, i -> i % 2 == 1))
            ))
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
                IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1000).toArray())
            ))
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
                IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray())
            ))
            .withChunk(new Chunk(
                IntegerBlock.wrap(IntStream.range(2000, 3500).toArray()),
                IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1500).toArray())
            ))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, true, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        List<Chunk> result = new ArrayList<>();
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrapWithNull(IntStream.generate(() -> 1).limit(1000).toArray(),
                getBooleanArray(1000, i -> i % 2 == 0)),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrapWithNull(IntStream.generate(() -> 1).limit(1000).toArray(),
                getBooleanArray(1000, i -> i % 2 == 1)),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(0, 1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(2000, 3000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray())
        ));
        result.add(new Chunk(
            IntegerBlock.wrap(IntStream.range(3000, 3500).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(500).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(500).toArray())
        ));
        assertExecResultByRow(test.result(), result, false);
    }

    @Test
    public void testHashWindowLargeNumerberBigDecimalSum() {
        DecimalBlockBuilder decimalBlockBuilder1 = new DecimalBlockBuilder(256);
        DecimalBlockBuilder decimalBlockBuilder2 = new DecimalBlockBuilder(256);
        DecimalBlockBuilder decimalBlockBuilder3 = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals = {
            new BigDecimal("100000000000000000000000000000000000000000000000000.001"),
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("300000000000000000000000000000000000000000000000000.003"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004")};
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(d -> {
            decimalBlockBuilder1.writeDecimal(d);
            decimalBlockBuilder2.writeDecimal(d);
            decimalBlockBuilder3.writeDecimal(d);
        });
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder3::writeDecimal);

        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                decimalBlockBuilder1.build()
            ))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                decimalBlockBuilder1.build()))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        outputColumn.add(DataTypes.DecimalType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        IntStream.range(0, 2).forEach((i) -> Arrays.stream(new BigDecimal[] {
            new BigDecimal("200000000000000000000000000000000000000000000000000.002"),
            new BigDecimal("400000000000000000000000000000000000000000000000000.004"),
            new BigDecimal("600000000000000000000000000000000000000000000000000.006"),
            new BigDecimal("800000000000000000000000000000000000000000000000000.008")
        }).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal));

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            decimalBlockBuilder3.build(),
            decimalBlockBuilder
        )), false);
    }

    @Test
    public void testHashWindowBigDecimalNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);
        outputColumn.add(DataTypes.DecimalType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        IntStream.range(0, 8).forEach((i) -> decimalBlockBuilder.appendNull());

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            IntegerBlock.of(null, null, null, null, null, null, null, null),
            decimalBlockBuilder.build()
        )), false);
    }

    @Test
    public void testHashWindowDoubleNullAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DoubleType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            IntegerBlock.of(null, null, null, null, null, null, null, null),
            DoubleBlock.of(null, null, null, null, null, null, null, null)
        )), false);
    }

    @Test
    public void testHashWindowDoubleNullAvg2() {
        MockExec inputExec = MockExec.builder(DataTypes.LongType, DataTypes.LongType)
            .withChunk(new Chunk(
                LongBlock.of(0l, 1l, 2l, 3l),
                LongBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                LongBlock.of(0l, 1l, 2l, 3l),
                LongBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.DoubleType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l),
            LongBlock.of(null, null, null, null, null, null, null, null),
            DoubleBlock.of(null, null, null, null, null, null, null, null)
        )), false);
    }

    @Test
    public void testHashWindowNullSum() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.IntegerType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            IntegerBlock.of(null, null, null, null, null, null, null, null),
            IntegerBlock.of(null, null, null, null, null, null, null, null)
        )), false);
    }

    @Test
    public void testHashWindowNullCount() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, null, null, null)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        int[] targetIndex = {1};
        aggregators.add(new CountV2(targetIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        outputColumn.add(DataTypes.LongType);
        HashWindowExec exec =
            new HashWindowExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            LongBlock.of(0l, 1l, 2l, 3l, 0l, 1l, 2l, 3l),
            LongBlock.of(null, null, null, null, null, null, null, null),
            LongBlock.of(0l, 0l, 0l, 0l, 0l, 0l, 0l, 0l)
        )), false);
    }
}

