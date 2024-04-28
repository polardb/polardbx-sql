package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.operator.HashAggExec;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.executor.operator.SingleExecTest;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SliceIntGroupByTest extends BaseExecTest {

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

    // Test Group by slice, int that not in dictionary
    @Test
    public void testNormalSlice() {
        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 7))
            )
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 1, 5, 3, 8, 1))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
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
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(9, 12, 27, 21, 15, 9, 24, 3)
        )), false);
    }

    // Test Group by dict, int in same dictionary
    @Test
    public void testDictSlice() {

        BlockDictionary dictionary = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 7))
            )
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 1, 5, 3, 8, 1))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
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
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(9, 12, 27, 21, 15, 9, 24, 3)
        )), false);
    }

    // Test Group by dict, int in different dictionary
    @Test
    public void testMultiDictSlice() {

        BlockDictionary dictionary1 = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        BlockDictionary dictionary2 = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abcd"), Slices.utf8Slice("abc"), Slices.utf8Slice("ab"),
            Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary1,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 7))
            )
            .withChunk(new Chunk(
                sliceOfDict(dictionary2,
                    1, 3, 2, 1, 1, 3, 2, 1, 1, 3, 2, 1
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 1, 5, 3, 8, 1))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
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
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(9, 12, 27, 21, 15, 9, 24, 3)
        )), false);
    }

    // Test Group by dict, int and Group by slice, int in mixed.
    @Test
    public void testMixSlice() {

        BlockDictionary dictionary = new LocalBlockDictionary(new Slice[] {
            Slices.utf8Slice("abc"), Slices.utf8Slice("ab"), Slices.utf8Slice("a")});

        MockExec inputExec = MockExec.builder(DataTypes.VarcharType, DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                sliceOfDict(dictionary,
                    0, 2, 1, 0, 0, 2, 1, 0, 0, 2, 1, 0
                ),
                IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7, 3, 4, 9, 7, 3, 4, 9, 7))
            )
            .withChunk(new Chunk(
                sliceOf(context,
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                    Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                    Slices.utf8Slice("ab"), Slices.utf8Slice("abc")
                ),
                IntegerBlock.of(7, 6, 9, 4, 7, 6, 9, 4, 7, 6, 9, 4),
                IntegerBlock.of(5, 3, 8, 1, 5, 3, 8, 1, 5, 3, 8, 1))
            )
            .build();

        // group key: slice and int
        int[] groups = {0, 1};

        // aggregators
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new SumV2(2, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));

        // outputColumnMeta
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.VarcharType);
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
            sliceOf(context,
                Slices.utf8Slice("abc"), Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"), Slices.utf8Slice("abc"),
                Slices.utf8Slice("abc"),
                Slices.utf8Slice("a"),
                Slices.utf8Slice("ab"),
                Slices.utf8Slice("abc")
            ),
            IntegerBlock.of(0, 1, 2, 3, 7, 6, 9, 4),
            IntegerBlock.of(9, 12, 27, 21, 15, 9, 24, 3)
        )), false);
    }
}
