package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashJoinKeepPartitionTest extends HashJoinTest {
    @Test
    public void testSemiJoin() {
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.of(0, 2, 2, 2),
            IntegerBlock.of(3, null, 9, null));
        inputChunk1.setPartIndex(0);
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(1, 3, 5, 3),
            IntegerBlock.of(3, null, 9, null));
        inputChunk2.setPartIndex(1);
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(inputChunk1)
            .withChunk(inputChunk2)
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 3, 5, 3, 0, null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(0, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null,
                context, true);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        List<Chunk> expects = Arrays.asList(new Chunk(
            IntegerBlock.of(0),
            IntegerBlock.of(3)
        ), new Chunk(
            IntegerBlock.of(1, 3, 5, 3),
            IntegerBlock.of(3, null, 9, null)
        ));
        List<Chunk> results = test.result();
        // check result count
        Assert.assertTrue(results.size() == expects.size());
        // check partition info of result
        Assert.assertTrue(results.get(0).getPartIndex() == 0);
        Assert.assertTrue(results.get(1).getPartIndex() == 1);
        // check match content
        for (int i = 0; i < expects.size(); ++i) {
            assertExecResultByRow(ImmutableList.of(results.get(i)), ImmutableList.of(expects.get(i)), false);
        }
    }

    @Test
    public void testSemiJoin2() {
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(0, 3, 6, 3)
        );
        inputChunk1.setPartIndex(0);
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(1, 4, 4, 1)
        );
        inputChunk2.setPartIndex(2);
        Chunk inputChunk3 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(2, 2, null, 2)
        );
        inputChunk3.setPartIndex(1);
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(inputChunk1)
            .withChunk(inputChunk2)
            .withChunk(null)
            .withChunk(inputChunk1)
            .withChunk(new Chunk(IntegerBlock.of(), IntegerBlock.of()))
            .withChunk(inputChunk3)
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 0, null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null,
                context, true);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        List<Chunk> expects = Arrays.asList(new Chunk(
            IntegerBlock.of(3),
            IntegerBlock.of(0)
        ), new Chunk(
            IntegerBlock.of(3, null),
            IntegerBlock.of(1, 1)
        ), new Chunk(
            IntegerBlock.of(3),
            IntegerBlock.of(0)
        ), new Chunk(
            IntegerBlock.of(3, null, null),
            IntegerBlock.of(2, 2, 2)
        ));
        List<Chunk> results = test.result();
        // check result count
        Assert.assertTrue(results.size() == expects.size());
        // check partition info of result
        Assert.assertTrue(results.get(0).getPartIndex() == 0);
        Assert.assertTrue(results.get(1).getPartIndex() == 2);
        Assert.assertTrue(results.get(2).getPartIndex() == 0);
        Assert.assertTrue(results.get(3).getPartIndex() == 1);
        // check match content
        for (int i = 0; i < expects.size(); ++i) {
            assertExecResultByRow(ImmutableList.of(results.get(i)), ImmutableList.of(expects.get(i)), false);
        }
    }

    @Test
    public void testAntiJoin() {
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(0, 3, 6, 3)
        );
        inputChunk1.setPartIndex(0);
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(1, 4, 4, 1)
        );
        inputChunk2.setPartIndex(2);
        Chunk inputChunk3 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(2, 2, null, 2)
        );
        inputChunk3.setPartIndex(1);
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(inputChunk1)
            .withChunk(inputChunk2)
            .withChunk(null)
            .withChunk(inputChunk1)
            .withChunk(new Chunk(IntegerBlock.of(), IntegerBlock.of()))
            .withChunk(inputChunk3)
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 0)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        List<Chunk> expects = Arrays.asList(new Chunk(
            IntegerBlock.of(null, 9, null),
            IntegerBlock.of(3, 6, 3)
        ), new Chunk(
            IntegerBlock.of(null, 9),
            IntegerBlock.of(4, 4)
        ), new Chunk(
            IntegerBlock.of(null, 9, null),
            IntegerBlock.of(3, 6, 3)
        ));
        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context, true);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        List<Chunk> results = test.result();
        // check result count
        Assert.assertTrue(results.size() == expects.size());
        // check partition info of result
        Assert.assertTrue(results.get(0).getPartIndex() == 0);
        Assert.assertTrue(results.get(1).getPartIndex() == 2);
        Assert.assertTrue(results.get(2).getPartIndex() == 0);
        // check match content
        for (int i = 0; i < expects.size(); ++i) {
            assertExecResultByRow(ImmutableList.of(results.get(i)), ImmutableList.of(expects.get(i)), false);
        }
    }

    @Test
    public void testInnerJoin() {
        Chunk inputChunk1 = new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray())
        );
        inputChunk1.setPartIndex(0);
        Chunk inputChunk2 = new Chunk(
            IntegerBlock.of(0, 4, 4, 0),
            IntegerBlock.of(3, null, 9, null)
        );
        inputChunk2.setPartIndex(2);
        Chunk inputChunk3 = new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1000).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1000).toArray())
        );
        inputChunk3.setPartIndex(1);
        Chunk inputChunk4 = new Chunk(
            IntegerBlock.of(3, null, 9, null),
            IntegerBlock.of(2, 2, 1, 2)
        );
        inputChunk4.setPartIndex(1);
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(inputChunk1)
            .withChunk(inputChunk2)
            .withChunk(null)
            .withChunk(inputChunk1)
            .withChunk(new Chunk(IntegerBlock.of(), IntegerBlock.of()))
            .withChunk(inputChunk3)
            .withChunk(inputChunk4)
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 2, 2, 1, 0, 9, 11)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(0, 0, DataTypes.IntegerType));

        List<Chunk> expects = new ArrayList<>(Arrays.asList(new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray())
        ), new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray())
        ), new Chunk(
            IntegerBlock.of(0, 0),
            IntegerBlock.of(3, null),
            IntegerBlock.of(0, 0)
        ), new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray())
        ), new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 1).limit(1024).toArray())
        ), new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray())
        ), new Chunk(
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 3).limit(1024).toArray()),
            IntegerBlock.wrap(IntStream.generate(() -> 2).limit(1024).toArray())
        )));
        int lastSize = 1000 * 3 - 1024 * 2;
        int[] col1 =
            IntStream.concat(IntStream.generate(() -> 2).limit(lastSize), Arrays.stream(new int[] {9})).toArray();
        int[] col2 =
            IntStream.concat(IntStream.generate(() -> 3).limit(lastSize), Arrays.stream(new int[] {1})).toArray();
        expects.add(new Chunk(IntegerBlock.wrap(col1), IntegerBlock.wrap(col2), IntegerBlock.wrap(col1)));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null,
                context, true);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        List<Chunk> results = test.result();
        // check result count
        Assert.assertTrue(results.size() == expects.size());
        // check partition info of result
        Assert.assertTrue(results.get(0).getPartIndex() == 0);
        Assert.assertTrue(results.get(1).getPartIndex() == 0);
        Assert.assertTrue(results.get(2).getPartIndex() == 2);
        Assert.assertTrue(results.get(3).getPartIndex() == 0);
        Assert.assertTrue(results.get(4).getPartIndex() == 0);
        Assert.assertTrue(results.get(5).getPartIndex() == 1);
        Assert.assertTrue(results.get(6).getPartIndex() == 1);
        Assert.assertTrue(results.get(7).getPartIndex() == 1);

        // check match content
        for (int i = 0; i < expects.size(); ++i) {
            assertExecResultByRow(ImmutableList.of(results.get(i)), ImmutableList.of(expects.get(i)), false);
        }
    }
}
