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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.chunk.StringBlock;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class HashJoinTest extends BaseExecTest {

    private static AsyncFileSingleStreamSpillerFactory spillerFactory;
    private static Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().addAll(spillPaths);
        spillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4);
    }

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 2);
        context.setParamManager(new ParamManager(connectionMap));
    }

    // be compatible with legacy unit test
    static EquiJoinKey mockEquiJoinKey(int outerIndex, int innerIndex, DataType unifiedType) {
        return new EquiJoinKey(outerIndex, innerIndex, unifiedType, false, false);
    }

    static ParallelHashJoinExec mockParallelHashJoinExec(Executor outerInput,
                                                         Executor innerInput,
                                                         JoinRelType joinType,
                                                         boolean maxOneRow,
                                                         List<EquiJoinKey> joinKeys,
                                                         IExpression otherCondition,
                                                         List<IExpression> antiJoinOperands,
                                                         ExecutionContext context) {
        return new ParallelHashJoinExec(
            new ParallelHashJoinExec.Synchronizer(1, false),
            outerInput, innerInput, joinType, maxOneRow,
            joinKeys, otherCondition, antiJoinOperands, false, context, 0);
    }

    static SingleExecTest mockHybridHashJoinExec(List<Chunk> outerChunks,
                                                 List<DataType> outerTypes,
                                                 List<Chunk> innerChunks,
                                                 List<DataType> innerTypes,
                                                 JoinRelType joinType,
                                                 boolean maxOneRow,
                                                 List<EquiJoinKey> joinKeys,
                                                 IExpression otherCondition,
                                                 List<IExpression> antiJoinOperands,
                                                 ExecutionContext context, int bucketNum) {
        List<Integer> outerKeyColumns = joinKeys.stream().map(t -> t.getOuterIndex()).collect(Collectors.toList());
        List<Integer> innerKeyColumns = joinKeys.stream().map(t -> t.getInnerIndex()).collect(Collectors.toList());

        BucketMockExec innerBucketInput = new BucketMockExec(innerTypes, innerChunks, bucketNum, innerKeyColumns);
        BucketMockExec outerBucketInput = new BucketMockExec(outerTypes, outerChunks, bucketNum, outerKeyColumns);

        HybridHashJoinExec exec = new HybridHashJoinExec(
            outerBucketInput, innerBucketInput, joinType, maxOneRow,
            joinKeys, otherCondition, antiJoinOperands, context, 1, 0, bucketNum, spillerFactory);
        return new SingleExecTest.Builder(exec, innerBucketInput).build();
    }

    @Test
    public void testInnerJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, "f", "d", "c", null, "a"))), false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.INNER, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, "f", "d", "c", null, "a"))), false);
        }
    }

    @Test
    public void testInnerJoin_MultiKey() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4),
                IntegerBlock.of(1, 1, 2, 2, null),
                StringBlock.of("a", "b", "a", "b", "a")))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8, 9),
                IntegerBlock.of(3, 3, 4, 4, 4),
                StringBlock.of("a", "b", "a", "b", null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "a", "a", null),
                StringBlock.of("A", "B", "C", "D")))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, null),
                StringBlock.of("a", "b", "c", "b"),
                StringBlock.of("E", "F", "G", "H")))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType),
            mockEquiJoinKey(2, 1, DataTypes.StringType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 0, 2, 3, 5),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            StringBlock.of("E", "A", "B", "F", "C")
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.INNER, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 0, 2, 3, 5),
                IntegerBlock.of(1, 1, 2, 2, 3),
                StringBlock.of("a", "a", "a", "b", "a"),
                IntegerBlock.of(1, 1, 2, 2, 3),
                StringBlock.of("a", "a", "a", "b", "a"),
                StringBlock.of("E", "A", "B", "F", "C")
            )), false);
        }
    }

    @Test
    public void testLeftOuterJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a")
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a")
            )), false);
        }
    }

    @Test
    public void testLeftOuterJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(3), "d");
            }
        };

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, condition, null,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, null, 7, null, 3, 8, 1),
            StringBlock.of("c", null, null, "f", null, "c", null, "a")
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, false, joinKeys, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                IntegerBlock.of(3, 4, null, 7, null, 3, 8, 1),
                StringBlock.of("c", null, null, "f", null, "c", null, "a")
            )), false);
        }

    }

    @Test
    public void testLeftOuterJoin_MultiKey() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4),
                IntegerBlock.of(1, 1, 2, 2, null),
                StringBlock.of("a", "b", "a", "b", "a")))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8, 9),
                IntegerBlock.of(3, 3, 4, 4, 4),
                StringBlock.of("a", "b", "a", "b", null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "a", "a", null),
                StringBlock.of("A", "B", "C", "D")))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, null),
                StringBlock.of("a", "b", "c", "b"),
                StringBlock.of("E", "F", "G", "H")))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType),
            mockEquiJoinKey(2, 1, DataTypes.StringType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
            StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null),
            IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
            StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
            StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
                StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null),
                IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
                StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
                StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null)
            )), false);
        }

    }

    @Test
    public void testRightOuterJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a"),
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.RIGHT, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a"),
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1)
            )), false);
        }

    }

    @Test
    public void testRightOuterJoin_MultiKey() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4),
                IntegerBlock.of(1, 1, 2, 2, null),
                StringBlock.of("a", "b", "a", "b", "a")))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8, 9),
                IntegerBlock.of(3, 3, 4, 4, 4),
                StringBlock.of("a", "b", "a", "b", null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "a", "a", null),
                StringBlock.of("A", "B", "C", "D")))
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, null),
                StringBlock.of("a", "b", "c", "b"),
                StringBlock.of("E", "F", "G", "H")))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType),
            mockEquiJoinKey(2, 1, DataTypes.StringType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
            StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
            StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null),
            IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
            StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.RIGHT, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
                StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
                StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null),
                IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
                StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null)
            )), false);
        }

    }

    @Test
    public void testSemiJoin() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 4, 5, 7),
            IntegerBlock.of(3, 4, 5, 3, 1)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.SEMI, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 4, 5, 7),
                IntegerBlock.of(3, 4, 5, 3, 1)
            )), false);
        }

    }

    @Test
    public void testSemiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.emptyList(), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.SEMI, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.emptyList(), false);
        }

    }

    @Test
    public void testAntiJoin_NotExists() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6, 7),
            IntegerBlock.of(9, 7, 8, null)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(2, 3, 6, 7),
                IntegerBlock.of(9, 7, 8, null)
            )), false);
        }

    }

    @Test
    public void testAntiJoin_NotIn() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6),
            IntegerBlock.of(9, 7, 8)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(2, 3, 6),
                IntegerBlock.of(9, 7, 8)
            )), false);
        }

    }

    @Test
    public void testAntiJoin_NotIn_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, null, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(4, 5, 6, 7),
            IntegerBlock.of(5, null, 8, null)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, null, 8, null)
            )), false);
        }

    }

    @Test
    public void testAntiJoin_NotIn_InnerContainsNull() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, null, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.emptyList(), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.emptyList(), false);
        }

    }

    @Test
    public void testAntiJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), 5);
            }
        };

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, condition, null,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 4, 6, 7),
            IntegerBlock.of(9, 7, 5, 8, null)
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(2, 3, 4, 6, 7),
                IntegerBlock.of(9, 7, 5, 8, null)
            )), false);
        }

    }

    @Test
    public void testInnerSingleJoin() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.StringType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(5, 6, 7, 8)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 1, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.INNER, true, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, "f", "d", "c", null, "a")
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.INNER, true, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, "f", "d", "c", null, "a")
            )), false);
        }

    }

    @Test
    public void testInnerSingleJoin_withError() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.StringType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(4, 5, 6, 7)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 1, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.INNER, true, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
        }
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.INNER, false, joinKeys, null, null, context,
                bucketNum);
            try {
                test.exec();
            } catch (TddlRuntimeException e) {
                assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
            }
        }

    }

    @Test
    public void testLeftSingleJoin() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.StringType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(5, 6, 7, 8)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 1, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a")
        )), false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, true, joinKeys, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a")
            )), false);
        }

    }

    @Test
    public void testLeftSingleJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.StringType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(5, 6, 7, 8)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 1, DataTypes.IntegerType));

        IExpression condition = condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), "d");
            }
        };

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, condition, null,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", null, "c", null, "a")
        )), false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, true, joinKeys, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", null, "c", null, "a")
            )), false);
        }

    }

    @Test
    public void testLeftSingleJoin_withError() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.StringType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(4, 5, 6, 7)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 1, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
        }
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.LEFT, true, joinKeys, null, null, context,
                bucketNum);
            try {
                test.exec();
            } catch (TddlRuntimeException e) {
                assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
            }
        }
    }

}
