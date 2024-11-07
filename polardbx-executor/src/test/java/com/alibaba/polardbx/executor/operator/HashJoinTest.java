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
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SyncFileCleaner;
import com.alibaba.polardbx.executor.operator.util.EquiJoinMockData;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.NotEqual;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_VEC_BUILD_JOIN_ROW;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_VEC_JOIN;
import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class HashJoinTest extends BaseExecTest {

    private static AsyncFileSingleStreamSpillerFactory spillerFactory;
    private static final Path tempPath = Paths.get("./tmp/" + UUID.randomUUID());

    @BeforeClass
    public static void beforeClass() {
        List<Path> spillPaths = new ArrayList<>();
        spillPaths.add(tempPath);
        spillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4);
    }

    // be compatible with legacy unit test
    static EquiJoinKey mockEquiJoinKey(int outerIndex, int innerIndex, DataType unifiedType) {
        return new EquiJoinKey(outerIndex, innerIndex, unifiedType, false);
    }

    static ParallelHashJoinExec mockParallelHashJoinExec(Executor outerInput,
                                                         Executor innerInput,
                                                         JoinRelType joinType,
                                                         boolean maxOneRow,
                                                         List<EquiJoinKey> joinKeys,
                                                         IExpression otherCondition,
                                                         List<IExpression> antiJoinOperands,
                                                         ExecutionContext context,
                                                         boolean keepPartition) {
        return mockParallelHashJoinExec(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition,
            antiJoinOperands, context, false, keepPartition);
    }

    static ParallelHashJoinExec mockParallelHashJoinExec(Executor outerInput,
                                                         Executor innerInput,
                                                         JoinRelType joinType,
                                                         boolean maxOneRow,
                                                         List<EquiJoinKey> joinKeys,
                                                         IExpression otherCondition,
                                                         List<IExpression> antiJoinOperands,
                                                         ExecutionContext context,
                                                         boolean outerDriver,
                                                         boolean keepPartition) {
        return new ParallelHashJoinExec(
            new Synchronizer(joinType, outerDriver, 1, false, 1),
            outerInput, innerInput, joinType, maxOneRow,
            joinKeys, otherCondition, antiJoinOperands, outerDriver, context, 0, 1, keepPartition);
    }

    static SingleExecTest mockParallelHashJoinExec(EquiJoinMockData mockData,
                                                   JoinRelType joinType,
                                                   boolean maxOneRow,
                                                   IExpression otherCondition,
                                                   List<IExpression> antiJoinOperands,
                                                   ExecutionContext context) {
        return mockParallelHashJoinExec(mockData, joinType, maxOneRow, otherCondition, antiJoinOperands, context,
            false);
    }

    static SingleExecTest mockParallelHashJoinExec(EquiJoinMockData mockData,
                                                   JoinRelType joinType,
                                                   boolean maxOneRow,
                                                   IExpression otherCondition,
                                                   List<IExpression> antiJoinOperands,
                                                   ExecutionContext context,
                                                   boolean outerDriver) {
        MockExec innerInput = new MockExec(mockData.getInnerTypes(), mockData.getInnerChunks());
        MockExec outerInput = new MockExec(mockData.getOuterTypes(), mockData.getOuterChunks());
        ParallelHashJoinExec exec = new ParallelHashJoinExec(
            new Synchronizer(joinType, outerDriver, 1, false, 1),
            outerInput, innerInput, joinType, maxOneRow,
            mockData.getEquiJoinKeysAndReset(), otherCondition, antiJoinOperands, outerDriver, context, 0, 1, false);
        return new SingleExecTest.Builder(exec, outerDriver ? outerInput : innerInput).build();
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

    static SingleExecTest mockHybridHashJoinExec(EquiJoinMockData mockData,
                                                 JoinRelType joinType,
                                                 boolean maxOneRow,
                                                 IExpression otherCondition,
                                                 List<IExpression> antiJoinOperands,
                                                 ExecutionContext context, int bucketNum) {
        return mockHybridHashJoinExec(
            mockData.getOuterChunks(), mockData.getOuterTypes(), mockData.getInnerChunks(), mockData.getInnerTypes(),
            joinType, maxOneRow, mockData.getEquiJoinKeysAndReset(), otherCondition, antiJoinOperands, context,
            bucketNum);
    }

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testInnerJoin_Simple() {
        List<DataType> outTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, "f", "d", "c", null, "a"));
        List<Chunk> expects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .build();
        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(1000, null, null, "XX")
            .build();
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.INNER, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
        test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.INNER, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.INNER, false, null, null, context,
                bucketNum);
            test.exec();

            assertExecResultByRow(test.result(), expects, false);
            EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.INNER, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }
    }

    @Test
    public void testInnerJoin_MultiKey() {
        List<DataType> outTypes = ImmutableList.of(
            DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType,
            DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 0, 2, 3, 5),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            StringBlock.of("E", "A", "B", "F", "C"));
        List<Chunk>[] expects = new List[4];

        expects[0] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            build();
        expects[1] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            row(8108, null, "XX", null, "XX", "YN").
            build();
        expects[2] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            row(8018, 1000, null, 1000, null, "NY").
            build();
        expects[3] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            row(8108, null, "XX", null, "XX", "YN").
            row(8018, 1000, null, 1000, null, "NY").
            row(8008, null, null, null, null, "NN").
            build();

        for (int nullSafeMask = 0; nullSafeMask < 4; nullSafeMask++) {
            List<Chunk> expect = expects[nullSafeMask];
            EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);
            SingleExecTest test =
                mockParallelHashJoinExec(EquiJoinMockData.MULTI_KEY_CASE, JoinRelType.INNER, false, null, null,
                    context);
            test.exec();

            assertExecResultByRow(test.result(), expect, false);
            for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
                EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);
                test = mockHybridHashJoinExec(
                    EquiJoinMockData.MULTI_KEY_CASE,
                    JoinRelType.INNER, false, null, null, context,
                    bucketNum);
                test.exec();
                assertExecResultByRow(test.result(), expect, false);
            }

        }
    }

    @Test
    public void testLeftOuterJoin_Simple() {
        List<DataType> outTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a"));

        List<Chunk> expects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(1000, null, null, null)
            .build();
        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(1000, null, null, "XX")
            .build();

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.LEFT, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
        test = mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.LEFT, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.LEFT, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.LEFT, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);
        }
    }

    @Test
    public void testLeftOuterJoin_WithCondition() {
        List<DataType> outTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            IntegerBlock.of(3, 4, null, 7, null, 3, 8, 1),
            StringBlock.of("c", null, null, "f", null, "c", null, "a"));

        List<Chunk> expects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(1000, null, null, null)
            .build();
        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(1000, null, null, "XX")
            .build();

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(3), "d");
            }
        };

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.LEFT, false, condition, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
        test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.LEFT, false, condition, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.LEFT, false, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.LEFT, false, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }

    }

    @Test
    public void testLeftOuterJoin_MultiKey() {
        List<DataType> outTypes = ImmutableList.of(
            DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType,
            DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
            StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null),
            IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
            StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
            StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null));
        List<Chunk>[] expects = new List[4];

        expects[0] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8008, null, null, null, null, null).
            row(8108, null, "XX", null, null, null).
            row(8018, 1000, null, null, null, null).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            build();
        expects[1] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8008, null, null, null, null, null).
            row(8108, null, "XX", null, "XX", "YN").
            row(8018, 1000, null, null, null, null).
            row(8118, 1000, "XX", 1000, "XX", "YY").
            build();
        expects[2] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8008, null, null, null, null, null).
            row(8108, null, "XX", null, null, null).
            row(8018, 1000, null, 1000, null, "NY").
            row(8118, 1000, "XX", 1000, "XX", "YY").
            build();
        expects[3] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(8008, null, null, null, null, "NN").
            row(8108, null, "XX", null, "XX", "YN").
            row(8018, 1000, null, 1000, null, "NY").
            row(8118, 1000, "XX", 1000, "XX", "YY").
            build();

        for (int nullSafeMask = 0; nullSafeMask < 4; nullSafeMask++) {
            List<Chunk> expect = expects[nullSafeMask];
            EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);
            SingleExecTest test =
                mockParallelHashJoinExec(EquiJoinMockData.MULTI_KEY_CASE, JoinRelType.LEFT, false, null, null, context);
            test.exec();
            assertExecResultByRow(test.result(), expect, false);
            for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
                EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);
                test = mockHybridHashJoinExec(
                    EquiJoinMockData.MULTI_KEY_CASE, JoinRelType.LEFT, false, null, null, context, bucketNum);
                test.exec();
                assertExecResultByRow(test.result(), expect, false);
            }
        }
    }

    @Test
    public void testRightOuterJoin_Simple() {
        List<DataType> outTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType, DataTypes.IntegerType, DataTypes.IntegerType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a"),
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1));
        List<Chunk> expects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(null, null, 1000, null)
            .build();

        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes)
            .addChunk(baseExpect)
            .row(null, "XX", 1000, null)
            .build();

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.RIGHT, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
        test =
            mockParallelHashJoinExec(EquiJoinMockData.SIMPLE_CASE, JoinRelType.RIGHT, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.RIGHT, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SIMPLE_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SIMPLE_CASE,
                JoinRelType.RIGHT, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }

    }

    @Test
    public void testRightOuterJoin_MultiKey() {
        List<DataType> outTypes = ImmutableList.of(
            DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType,
            DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);

        Chunk baseExpect = new Chunk(
            IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
            StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
            StringBlock.of("E", "A", null, "B", "F", null, "C", null, null, null, null),
            IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
            StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null)
        );
        List<Chunk>[] expects = new List[4];

        expects[0] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(null, null, null, 8008, null, null).
            row(null, null, null, 8108, null, "XX").
            row(null, null, null, 8018, 1000, null).
            row(1000, "XX", "YY", 8118, 1000, "XX").
            build();
        expects[1] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(null, null, null, 8008, null, null).
            row(null, "XX", "YN", 8108, null, "XX").
            row(null, null, null, 8018, 1000, null).
            row(1000, "XX", "YY", 8118, 1000, "XX").
            build();
        expects[2] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(null, null, null, 8008, null, null).
            row(null, null, null, 8108, null, "XX").
            row(1000, null, "NY", 8018, 1000, null).
            row(1000, "XX", "YY", 8118, 1000, "XX").
            build();
        expects[3] = rowChunksBuilder(outTypes).addChunk(baseExpect).
            row(null, null, "NN", 8008, null, null).
            row(null, "XX", "YN", 8108, null, "XX").
            row(1000, null, "NY", 8018, 1000, null).
            row(1000, "XX", "YY", 8118, 1000, "XX").
            build();
        for (int nullSafeMask = 0; nullSafeMask < 4; nullSafeMask++) {
            List<Chunk> expect = expects[nullSafeMask];
            EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);

            SingleExecTest test =
                mockParallelHashJoinExec(EquiJoinMockData.MULTI_KEY_CASE, JoinRelType.RIGHT, false, null, null,
                    context);
            test.exec();
            assertExecResultByRow(test.result(), expect, false);
            for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
                EquiJoinMockData.MULTI_KEY_CASE.setKeyIsNullSafeWithMask(nullSafeMask);
                test = mockHybridHashJoinExec(
                    EquiJoinMockData.MULTI_KEY_CASE,
                    JoinRelType.RIGHT, false, null, null, context,
                    bucketNum);
                test.exec();
                assertExecResultByRow(test.result(), expect, false);
            }
        }
    }

    @Test
    public void testSemiJoin() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 4, 5),
            IntegerBlock.of(3, 4, 5, 3)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.SEMI, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SEMI_CASE,
                JoinRelType.SEMI, false, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);
        }

    }

    @Test
    public void testInnerLongJoinVec() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(0L, 0L, 1L, 1L, 4L, 5L, 5L),
            LongBlock.of(3L, 3L, 4L, 4L, 5L, 3L, 3L),
            LongBlock.of(3L, 3L, 4L, 4L, 5L, 3L, 3L)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_CASE, JoinRelType.INNER, false, null, null, context);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.LongProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    /**
     * innerKey和outerKey的类型不同
     */
    @Test
    public void testInnerIntJoinLong2() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(3L, 3L, 4L, 3L, 3L, 4L, 5L),
            IntegerBlock.of(0, 5, 1, 0, 5, 1, 4),
            IntegerBlock.of(3, 3, 4, 3, 3, 4, 5),
            IntegerBlock.of(6, 6, 7, 4, 4, 5, 6)
        ));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.INT_JOIN_LONG_CASE, JoinRelType.INNER, false, null, null,
                context);
        // 目前只能走非向量化形式
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractBufferedJoinExec.DefaultProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testInnerIntJoinVec() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 0, 1, 3, 4, 5, 5),
            IntegerBlock.of(3, 3, 4, 7, 5, 3, 3),
            IntegerBlock.of(3, 3, 4, 7, 5, 3, 3)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.INNER_INT_CASE, JoinRelType.INNER, false, null, null, context);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.IntProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testSemiLongJoinVec() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(0L, 1L, 4L, 5L),
            LongBlock.of(3L, 4L, 5L, 3L)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_CASE, JoinRelType.SEMI, false, null, null, context);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.SemiLongProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

//    @Test
//    public void testSemiLongJoinNotEqIntVec() {
//        enableVecJoin();
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(1), new InputRefExpression(3)), new NotEqual(null, null), context);
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            LongBlock.of(1L, 3L, 4L, 7L),
//            IntegerBlock.of(4, 7, 5, 10),
//            LongBlock.of(12L, 14L, 15L, 18L)));
//
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_NOT_EQ_INT_CASE, JoinRelType.SEMI, false,
//                condition, null, context);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.SemiLongNotEqIntegerProbeOperator);
//
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

    @Test
    public void testSemiLongJoinNotEqLongVec() {
        enableVecJoin();

        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
            ImmutableList.of(new InputRefExpression(1), new InputRefExpression(5)), new NotEqual(), context);

        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(1L, 3L, 4L, 7L),
            IntegerBlock.of(4, 7, 5, 10),
            LongBlock.of(12L, 14L, 15L, 18L)));

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_NOT_EQ_LONG_CASE, JoinRelType.SEMI, false,
                condition, null, context);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.SemiLongNotEqLongProbeOperator);

        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseSemiJoin() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 4, 5),
            IntegerBlock.of(3, 4, 5, 3)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.SEMI, false,
                null, null, context, true);

        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseSemiJoin2() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 4, 5),
            IntegerBlock.of(3, 4, 5, 3)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE_2, JoinRelType.SEMI, false,
                null, null, context, true);

        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testSemiJoinWithCondition() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 5),
            IntegerBlock.of(3, 4, 3)));
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), 5);
            }
        };
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.SEMI, false,
                condition, null, context, false);

        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseSemiJoinWithCondition() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 4, 5),
            IntegerBlock.of(3, 5, 3)));
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(0), 1);
            }
        };
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.SEMI, false,
                condition, null, context, true);

        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseSemiLongJoinVec() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(0L, 1L, 4L, 5L),
            LongBlock.of(3L, 4L, 5L, 3L)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_CASE, JoinRelType.SEMI,
                false, null, null, context, true);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseSemiLongProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseSemiIntJoinVec() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 4, 5),
            IntegerBlock.of(3, 4, 5, 3)));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.REVERSE_SEMI_INT_CASE, JoinRelType.SEMI,
                false, null, null, context, true);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseSemiIntProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }
//
//    @Test
//    public void testReverseSemiLongNotEqIntJoinVec() {
//        enableVecJoin();
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(1), new InputRefExpression(5)), new NotEqual(), context);
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            LongBlock.of(1L, 3L, 4L, 7L),
//            IntegerBlock.of(4, 7, 5, 10),
//            LongBlock.of(12L, 14L, 15L, 18L)));
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_NOT_EQ_INT_CASE, JoinRelType.SEMI,
//                false, condition, null, context, true);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseSemiLongNotEqIntegerProbeOperator);
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

//    /**
//     * This join condition is not implemented with vectorization.
//     * This case is for correctness validation.
//     */
//    @Test
//    public void testSemiIntNotEqIntJoin() {
//        enableVecJoin();
//        NotEqual notEqual = new NotEqual(null, null);
//        notEqual.setResultField(new Field(new LongType()));
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(1), new InputRefExpression(4)), notEqual, context);
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            IntegerBlock.of(1, 3, 4),
//            IntegerBlock.of(4, 9, 5)));
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.SEMI_INT_NOT_EQ_INT_CASE, JoinRelType.SEMI,
//                false, condition, null, context);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.DefaultProbeOperator);
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

//    @Test
//    public void testReverseSemiIntNotEqIntJoinVec() {
//        enableVecJoin();
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(1), new InputRefExpression(4)), new NotEqual(), context);
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            IntegerBlock.of(1, 3, 4),
//            IntegerBlock.of(4, 9, 5)));
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.SEMI_INT_NOT_EQ_INT_CASE, JoinRelType.SEMI,
//                false, condition, null, context, true);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseSemiIntNotEqIntegerProbeOperator);
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

    @Test
    public void testSemiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null,
                context, false);
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
    public void testReverseSemiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null,
                context, true, false);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.emptyList(), false);
    }

    @Test
    public void testAntiJoin_NotExists() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6, 7),
            IntegerBlock.of(9, 7, 8, null)
        ));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.ANTI, false, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseAntiJoin_NotExists() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6, 7),
            IntegerBlock.of(9, 7, 8, null)
        ));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.ANTI, false, null, null, context, true);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testAntiJoin_NotExistsWithCondition() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 2, 3, 6, 7, null),
            IntegerBlock.of(3, 9, 7, 8, null, null)
        ));

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(0), 0);
            }
        };
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE_2, JoinRelType.ANTI, false, condition, null, context,
                false);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseAntiJoin_NotExistsWithCondition() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 2, 3, 6, 7, null),
            IntegerBlock.of(3, 9, 7, 8, null, null)
        ));

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(0), 0);
            }
        };
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE_2, JoinRelType.ANTI, false, condition, null, context,
                true);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseAntiJoin_NotExists2() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6, 7, null),
            IntegerBlock.of(9, 7, 8, null, null)
        ));
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE_2, JoinRelType.ANTI, false, null, null, context, true);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testAntiJoin_NotIn() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6),
            IntegerBlock.of(9, 7, 8)
        ));
        List<IExpression> antiJoinOperands = Collections.singletonList(
            new InputRefExpression(1)
        );

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.ANTI, false, null, antiJoinOperands,
                context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SEMI_CASE,
                JoinRelType.ANTI, false, null, antiJoinOperands, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);
        }

    }

    @Test
    public void testSimpleReverseAntiJoin_NotIn() {
        List<Chunk> expects = Collections.singletonList(new Chunk(
            LongBlock.of(2L, 3L, 6L, 7L),
            LongBlock.of(9L, 7L, 8L, 10L)
        ));

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_LONG_CASE, JoinRelType.ANTI, false, null, null,
                context, true);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.SimpleReverseAntiProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

    @Test
    public void testReverseAntiIntJoinVec_NotIn() {
        enableVecJoin();

        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 6, 7),
            IntegerBlock.of(9, 7, 8, 10)
        ));

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.REVERSE_SEMI_INT_CASE, JoinRelType.ANTI, false, null, null,
                context, true);
        Assert.assertTrue(
            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseAntiIntegerProbeOperator);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
    }

//    @Test
//    public void testReverseAntiIntNotEqIntJoinVec_NotIn() {
//        enableVecJoin();
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            IntegerBlock.of(0, 9, 5, 12, 11),
//            LongBlock.of(-5L, -1L, -8L, -7L, -6L),
//            IntegerBlock.of(3, 7, 7, 8, 10)
//        ));
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(2), new InputRefExpression(6)), new NotEqual(), context);
//
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.ANTI_INT_NOT_EQ_INT_CASE, JoinRelType.ANTI, false, condition,
//                null,
//                context, true);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseAntiIntNotEqIntegerProbeOperator);
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

//    @Test
//    public void testReverseAntiLongNotEqIntJoinVec_NotIn() {
//        enableVecJoin();
//
//        List<Chunk> expects = Collections.singletonList(new Chunk(
//            LongBlock.of(0L, 3L, 5L, 6L),
//            LongBlock.of(-2L, -4L, -2L, -3L),
//            IntegerBlock.of(3, 5, 7, 8),
//            LongBlock.of(-5L, -7L, -7L, -8L)
//        ));
//
//        IExpression condition = ScalarFunctionExpression.getScalarFunctionExp(
//            ImmutableList.of(new InputRefExpression(2), new InputRefExpression(7)), new NotEqual(), context);
//
//        SingleExecTest test =
//            mockParallelHashJoinExec(EquiJoinMockData.ANTI_LONG_NOT_EQ_INT_CASE, JoinRelType.ANTI, false, condition,
//                null,
//                context, true);
//        Assert.assertTrue(
//            ((ParallelHashJoinExec) test.exec).probeOperator instanceof AbstractHashJoinExec.ReverseAntiLongNotEqIntegerProbeOperator);
//        test.exec();
//        assertExecResultByRow(test.result(), expects, false);
//    }

    @Test
    public void testAntiJoin_NotIn_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, null, 8, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Collections.singletonList(
            new InputRefExpression(1)
        );

        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(4, 5, 6, 7),
            IntegerBlock.of(5, null, 8, null)
        ));
        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context, false);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                outerInput.getChunks(), outerInput.getDataTypes(),
                innerInput.getChunks(), innerInput.getDataTypes(),
                JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);
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

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Collections.singletonList(
            new InputRefExpression(1)
        );

        Executor exec =
            mockParallelHashJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context, false);
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
        List<Chunk> expects = Collections.singletonList(new Chunk(
            IntegerBlock.of(2, 3, 4, 6, 7),
            IntegerBlock.of(9, 7, 5, 8, null)
        ));
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), 5);
            }
        };
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SEMI_CASE, JoinRelType.ANTI, false, condition, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SEMI_CASE,
                JoinRelType.ANTI, false, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);
        }

    }

    @Test
    public void testInnerSingleJoin() {
        List<DataType> outTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, "f", "d", "c", null, "a"));

        List<Chunk> expects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            build();
        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            row(1000, null, "XX").
            build();

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.INNER, true, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
        test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.INNER, true, null, null, context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.INNER, true, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.INNER, true, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }

    }

    @Test
    public void testInnerSingleJoin_withError() {
        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.INNER, true, null, null, context);
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
        }
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.INNER, false, null, null, context,
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
        List<DataType> outTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);
        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", "d", "c", null, "a"));
        List<Chunk> expects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            row(1000, null, null).
            build();

        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            row(1000, null, "XX").
            build();

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.LEFT, true, null, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
        test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.LEFT, true, null, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.LEFT, true, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.LEFT, true, null, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }

    }

    @Test
    public void testLeftSingleJoin_WithCondition() {
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), "d");
            }
        };
        List<DataType> outTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);

        Chunk baseExpect = new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
            StringBlock.of("c", null, null, "f", null, "c", null, "a"));

        List<Chunk> expects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            row(1000, null, null).
            build();

        List<Chunk> nullSafeExpects = rowChunksBuilder(outTypes).
            addChunk(baseExpect).
            row(1000, null, "XX").
            build();

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.LEFT, true, condition, null,
                context);
        test.exec();
        assertExecResultByRow(test.result(), expects, false);

        EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
        test = mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.LEFT, true, condition, null,
            context);
        test.exec();
        assertExecResultByRow(test.result(), nullSafeExpects, false);

        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.LEFT, true, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), expects, false);

            EquiJoinMockData.SINGLE_JOIN_CASE.setKeyIsNullSafe(0);
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.LEFT, true, condition, null, context,
                bucketNum);
            test.exec();
            assertExecResultByRow(test.result(), nullSafeExpects, false);

        }

    }

    @Test
    public void testLeftSingleJoin_withError() {

        SingleExecTest test =
            mockParallelHashJoinExec(EquiJoinMockData.SINGLE_JOIN_CASE, JoinRelType.LEFT, true, null, null,
                context);
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
        }
        for (int bucketNum = 1; bucketNum <= 4; bucketNum++) {
            test = mockHybridHashJoinExec(
                EquiJoinMockData.SINGLE_JOIN_CASE,
                JoinRelType.LEFT, true, null, null, context,
                bucketNum);
            try {
                test.exec();
            } catch (TddlRuntimeException e) {
                assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
            }
        }
    }

    /**
     * TODO move to columnar test package
     */
    private void enableVecJoin() {
        this.context.getParamManager().getProps().put(ENABLE_VEC_JOIN, "true");
        this.context.getParamManager().getProps().put(ENABLE_VEC_BUILD_JOIN_ROW, "true");
    }

    @After
    public void after() {
        this.context.getParamManager().getProps().put(ENABLE_VEC_JOIN, "false");
        this.context.getParamManager().getProps().put(ENABLE_VEC_BUILD_JOIN_ROW, "false");
    }
}
