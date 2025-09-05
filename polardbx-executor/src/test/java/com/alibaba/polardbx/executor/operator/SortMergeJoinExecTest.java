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

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SortMergeJoinExecTest extends BaseExecTest {
    static EquiJoinKey mockEquiJoinKey(int outerIndex, int innerIndex, DataType unifiedType) {
        return new EquiJoinKey(outerIndex, innerIndex, unifiedType, false);
    }

    static SortMergeJoinExec mockSortMergeJoinExec(Executor outerInput,
                                                   Executor innerInput,
                                                   JoinRelType joinType,
                                                   boolean maxOneRow,
                                                   List<EquiJoinKey> joinKeys,
                                                   IExpression otherCondition,
                                                   List<IExpression> antiJoinOperands,
                                                   ExecutionContext context) {
        List<Boolean> keyColumnIsAscending = new ArrayList<>(Collections.nCopies(joinKeys.size(), true));
        return new SortMergeJoinExec(
            outerInput, innerInput, joinType, maxOneRow,
            joinKeys, keyColumnIsAscending, otherCondition, antiJoinOperands, context);
    }

    @Test
    public void testInnerJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(1, 1, 2, 2, 3, 3, 5, 6, 7, 8),
            IntegerBlock.of(1, 1, 1, 1, 2, 2, 4, 5, 6, 7),
            IntegerBlock.of(1, 1, 1, 1, 2, 2, 4, 5, 6, 7),
            StringBlock.of("a", "b", "a", "b", "c", null, "d", "e", "f", null)
        ));
    }

    @Test
    public void testInnerJoin_EmptyInnerSide() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(1, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec);
    }

    @Test
    public void testInnerJoin_EmptyOuterSide() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType).build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec);
    }

    @Test
    public void testInnerJoin_EmptyBothSides() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType).build();
        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec);
    }

    @Test
    public void testInnerJoin_MultiKey() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(-1, 0, 1, 2, 3),
                IntegerBlock.of(0, 1, 1, 2, 2),
                StringBlock.of(null, "a", "b", "a", "b")))
            .withChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                IntegerBlock.of(3, 3, 4, 4),
                StringBlock.of("a", "b", "a", "b")))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 1, 1, 2),
                StringBlock.of("b", "a", "a", "a"),
                StringBlock.of("H", "A", "E", "B")))
            .withChunk(new Chunk(
                IntegerBlock.of(2, 3, 3, 4),
                StringBlock.of("b", "a", "c", null),
                StringBlock.of("F", "C", "G", "D")))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType),
            mockEquiJoinKey(2, 1, DataTypes.StringType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 0, 2, 3, 5),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            IntegerBlock.of(1, 1, 2, 2, 3),
            StringBlock.of("a", "a", "a", "b", "a"),
            StringBlock.of("A", "E", "B", "F", "C")
        ));
    }

    @Test
    public void testLeftOuterJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 1, 1, 2, 2),
                StringBlock.of("!", "a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, null, 4, 5, 6, 7),
            StringBlock.of(null, "a", "b", "a", "b", "c", null, null, "d", "e", "f", null)
        ));
    }

    @Test
    public void testLeftOuterJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 1, 1, 2, 2),
                StringBlock.of("!", "a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, condition, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, null, null, 5, 6, 7),
            StringBlock.of(null, "a", "b", "a", "b", "c", null, null, null, "e", "f", null)
        ));
    }

    @Test
    public void testLeftOuterJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 2, 3, 4, 5, 6, 7),
            IntegerBlock.of(null, null, null, null, null, null, null, null, null, null),
            StringBlock.of(null, null, null, null, null, null, null, null, null, null)
        ));
    }

    @Test
    public void testRightOuterJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 1, 2, 2),
                StringBlock.of("a", "b", "c", null)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                StringBlock.of("d", "e", "f", null)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, null, 4, 5, 6, 7),
            StringBlock.of(null, "a", "b", "a", "b", "c", null, null, "d", "e", "f", null),
            IntegerBlock.of(0, 1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 1, 1, 2, 2, 3, 4, 5, 6, 7)
        ));
    }

    @Test
    public void testRightOuterJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(null, null, null, null, null, null, null, null, null),
            StringBlock.of(null, null, null, null, null, null, null, null, null),
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 2, 3, 4, 5, 6, 7)
        ));
    }

    @Test
    public void testSemiJoin_Simple() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 1, 1, 2, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(1, 2, 3, 5, 6, 7, 8),
            IntegerBlock.of(1, 1, 2, 4, 5, 6, 7)
        ));
    }

    @Test
    public void testSemiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 7)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, joinKeys, null, null, context);

        assertExecResults(exec);
    }

    @Test
    public void testAntiJoin_NotExists() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 0, 5, 1),
                IntegerBlock.of(null, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(7, 3, 6, 2),
            IntegerBlock.of(null, 7, 8, 9)
        ));
    }

    @Test
    public void testAntiJoin_NotIn() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 0, 5, 1),
                IntegerBlock.of(null, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(3, 6, 2),
            IntegerBlock.of(7, 8, 9)
        ));
    }

    @Test
    public void testAntiJoin_NotIn_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 0, 5, 1),
                IntegerBlock.of(null, null, 3, 4)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(7, 0, 5, 1),
            IntegerBlock.of(null, null, 3, 4)
        ));
    }

    @Test
    public void testAntiJoin_NotIn_InnerContainsNull() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 0, 5, 1),
                IntegerBlock.of(null, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 2, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, antiJoinOperands,
                context);

        assertExecResults(exec);
    }

    @Test
    public void testAntiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 9)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
            IntegerBlock.of(null, 1, 1, 2, 3, 4, 5, 6, 9)
        ));
    }

    @Test
    public void testAntiJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(null, 1, 1, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, 8),
                IntegerBlock.of(3, 4, 5, 6, 9)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(null, 1, 1, 2, 2)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7)))
            .build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(1, 0, DataTypes.IntegerType));
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), 4);
            }
        };

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, joinKeys, condition, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 4, 5, 8),
            IntegerBlock.of(null, 3, 4, 9)
        ));
    }

    @Test
    public void testInnerSingleJoin() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 5, 0, 1),
                IntegerBlock.of(1, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, true, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(7, 5, 0, 1, 4, 3, 6),
            IntegerBlock.of(1, 3, 3, 4, 5, 7, 8),
            StringBlock.of("a", "c", "c", null, "d", "f", null)
        ));
    }

    @Test
    public void testInnerSingleJoin_withError() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 5, 0, 1),
                IntegerBlock.of(1, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, true, joinKeys, null, null, context);

        assertExecError(exec, "more than 1 row");
    }

    @Test
    public void testLeftSingleJoin() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 5, 0, 1),
                IntegerBlock.of(1, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(7, 5, 0, 1, 4, 3, 6, 2),
            IntegerBlock.of(1, 3, 3, 4, 5, 7, 8, 9),
            StringBlock.of("a", "c", "c", null, "d", "f", null, null)
        ));
    }

    @Test
    public void testLeftSingleJoin_WithCondition() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 5, 0, 1),
                IntegerBlock.of(1, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return !Objects.equals(row.getObject(2), "d");
            }
        };

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, condition, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(7, 5, 0, 1, 4, 3, 6, 2),
            IntegerBlock.of(1, 3, 3, 4, 5, 7, 8, 9),
            StringBlock.of("a", "c", "c", null, null, "f", null, null)
        ));
    }

    @Test
    public void testLeftSingleJoin_withError() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(7, 5, 0, 1),
                IntegerBlock.of(1, 3, 3, 4)))
            .withChunk(new Chunk(
                IntegerBlock.of(4, 3, 6, 2),
                IntegerBlock.of(5, 7, 8, 9)))
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

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, joinKeys, null, null, context);

        assertExecError(exec, "more than 1 row");
    }

    @Test
    public void testInnerJoinMultiRows() {
        MockExec.MockExecBuilder outerInputBuilder = MockExec.builder(DataTypes.IntegerType);
        outerInputBuilder.withChunk(new Chunk(IntegerBlock.of(0, 0, 0, 0)));
        MockExec outerInput = outerInputBuilder.build();

        MockExec.MockExecBuilder innerInputBuilder = MockExec.builder(DataTypes.IntegerType);

        innerInputBuilder.withChunk(new Chunk(IntegerBlock.of(0)));
        MockExec innerInput = innerInputBuilder.build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(0, 0, DataTypes.IntegerType));

        context.getExtraCmds().put(ConnectionProperties.CHUNK_SIZE, 2);

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.INNER, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 0),
            IntegerBlock.of(0, 0)
        ), new Chunk(
            IntegerBlock.of(0, 0),
            IntegerBlock.of(0, 0)
        ));
    }

    @Test
    public void testOuterJoinMultiRows() {
        MockExec.MockExecBuilder outerInputBuilder = MockExec.builder(DataTypes.IntegerType);
        outerInputBuilder.withChunk(new Chunk(IntegerBlock.of(0, 0, 0, 0)));
        MockExec outerInput = outerInputBuilder.build();

        MockExec.MockExecBuilder innerInputBuilder = MockExec.builder(DataTypes.IntegerType);

        innerInputBuilder.withChunk(new Chunk(IntegerBlock.of(1)));
        MockExec innerInput = innerInputBuilder.build();

        List<EquiJoinKey> joinKeys = Arrays.asList(
            mockEquiJoinKey(0, 0, DataTypes.IntegerType));

        context.getExtraCmds().put(ConnectionProperties.CHUNK_SIZE, 2);

        SortMergeJoinExec exec =
            mockSortMergeJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, joinKeys, null, null, context);

        assertExecResults(exec, new Chunk(
            IntegerBlock.of(0, 0),
            IntegerBlock.of(null, null)
        ), new Chunk(
            IntegerBlock.of(0, 0),
            IntegerBlock.of(null, null)
        ));
    }
}
