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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.NullBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractExpression;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;

/**
 * TODO: test anti-condition
 * FIXME: testAntiJoin_NotIn not pass
 */
public class NestedLoopJoinExecTest extends BaseExecTest {

    static NestedLoopJoinExec mockNestedLoopJoinExec(Executor outerInput,
                                                     Executor innerInput,
                                                     JoinRelType joinType,
                                                     boolean maxOneRow,
                                                     IExpression otherCondition,
                                                     List<IExpression> antiJoinOperands,
                                                     ExecutionContext context) {

        return new NestedLoopJoinExec(
            outerInput, innerInput, joinType, maxOneRow,
            otherCondition, antiJoinOperands, null, context, new NestedLoopJoinExec.Synchronizer());
    }

    @Test
    public void testInnerJoin_CrossProduct() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2),
                IntegerBlock.of(3, 4, 9)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.StringType)
            .withChunk(new Chunk(
                IntegerBlock.of(1, 2, 3),
                StringBlock.of("a", "b", null)))
            .build();

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, false, null, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 0, 0, 1, 1, 1, 2, 2, 2),
                IntegerBlock.of(3, 3, 3, 4, 4, 4, 9, 9, 9),
                IntegerBlock.of(1, 2, 3, 1, 2, 3, 1, 2, 3),
                StringBlock.of("a", "b", null, "a", "b", null, "a", "b", null)
            )), true);
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

        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, "f", "d", "c", null, "a")
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && row.getObject(2) != null
                    && Objects.equals(row.getObject(1), row.getObject(3))
                    && Objects.equals(row.getObject(2), row.getObject(4));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 0, 2, 3, 5),
                IntegerBlock.of(1, 1, 2, 2, 3),
                StringBlock.of("a", "a", "a", "b", "a"),
                IntegerBlock.of(1, 1, 2, 2, 3),
                StringBlock.of("a", "a", "a", "b", "a"),
                StringBlock.of("A", "E", "B", "F", "C")
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a")
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && row.getObject(2) != null
                    && Objects.equals(row.getObject(1), row.getObject(3))
                    && Objects.equals(row.getObject(2), row.getObject(4));

            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.LEFT, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
                StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null),
                IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
                StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
                StringBlock.of("A", "E", null, "B", "F", null, "C", null, null, null, null)
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(3) != null && Objects.equals(row.getObject(3), row.getObject(0));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(3, 4, null, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a"),
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1)
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(4) != null && row.getObject(5) != null
                    && Objects.equals(row.getObject(4), row.getObject(0))
                    && Objects.equals(row.getObject(5), row.getObject(1));

            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.RIGHT, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(1, 1, null, 2, 2, null, 3, null, null, null, null),
                StringBlock.of("a", "a", null, "a", "b", null, "a", null, null, null, null),
                StringBlock.of("A", "E", null, "B", "F", null, "C", null, null, null, null),
                IntegerBlock.of(0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                IntegerBlock.of(1, 1, 1, 2, 2, null, 3, 3, 4, 4, 4),
                StringBlock.of("a", "a", "b", "a", "b", "a", "a", "b", "a", "b", null)
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));

            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 4, 5, 7),
                IntegerBlock.of(3, 4, 5, 3, 1)
            )), true);
    }

    @Test
    public void testSemiJoin_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.SEMI, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.EMPTY_LIST, false);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(2, 3, 6, 7),
                IntegerBlock.of(9, 7, 8, null)
            )), true);
    }

    @Ignore
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, condition, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(2, 3, 6),
                IntegerBlock.of(9, 7, 8)
            )), false);
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
                IntegerBlock.of(3, 4, 5, null)))
            .build();
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, condition, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultByRow(test.result(), Collections.EMPTY_LIST, false);
    }

    @Test
    public void testAntiJoin_NotIn_InnerEmpty() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType).build();
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(2));
            }
        };

        List<IExpression> antiJoinOperands = Arrays.asList(
            new InputRefExpression(1)
        );

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.ANTI, false, condition, antiJoinOperands,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, null, 9, null)
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(3));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, true, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, "f", "d", "c", null, "a")
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(3));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, true, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getMessage().contains("more than 1 row");
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(3));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();

        assertExecResultByRow(test.result(),
            Collections.singletonList(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4, 5, 6, 7),
                IntegerBlock.of(3, 4, 9, 7, 5, 3, 8, 1),
                StringBlock.of("c", null, null, "f", "d", "c", null, "a")
            )), true);
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
        IExpression condition = new AbstractExpression() {
            @Override
            public Object eval(Row row) {
                return row.getObject(1) != null && Objects.equals(row.getObject(1), row.getObject(3));
            }
        };

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.LEFT, true, condition, null, context);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        try {
            test.exec();
        } catch (TddlRuntimeException e) {
            assert e.getMessage().contains("more than 1 row");
            assert e.getErrorCodeType() == ErrorCode.ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW;
        }
    }

    @Test
    public void testInnerJoin_LargeResultSet() {
        MockExec outerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(new NullBlock(97)))
            .withChunk(new Chunk(new NullBlock(83)))
            .build();

        MockExec innerInput = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(new NullBlock(83)))
            .withChunk(new Chunk(new NullBlock(97)))
            .build();

        NestedLoopJoinExec exec =
            mockNestedLoopJoinExec(outerInput, innerInput, JoinRelType.INNER, false, null, null, context);

        final int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        int expectedRows = 180 * 180;
        int expectedChunks = -Math.floorDiv(-expectedRows, chunkLimit);
        SingleExecTest test = new SingleExecTest.Builder(exec, innerInput).build();
        test.exec();
        assertExecResultChunksSize(test.result(), expectedChunks, expectedRows);
    }
}
