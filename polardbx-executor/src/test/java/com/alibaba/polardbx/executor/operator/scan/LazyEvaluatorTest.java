package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultLazyEvaluator;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

@RunWith(value = Parameterized.class)
public class LazyEvaluatorTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    public static final int STARTING_POSITION = 10000;
    public static final int POSITION_COUNT = 900;
    public static final long OPERAND = 1000L;

    public final TestExpression testExpression;

    public LazyEvaluatorTest(TestExpression testExpression) {
        this.testExpression = testExpression;
    }

    @Parameterized.Parameters(name = "testExpression={0}")
    public static List<Object[]> prepare() {

        Object[][] object = {
            {new NormalTestExpression()},
            {new FalseTestExpression()},
            {new TrueTestExpression()}};
        return Arrays.asList(object);
    }

    @Test
    public void testPostIntersection() {
        // Test strategy of POST_INTERSECTION
        doTest(testExpression.buildCondition(),
            RoaringBitmap.bitmapOf(
                STARTING_POSITION + 100, STARTING_POSITION + 200, STARTING_POSITION + 300,
                STARTING_POSITION + 400, STARTING_POSITION + 500, STARTING_POSITION + 600,
                STARTING_POSITION + 700, STARTING_POSITION + 800, STARTING_POSITION + 900
            ), (result) -> {
                for (int i = 1; i < POSITION_COUNT; i++) {
                    if (i % 100 != 0 && testExpression.predicate(i)) {
                        Assert.assertTrue("i = " + i, result.get(i));
                    } else {
                        Assert.assertTrue("i = " + i, !result.get(i));
                    }
                }
            });
    }

    @Test
    public void testPartialSelection() {
        // Test strategy of PARTIAL_SELECTION
        int[] bitmap = new int[700];
        for (int i = 0; i < bitmap.length; i++) {
            bitmap[i] = STARTING_POSITION + i + 20;
        }

        doTest(testExpression.buildCondition(),
            RoaringBitmap.bitmapOf(bitmap),
            (result) -> {
                for (int i = 0; i < POSITION_COUNT; i++) {
                    if (!(i >= 20 && i < 720) && testExpression.predicate(i)) {
                        Assert.assertTrue("i = " + i, result.get(i));
                    } else {
                        Assert.assertTrue("i = " + i, !result.get(i));
                    }
                }
            });
    }

    @Test
    public void testNoSelection() {
        // Test strategy of NO_SELECTION
        int[] bitmap = new int[POSITION_COUNT];
        for (int i = 0; i < bitmap.length; i++) {
            // mark all positions as deleted.
            bitmap[i] = STARTING_POSITION + i;
        }

        doTest(testExpression.buildCondition(),
            RoaringBitmap.bitmapOf(bitmap),
            (result) -> {
                for (int i = 0; i < POSITION_COUNT; i++) {
                    Assert.assertTrue("i = " + i, !result.get(i));
                }
            });
    }

    @Test
    public void testFullSelection() {
        // Test strategy of FULL_SELECTION
        int[] bitmap = new int[POSITION_COUNT];
        for (int i = 0; i < bitmap.length; i++) {
            // All deleted positions are not contained in range of test block.
            bitmap[i] = STARTING_POSITION - i - 1;
        }

        doTest(testExpression.buildCondition(),
            RoaringBitmap.bitmapOf(bitmap),
            (result) -> {
                for (int i = 0; i < POSITION_COUNT; i++) {
                    if (testExpression.predicate(i)) {
                        Assert.assertTrue("i = " + i, result.get(i));
                    } else {
                        Assert.assertTrue("i = " + i, !result.get(i));
                    }
                }
            });
    }

    public void doTest(RexNode predicate, RoaringBitmap deletion, Consumer<BitSet> checker) {
        LazyEvaluator<Chunk, BitSet> evaluator =
            DefaultLazyEvaluator.builder()
                .setContext(new ExecutionContext())
                .setRexNode(predicate)
                .setRatio(.3D)
                .setInputTypes(ImmutableList.of(DataTypes.LongType)) // input type: bigint.
                .build();

        VectorizedExpression expression = ((DefaultLazyEvaluator) evaluator).getCondition();

        // GELongColLongConstVectorizedExpression, { LongType, 4 }
        //   └ AddLongColLongConstVectorizedExpression, { LongType, 2 }
        //      └ InputRefVectorizedExpression, { LongType, 0 }
        //      └ LiteralVectorizedExpression, { LongType, 1 }
        //   └ LiteralVectorizedExpression, { LongType, 3 }
        String digest = VectorizedExpressionUtils.digest(expression);
        System.out.println(digest);

        final int positionCount = POSITION_COUNT;
        Block longBlock = createBlock(positionCount);
        Block[] blocks = new Block[] {longBlock};
        Chunk dataChunk = new Chunk(blocks);

        // deleted positions: offset + {100, 200, 300, ... 900}
        final int startingPosition = STARTING_POSITION;

        BitSet result = evaluator.eval(dataChunk, startingPosition, positionCount, deletion);

        // Check result.
        checker.accept(result);
    }

    // The block stores the long values: {500, 501, 502, ... positionCount + 500}
    private Block createBlock(int positionCount) {
        BlockBuilder blockBuilder = BlockBuilders.create(DataTypes.LongType, new ExecutionContext(), positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.writeLong(i + 500);
        }
        return blockBuilder.build();
    }

    interface TestExpression {
        RexNode buildCondition();

        boolean predicate(int position);
    }

    static class FalseTestExpression implements TestExpression {

        @Override
        public RexNode buildCondition() {
            return REX_BUILDER.makeBigIntLiteral(0L);
        }

        @Override
        public boolean predicate(int position) {
            return false;
        }
    }

    static class TrueTestExpression implements TestExpression {

        @Override
        public RexNode buildCondition() {
            return REX_BUILDER.makeBigIntLiteral(1L);
        }

        @Override
        public boolean predicate(int position) {
            return true;
        }
    }

    static class NormalTestExpression implements TestExpression {
        public RexNode buildCondition() {
            // column a and literal 1
            RexInputRef inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);
            RexLiteral literal = REX_BUILDER.makeLiteral(1L, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT);

            // call: a+1
            RexNode plus = REX_BUILDER.makeCall(
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                TddlOperatorTable.PLUS,
                ImmutableList.of(
                    // column
                    inputRef,
                    // const
                    literal
                )
            );

            // call: (a+1) >= 1000
            RexNode predicate = REX_BUILDER.makeCall(
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                TddlOperatorTable.GREATER_THAN_OR_EQUAL,
                ImmutableList.of(
                    // column
                    plus,
                    // const
                    REX_BUILDER.makeLiteral(OPERAND, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT)
                )
            );
            return predicate;
        }

        public boolean predicate(int position) {
            return (position + 500) + 1 >= 1000;
        }
    }
}
