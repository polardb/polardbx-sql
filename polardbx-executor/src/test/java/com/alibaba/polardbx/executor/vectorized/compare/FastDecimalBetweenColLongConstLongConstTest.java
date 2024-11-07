package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Test for FastBetweenDecimalColLongConstLongConstVectorizedExpression
 */
@RunWith(Parameterized.class)
public class FastDecimalBetweenColLongConstLongConstTest extends BaseExecTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private final DecimalType decimalType = new DecimalType(15, 2);
    private final int COUNT = 1000;
    private final int[] sel;
    private final boolean withSelection;
    private final int nullProb = 10;
    private final Random random = new Random();
    private final Long betweenLeft;
    private final Long betweenRight;

    public FastDecimalBetweenColLongConstLongConstTest(boolean withSelection, Long left, Long right) {
        this.withSelection = withSelection;
        this.betweenLeft = left;
        this.betweenRight = right;
        if (withSelection) {
            final int offset = 10;
            this.sel = new int[COUNT / 2];
            for (int i = 0; i < sel.length; i++) {
                sel[i] = i + offset;
            }
        } else {
            this.sel = null;
        }
    }

    @Parameterized.Parameters(name = "sel={0},left={1},right={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {false, 0L, 999L});
        list.add(new Object[] {true, null, 999L});
        list.add(new Object[] {true, -999L, 0L});
        list.add(new Object[] {true, -100L, null});
        list.add(new Object[] {true, 0L, null});
        list.add(new Object[] {false, -9999L, 9999L});
        return list;
    }

    @Before
    public void before() {
        Map params = new HashMap();
        params.put(ConnectionParams.CHUNK_SIZE.getName(), COUNT);
        params.put(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION.getName(), true);
        context.setParamManager(new ParamManager(params));
    }

    @Test
    public void testDecimal64() {
        VectorizedExpression expression = buildExpression();

        MutableChunk preAllocatedChunk = buildDecimal64Chunk();
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(preAllocatedChunk.slotIn(0));
        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(preAllocatedChunk.slotIn(3));

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        // check actual boolean values with expected values from decimal compare.
        Decimal lower;
        if (betweenLeft == null) {
            lower = Decimal.ZERO;
        } else {
            lower = Decimal.fromLong(betweenLeft);
        }
        Decimal upper;
        if (betweenRight == null) {
            upper = Decimal.ZERO;
        } else {
            upper = Decimal.fromLong(betweenRight);
        }

        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                boolean actual = outputBlock.getLong(j) == 1 && !outputBlock.isNull(j);
                boolean expected = (!inputBlock.isNull(j)
                    && inputBlock.getDecimal(j).compareTo(lower) >= 0
                    && inputBlock.getDecimal(j).compareTo(upper) <= 0);
                Assert.assertEquals(actual, expected);
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                boolean actual = outputBlock.getLong(i) == 1 && !outputBlock.isNull(i);
                boolean expected = (!inputBlock.isNull(i)
                    && inputBlock.getDecimal(i).compareTo(lower) >= 0
                    && inputBlock.getDecimal(i).compareTo(upper) <= 0);
                Assert.assertEquals(actual, expected);
            }
        }
    }

    @Test
    public void testDecimal() {
        VectorizedExpression expression = buildExpression();

        MutableChunk preAllocatedChunk = buildDecimalChunk();
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(preAllocatedChunk.slotIn(0));
        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(preAllocatedChunk.slotIn(3));

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        // check actual boolean values with expected values from decimal compare.
        Decimal lower;
        if (betweenLeft == null) {
            lower = Decimal.ZERO;
        } else {
            lower = Decimal.fromLong(betweenLeft);
        }
        Decimal upper;
        if (betweenRight == null) {
            upper = Decimal.ZERO;
        } else {
            upper = Decimal.fromLong(betweenRight);
        }

        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                boolean actual = outputBlock.getLong(j) == 1 && !outputBlock.isNull(j);
                boolean expected = (!inputBlock.isNull(j)
                    && inputBlock.getDecimal(j).compareTo(lower) >= 0
                    && inputBlock.getDecimal(j).compareTo(upper) <= 0);
                Assert.assertEquals(actual, expected);
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                boolean actual = outputBlock.getLong(i) == 1 && !outputBlock.isNull(i);
                boolean expected = (!inputBlock.isNull(i)
                    && inputBlock.getDecimal(i).compareTo(lower) >= 0
                    && inputBlock.getDecimal(i).compareTo(upper) <= 0);
                Assert.assertEquals(actual, expected);
            }
        }
    }

    private MutableChunk buildDecimal64Chunk() {
        Block[] blocks = new Block[4];
        blocks[0] = new DecimalBlock(decimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);
        blocks[2] = new LongBlock(DataTypes.LongType, COUNT);
        blocks[3] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, decimalType);
        for (int i = 0; i < COUNT; i++) {
            if (random.nextInt(COUNT) <= nullProb) {
                inputBuilder.appendNull();
            } else {
                long left = genDecimal64NotOverflowLong();
                inputBuilder.writeLong(left);
            }

        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal64());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
            chunk.setBatchSize(sel.length);
        }
        return chunk;
    }

    private MutableChunk buildDecimalChunk() {
        Block[] blocks = new Block[4];
        blocks[0] = new DecimalBlock(decimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);
        blocks[2] = new LongBlock(DataTypes.LongType, COUNT);
        blocks[3] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, decimalType);
        for (int i = 0; i < COUNT; i++) {
            if (random.nextInt(COUNT) <= nullProb) {
                inputBuilder.appendNull();
            } else {
                long left = random.nextInt();
                inputBuilder.writeDecimal(new Decimal(left, decimalType.getScale()));
            }

        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertFalse(inputBlock.isDecimal64());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
            chunk.setBatchSize(sel.length);
        }
        return chunk;
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    private VectorizedExpression buildExpression() {
        List<DataType<?>> inputTypes = ImmutableList.of(decimalType);

        RexNode root = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.BETWEEN,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(
                    TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, decimalType.getPrecision(), decimalType.getScale()),
                    0),
                REX_BUILDER.makeBigIntLiteral(betweenLeft),
                REX_BUILDER.makeBigIntLiteral(betweenRight)
            ));

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        return root.accept(converter);
    }
}
