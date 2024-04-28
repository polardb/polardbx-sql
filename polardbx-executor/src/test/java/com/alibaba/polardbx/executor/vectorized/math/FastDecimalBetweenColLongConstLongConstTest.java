package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FastDecimalBetweenColLongConstLongConstTest extends BaseExecTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private final static Random RANDOM = new Random();

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);
        connectionMap.put(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testDecimal64() {
        final DecimalType decimalType = new DecimalType(15, 2);
        final int positionCount = context.getExecutorChunkLimit();
        final int nullCount = 20;
        final int lowerBound = 0; // 0.00
        final int upperBound = 1000; // 10.00
        final long betweenLeft = 3; // 3.00
        final long betweenRight = 6; // 6.00

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

        VectorizedExpression expression = root.accept(converter);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(positionCount)
            .addEmptySlots(inputTypes)
            .addEmptySlots(converter.getOutputDataTypes())
            .build();

        // build input decimal block
        DecimalBlock inputBlock =
            generateDecimal64Block(decimalType, positionCount, nullCount, lowerBound, upperBound);
        Chunk inputChunk = new Chunk(positionCount, inputBlock);

        LongBlock outputBlock = (LongBlock) BlockUtils.createBlock(DataTypes.LongType, inputChunk.getPositionCount());

        preAllocatedChunk.setSelection(null);
        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.setSlotAt(inputBlock, 0);
        preAllocatedChunk.setSlotAt(outputBlock, expression.getOutputIndex());
        preAllocatedChunk.setBatchSize(positionCount);

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        // check actual boolean values with expected values from decimal compare.
        Decimal lower = Decimal.fromLong(betweenLeft);
        Decimal upper = Decimal.fromLong(betweenRight);
        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            boolean actual = outputBlock.getLong(i) == 1 && !outputBlock.isNull(i);
            boolean expected = (!inputBlock.isNull(i)
                && inputBlock.getDecimal(i).compareTo(lower) >= 0
                && inputBlock.getDecimal(i).compareTo(upper) <= 0);
            Assert.assertTrue(actual == expected);
        }

    }

    @Test
    public void testDecimal() {
        final DecimalType decimalType = new DecimalType(15, 2);
        final int positionCount = context.getExecutorChunkLimit();
        final int nullCount = 20;
        final int lowerBound = 0; // 0.00
        final int upperBound = 1000; // 10.00
        final long betweenLeft = 3; // 3.00
        final long betweenRight = 6; // 6.00

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

        VectorizedExpression expression = root.accept(converter);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(positionCount)
            .addEmptySlots(inputTypes)
            .addEmptySlots(converter.getOutputDataTypes())
            .build();

        // build input decimal block
        DecimalBlock inputBlock =
            generateDecimalBlock(decimalType, positionCount, nullCount, lowerBound, upperBound);
        Chunk inputChunk = new Chunk(positionCount, inputBlock);

        LongBlock outputBlock = (LongBlock) BlockUtils.createBlock(DataTypes.LongType, inputChunk.getPositionCount());

        preAllocatedChunk.setSelection(null);
        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.setSlotAt(inputBlock, 0);
        preAllocatedChunk.setSlotAt(outputBlock, expression.getOutputIndex());
        preAllocatedChunk.setBatchSize(positionCount);

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        // check actual boolean values with expected values from decimal compare.
        Decimal lower = Decimal.fromLong(betweenLeft);
        Decimal upper = Decimal.fromLong(betweenRight);
        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            boolean actual = outputBlock.getLong(i) == 1 && !outputBlock.isNull(i);
            boolean expected = (!inputBlock.isNull(i)
                && inputBlock.getDecimal(i).compareTo(lower) >= 0
                && inputBlock.getDecimal(i).compareTo(upper) <= 0);
            Assert.assertTrue(actual == expected);
        }

    }

    private DecimalBlock generateDecimal64Block(DecimalType decimalType, int positionCount, int nullCount,
                                                int lowerBound, int upperBound) {
        DecimalBlock decimalBlock = (DecimalBlock) BlockUtils.createBlock(decimalType, positionCount);
        long[] decimal64Values = decimalBlock.allocateDecimal64();
        boolean[] nulls = decimalBlock.nulls();

        for (int i = 0; i < positionCount; i++) {
            int decimalValue = lowerBound + RANDOM.nextInt(upperBound - lowerBound);
            decimal64Values[i] = decimalValue;
        }

        for (int i = 0; i < positionCount; i++) {
            if (RANDOM.nextInt(positionCount) < nullCount) {
                nulls[i] = true;
            }
        }

        return decimalBlock;
    }

    private DecimalBlock generateDecimalBlock(DecimalType decimalType, int positionCount, int nullCount,
                                              int lowerBound, int upperBound) {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(positionCount, decimalType);
        final int scale = decimalType.getScale();
        for (int i = 0; i < positionCount; i++) {
            if (RANDOM.nextInt(positionCount) < nullCount) {
                blockBuilder.appendNull();
            } else {
                int decimalValue = lowerBound + RANDOM.nextInt(upperBound - lowerBound);
                blockBuilder.writeDecimal(new Decimal(decimalValue, scale));
            }
        }
        return (DecimalBlock) blockBuilder.build();
    }

}
