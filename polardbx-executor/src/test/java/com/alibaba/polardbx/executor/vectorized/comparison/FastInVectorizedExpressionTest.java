package com.alibaba.polardbx.executor.vectorized.comparison;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InValuesVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastInVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FastInVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void testIntInLong() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testIntInInt() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        int[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testLongInLong() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testIntInString() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testLongInString() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testStringInLong() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);

        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType,
            10, new ExecutionContext(), false);
        for (int i = 0; i < longBlock.getPositionCount(); i++) {
            if (longBlock.isNull(i)) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(Long.toString(longBlock.getLong(i)));
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();

        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testStringInString() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);

        SliceBlockBuilder sliceBlockBuilder = new SliceBlockBuilder(DataTypes.VarcharType,
            10, new ExecutionContext(), false);
        for (int i = 0; i < longBlock.getPositionCount(); i++) {
            if (longBlock.isNull(i)) {
                sliceBlockBuilder.appendNull();
            } else {
                sliceBlockBuilder.writeString(Long.toString(longBlock.getLong(i)));
            }
        }
        SliceBlock sliceBlock = (SliceBlock) sliceBlockBuilder.build();

        Chunk inputChunk = new Chunk(sliceBlock.getPositionCount(), sliceBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 0L, 0L, 1L, 0L, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testLongInLongWithNull() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        Long[] inValues = {1L, 100L, null};
        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    private List<RexNode> convertInValues(long[] inValues) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        for (long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(Long[] inValues) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        for (Long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(int[] inValues) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        for (long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(String[] inValues) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        for (String inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private void doTest(DataType inputType, Chunk inputChunk,
                        List<RexNode> rexLiteralList, RandomAccessBlock expectBlock) {
        ExecutionContext context = new ExecutionContext();

        int outputIndex = 2;
        VectorizedExpression inputRef = new InputRefVectorizedExpression(inputType, 0, 0);
        VectorizedExpression inValueExpr = InValuesVectorizedExpression.from(rexLiteralList, 2);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(inputChunk.getPositionCount())
            .addSlot((RandomAccessBlock) inputChunk.getBlock(0))
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.LongType,
                DataTypes.LongType
            ))
            .build();
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(outputIndex);

        VectorizedExpression inExpr = new FastInVectorizedExpression(
            DataTypes.LongType,
            outputIndex,
            new VectorizedExpression[] {
                inputRef,
                inValueExpr
            }
        );
        inExpr.eval(evaluationContext);

        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            Assert.assertEquals(resultBlock.elementAt(i), expectBlock.elementAt(i));
        }
    }
}
