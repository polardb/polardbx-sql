package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EQIntegerColCharConstVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void test() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithSelection() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 1L, null, 0L, 0L, null, 1L, 0L);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testNull() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTestNull(inputChunk, expectBlock, null);
    }

    @Test
    public void testNullWithSelection() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTestNull(inputChunk, expectBlock, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        // rex node is "=(int ref, char const)"
        RexNode rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0),
                REX_BUILDER.makeLiteral("100", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true)
            ));

        List<DataType<?>> inputTypes = ImmutableList.of(DataTypes.IntegerType);

        RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);

        Assert.assertTrue(condition instanceof EQIntegerColCharConstVectorizedExpression);

        // Data types of intermediate results or final results.
        List<DataType<?>> filterOutputTypes = converter.getOutputDataTypes();

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(inputTypes)
            .addEmptySlots(filterOutputTypes)
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .addLiteralBitmap(converter.getLiteralBitmap())
            .build();

        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputTypes.size(), false);

        // Prepare selection array for evaluation.
        if (sel != null) {
            preAllocatedChunk.setBatchSize(sel.length);
            preAllocatedChunk.setSelection(sel);
            preAllocatedChunk.setSelectionInUse(true);
        } else {
            preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
            preAllocatedChunk.setSelection(null);
            preAllocatedChunk.setSelectionInUse(false);
        }

        for (int i = 0; i < inputTypes.size(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }

    private void doTestNull(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        EQIntegerColCharConstVectorizedExpression condition = new EQIntegerColCharConstVectorizedExpression(
            2,
            new VectorizedExpression[] {
                new InputRefVectorizedExpression(DataTypes.IntegerType, 0, 0),
                new LiteralVectorizedExpression(DataTypes.VarcharType, null, 1)
            });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.IntegerType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();

        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);

        // Prepare selection array for evaluation.
        if (sel != null) {
            preAllocatedChunk.setBatchSize(sel.length);
            preAllocatedChunk.setSelection(sel);
            preAllocatedChunk.setSelectionInUse(true);
        } else {
            preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
            preAllocatedChunk.setSelection(null);
            preAllocatedChunk.setSelectionInUse(false);
        }

        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }
}
