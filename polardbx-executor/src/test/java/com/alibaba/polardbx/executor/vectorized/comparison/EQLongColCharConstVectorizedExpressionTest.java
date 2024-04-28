package com.alibaba.polardbx.executor.vectorized.comparison;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.executor.vectorized.compare.EQLongColCharConstVectorizedExpression;
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

import java.util.List;
import java.util.Objects;

public class EQLongColCharConstVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void test() {

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);

        LongBlock expectBlock = LongBlock.of(0L, 0L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(inputChunk, expectBlock);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock) {
        ExecutionContext context = new ExecutionContext();

        // rex node is "=(int ref, char const)"
        RexNode rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
                REX_BUILDER.makeLiteral("100", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true)
            ));

        List<DataType<?>> inputTypes = ImmutableList.of(DataTypes.LongType);

        RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);

        Assert.assertTrue(condition instanceof EQLongColCharConstVectorizedExpression);

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
        preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
        preAllocatedChunk.setSelection(null);
        preAllocatedChunk.setSelectionInUse(false);

        for (int i = 0; i < inputTypes.size(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            Assert.assertTrue(Objects.equals(
                resultBlock.elementAt(i), expectBlock.elementAt(i)
            ));
        }
    }
}
