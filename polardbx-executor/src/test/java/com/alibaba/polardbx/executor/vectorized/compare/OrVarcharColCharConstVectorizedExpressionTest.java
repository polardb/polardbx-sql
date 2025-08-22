package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.executor.vectorized.logical.OrCharConstVarcharColVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class OrVarcharColCharConstVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private final int count = 10;
    private ExecutionContext context;

    @Before
    public void before() {
        context = new ExecutionContext();
    }

    @Test
    public void testSlice() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, false);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        doTest(inputChunk, expectBlock, "0", null);
    }

    @Test
    public void testSliceWithSelection() {

        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, false);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, "0", sel);
    }

    @Test
    public void testReference() {
        ObjectBlockBuilder blockBuilder = new ObjectBlockBuilder(count);
        for (int i = 0; i < count - 1; i++) {
            Slice slice = Slices.utf8Slice(String.valueOf(i * 100));
            blockBuilder.writeObject(slice);
        }
        blockBuilder.appendNull();
        ReferenceBlock refBlock = (ReferenceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, refBlock);

        LongBlock expectBlock = LongBlock.of(0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        doTest(inputChunk, expectBlock, "0", null);
    }

    @Test
    public void testReferenceWithSelection() {
        ObjectBlockBuilder blockBuilder = new ObjectBlockBuilder(count);
        for (int i = 0; i < count - 1; i++) {
            Slice slice = Slices.utf8Slice(String.valueOf(i * 100));
            blockBuilder.writeObject(slice);
        }
        blockBuilder.appendNull();
        ReferenceBlock refBlock = (ReferenceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, refBlock);

        LongBlock expectBlock = LongBlock.of(0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, "0", sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, String value, int[] sel) {
        // rex node is "=(varchar ref, char const)"
        RexNode rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.OR,
            ImmutableList.of(
                REX_BUILDER.makeLiteral(value, TYPE_FACTORY.createSqlType(SqlTypeName.CHAR), false),
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), 0)
            ));

        List<DataType<?>> inputTypes = ImmutableList.of(DataTypes.VarcharType);

        RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);

        Assert.assertTrue(condition instanceof OrCharConstVarcharColVectorizedExpression);

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

}
