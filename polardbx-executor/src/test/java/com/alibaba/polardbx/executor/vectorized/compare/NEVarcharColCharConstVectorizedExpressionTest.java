package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class NEVarcharColCharConstVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private final int count = 10;
    private final boolean enableCompatible;
    private ExecutionContext context;

    public NEVarcharColCharConstVectorizedExpressionTest(boolean compatible) {
        this.enableCompatible = compatible;
    }

    @Before
    public void before() {
        context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_OSS_COMPATIBLE", String.valueOf(enableCompatible));
    }

    @Parameterized.Parameters(name = "compatible={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {false});
        list.add(new Object[] {true});
        return list;
    }

    @Test
    public void testSlice() {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, enableCompatible);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        doTest(inputChunk, expectBlock, "100", null);
    }

    @Test
    public void testSliceWithSelection() {

        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, enableCompatible);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, "100", sel);
    }

    @Test
    public void testNull() {

        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, enableCompatible);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        doTestNull(inputChunk, expectBlock, null);
    }

    @Test
    public void testWithSelectionNull() {

        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(DataTypes.VarcharType, count, context, enableCompatible);
        for (int i = 0; i < count - 1; i++) {
            blockBuilder.writeString(String.valueOf(i * 100));
        }
        blockBuilder.appendNull();
        SliceBlock sliceBlock = (SliceBlock) blockBuilder.build();
        Chunk inputChunk = new Chunk(count, sliceBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTestNull(inputChunk, expectBlock, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, String value, int[] sel) {
        // rex node is "=(varchar ref, char const)"
        RexNode rexNode = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.NOT_EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), 0),
                REX_BUILDER.makeLiteral(value, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), false)
            ));

        List<DataType<?>> inputTypes = ImmutableList.of(DataTypes.VarcharType);

        RexNode root = VectorizedExpressionBuilder.rewriteRoot(rexNode, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);

        Assert.assertTrue(condition instanceof NEVarcharColCharConstVectorizedExpression);

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

    private void doTestNull(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        NEVarcharColCharConstVectorizedExpression condition = new NEVarcharColCharConstVectorizedExpression(
            2,
            new VectorizedExpression[] {
                new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0),
                new LiteralVectorizedExpression(DataTypes.VarcharType, null, 1)
            });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.VarcharType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();

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
