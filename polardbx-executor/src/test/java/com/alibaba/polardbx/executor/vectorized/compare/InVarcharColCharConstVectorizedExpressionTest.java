package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionRegistry;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentInfo;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionConstructor;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class InVarcharColCharConstVectorizedExpressionTest {

    private final int count = 10;
    protected boolean ossCompatible;
    private ExecutionContext context;

    public InVarcharColCharConstVectorizedExpressionTest(boolean ossCompatible) {
        this.ossCompatible = ossCompatible;
    }

    @Parameterized.Parameters(name = "ossCompatible={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {true});
        list.add(new Object[] {false});
        return list;
    }

    @Before
    public void before() {
        this.context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_OSS_COMPATIBLE", Boolean.toString(ossCompatible));
    }

    @Test
    public void testInRef() {
        VectorizedExpression expr = buildVecExpression();

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(16);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            objectBlockBuilder.writeObject(Slices.utf8Slice(values[i % values.length]));
        }
        objectBlockBuilder.appendNull();

        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);
        LongBlock expectBlock = LongBlock.of(1L, 1L, 0L, 1L, 1L, 0L, 1L, 1L, 0L, null);
        doTest(expr, inputChunk, null, expectBlock);
    }

    @Test
    public void testInRefWithSel() {
        VectorizedExpression expr = buildVecExpression();

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(16);
        String[] values = new String[] {"val1", "val2", "val3"};
        for (int i = 0; i < count - 1; i++) {
            objectBlockBuilder.writeObject(Slices.utf8Slice(values[i % values.length]));
        }
        objectBlockBuilder.appendNull();

        ReferenceBlock strBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(strBlock.getPositionCount(), strBlock);
        LongBlock expectBlock = LongBlock.of(1L, 1L, 0L, 1L, 1L, 0L, 1L, 1L, 0L, null);
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(expr, inputChunk, sel, expectBlock);
    }

    private void doTest(VectorizedExpression expr, Chunk inputChunk, int[] sel, LongBlock expectBlock) {
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.VarcharType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.CharType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {expr.getOutputIndex()})
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
        expr.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(expr.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                if (((Block) expectBlock).isNull(j)) {
                    Assert.assertTrue("Failed at pos: " + j, ((Block) resultBlock).isNull(j));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                if (((Block) expectBlock).isNull(i)) {
                    Assert.assertTrue("Failed at pos: " + i, ((Block) resultBlock).isNull(i));
                    continue;
                }
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }

    private VectorizedExpression buildVecExpression() {
        final String targetExprName = "InVarcharColCharConst2OperandsVectorizedExpression";

        ArgumentInfo arg1 = new ArgumentInfo(DataTypes.VarcharType, ArgumentKind.Variable);
        ArgumentInfo arg2 = new ArgumentInfo(DataTypes.CharType, ArgumentKind.Const);
        ArgumentInfo arg3 = new ArgumentInfo(DataTypes.CharType, ArgumentKind.Const);
        ExpressionSignature sig = new ExpressionSignature("IN", new ArgumentInfo[] {arg1, arg2, arg3});

        Optional<ExpressionConstructor<?>> constructor =
            VectorizedExpressionRegistry.builderConstructorOf(sig);
        assertTrue("Construct of " + sig + " should exist.", constructor.isPresent());
        assertEquals("Class of " + sig + " should be " + targetExprName,
            targetExprName, constructor.get().getDeclaringClass().getSimpleName());

        VectorizedExpression expr = constructor.get().build(3, new VectorizedExpression[] {
            new InputRefVectorizedExpression(DataTypes.VarcharType, 0, 0),
            new LiteralVectorizedExpression(DataTypes.CharType, "val1", 1),
            new LiteralVectorizedExpression(DataTypes.CharType, "val2", 2)
        });
        return expr;
    }

}