package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class SubStrExpressionTest extends BaseVectorizedExpressionTest {

    private final int count = 1024;
    /**
     * start from index 1
     */
    private final int start = 2;
    private final int length = 5;
    private final DataType outputDataType = DataTypes.LongType;
    private final boolean withSelection;
    private final int[] sel;
    private List<String> strList = new ArrayList<>(count);
    SliceBlockBuilder inputBlockBuilder = new SliceBlockBuilder(new SliceType(), count / 4,
        new ExecutionContext(), false);

    public SubStrExpressionTest(boolean withSelection) {
        this.withSelection = withSelection;
        if (withSelection) {
            this.sel = new int[count / 2];
            for (int i = 0; i < this.sel.length; i++) {
                this.sel[i] = i * 2 + 1;
            }
        } else {
            this.sel = null;
        }
        StringBuilder stringBuilder = new StringBuilder(32);
        for (int i = 0; i < count; i++) {
            if (i % 50 == 0) {
                stringBuilder.append(((char) 'a' + i % 26)).append(i);
            }
            String s = stringBuilder.toString();
            strList.add(s);
            inputBlockBuilder.writeString(s);
        }
    }

    @Parameterized.Parameters(name = "sel={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {true});
        list.add(new Object[] {false});
        return list;
    }

    @Test
    public void testSubStrExprSlice() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExpr();
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                String s = strList.get(j);
                String subStr;
                if (start - 1 + length >= s.length()) {
                    subStr = s.substring(start - 1);
                } else {
                    subStr = s.substring(start - 1, start - 1 + length);
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(j)).toStringUtf8());
            }
        } else {
            for (int i = 0; i < count; i++) {
                String s = strList.get(i);
                String subStr;
                if (start - 1 + length >= s.length()) {
                    subStr = s.substring(start - 1);
                } else {
                    subStr = s.substring(start - 1, start - 1 + length);
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(i)).toStringUtf8());
            }
        }
    }

    @Test
    public void testSubStrExprReference() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExpr();
        ObjectBlockBuilder refBlockBuilder = new ObjectBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            refBlockBuilder.writeObject(inputBlockBuilder.getObject(i));
        }
        ReferenceBlock inputBlock = (ReferenceBlock) refBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                String s = strList.get(j);
                String subStr;
                if (start - 1 + length >= s.length()) {
                    subStr = s.substring(start - 1);
                } else {
                    subStr = s.substring(start - 1, start - 1 + length);
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(j)).toStringUtf8());
            }
        } else {
            for (int i = 0; i < count; i++) {
                String s = strList.get(i);
                String subStr;
                if (start - 1 + length >= s.length()) {
                    subStr = s.substring(start - 1);
                } else {
                    subStr = s.substring(start - 1, start - 1 + length);
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(i)).toStringUtf8());
            }
        }
    }

    @Test
    public void testSubStrExprWithNullArg() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExprWithNullArg();
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertNull(outputBlock.getObject(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertNull(outputBlock.getObject(i));
            }
        }
    }

    /**
     * if length is negative, the result is "" rather than NULL
     */
    @Test
    public void testSubStrExprNegLength() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExpr(start, -length);
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals(0, ((Slice) outputBlock.getObject(j)).length());
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(0, ((Slice) outputBlock.getObject(i)).length());
            }
        }
    }

    /**
     * if start is negative, the substring result begins from the end
     */
    @Test
    public void testSubStrExprNegStart() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExpr(-start, length);
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                String s = strList.get(j);
                int startIndex = s.length() - start + 1;
                String subStr;
                if (startIndex < 0) {
                    subStr = "";
                } else {
                    if (startIndex - 1 + length >= s.length()) {
                        subStr = s.substring(startIndex - 1);
                    } else {
                        subStr = s.substring(startIndex - 1, startIndex - 1 + length);
                    }
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(j)).toStringUtf8());
            }
        } else {
            for (int i = 0; i < count; i++) {
                String s = strList.get(i);
                int startIndex = s.length() - start + 1;
                String subStr;
                if (startIndex <= 0) {
                    subStr = "";
                } else {
                    if (startIndex - 1 + length >= s.length()) {
                        subStr = s.substring(startIndex - 1);
                    } else {
                        subStr = s.substring(startIndex - 1, startIndex - 1 + length);
                    }
                }
                Assert.assertEquals(subStr, ((Slice) outputBlock.getObject(i)).toStringUtf8());
            }
        }
    }

    /**
     * if start is zero, the result is "" rather than NULL
     */
    @Test
    public void testSubStrExprZeroStart() {
        SubStrVarcharVectorizedExpression expr = mockBenchmarkExpr(0, length);
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals(0, ((Slice) outputBlock.getObject(j)).length());
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(0, ((Slice) outputBlock.getObject(i)).length());
            }
        }
    }

    private SubStrVarcharVectorizedExpression mockBenchmarkExpr() {
        return mockBenchmarkExpr(start, length);
    }

    private SubStrVarcharVectorizedExpression mockBenchmarkExpr(int start, int length) {
        VectorizedExpression[] children = new VectorizedExpression[3];
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, start, 0);
        children[2] = new LiteralVectorizedExpression(DataTypes.LongType, length, 0);
        children[0] = new VectorizedExpression() {
            @Override
            public void eval(EvaluationContext ctx) {

            }

            @Override
            public VectorizedExpression[] getChildren() {
                return new VectorizedExpression[0];
            }

            @Override
            public DataType<?> getOutputDataType() {
                return DataTypes.StringType;
            }

            @Override
            public int getOutputIndex() {
                return 0;
            }
        };
        return new SubStrVarcharVectorizedExpression(outputDataType, 1, children);
    }

    private SubStrVarcharVectorizedExpression mockBenchmarkExprWithNullArg() {
        VectorizedExpression[] children = new VectorizedExpression[3];
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, null, 0);
        children[2] = new LiteralVectorizedExpression(DataTypes.LongType, null, 0);
        children[0] = new VectorizedExpression() {
            @Override
            public void eval(EvaluationContext ctx) {

            }

            @Override
            public VectorizedExpression[] getChildren() {
                return new VectorizedExpression[0];
            }

            @Override
            public DataType<?> getOutputDataType() {
                return DataTypes.StringType;
            }

            @Override
            public int getOutputIndex() {
                return 0;
            }
        };
        return new SubStrVarcharVectorizedExpression(outputDataType, 1, children);
    }
}
