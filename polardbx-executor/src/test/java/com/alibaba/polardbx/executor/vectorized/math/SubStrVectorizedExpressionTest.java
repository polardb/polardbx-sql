package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.SubStrCharVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.SubStrVarcharVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SubStrVectorizedExpressionTest extends BaseExecTest {
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
    public void test1() {
        doTest(2, 5, true);
        doTest(2, 5, false);
    }

    @Test
    public void test2() {
        doTest(-5, 2, true);
        doTest(-5, 2, false);
    }

    @Test
    public void test3() {
        doTest(-3, 4, true);
        doTest(-3, 4, false);
    }

    @Test
    public void test4() {
        doTest(-1, 3, true);
        doTest(-1, 3, false);
    }

    /**
     * @param isVarchar is isVarchar or char
     */
    protected void doTest(int startPos, int subStrLen, boolean isVarchar) {
        final SliceType sliceType = new SliceType();
        final CharType charType = new CharType();
        final int positionCount = context.getExecutorChunkLimit();
        final int nullCount = 20;
        final int lowerBound = 0; // 0.00
        final int upperBound = 1000; // 10.00
        final SqlOperator operator = TddlOperatorTable.SUBSTRING;

        List<DataType<?>> inputTypes = isVarchar ? ImmutableList.of(sliceType) : ImmutableList.of(charType);

        RexNode root = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            operator,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(isVarchar ? TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)
                    : TYPE_FACTORY.createSqlType(SqlTypeName.CHAR), 0),
                REX_BUILDER.makeBigIntLiteral(Long.valueOf(startPos)),
                REX_BUILDER.makeBigIntLiteral(Long.valueOf(subStrLen))
            ));

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());

        VectorizedExpression expression = root.accept(converter);
        if (isVarchar) {
            Assert.assertEquals(SubStrVarcharVectorizedExpression.class, expression.getClass());
        } else {
            Assert.assertEquals(SubStrCharVectorizedExpression.class, expression.getClass());
        }

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(positionCount)
            .addEmptySlots(inputTypes)
            .addEmptySlots(converter.getOutputDataTypes())
            .build();

        // build input decimal block
        SliceBlock inputBlock =
            generateSliceBlock(sliceType, positionCount, nullCount, lowerBound, upperBound);
        Chunk inputChunk = new Chunk(positionCount, inputBlock);

        ReferenceBlock outputBlock = (ReferenceBlock) BlockUtils.createBlock(sliceType, inputChunk.getPositionCount());

        preAllocatedChunk.setSelection(null);
        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.setSlotAt(inputBlock, 0);
        preAllocatedChunk.setSlotAt(outputBlock, expression.getOutputIndex());
        preAllocatedChunk.setBatchSize(positionCount);

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            Object actual = outputBlock.getObject(i);

            Object expected = null;
            if (!inputBlock.isNull(i)) {
                Slice slice = inputBlock.getRegion(i);
                int start = startPos < 0 ? slice.length() + startPos : startPos - 1;
                int length = Math.min(subStrLen, slice.length() - start);
                if (start + 1 > slice.length()) {
                    expected = Slices.EMPTY_SLICE;
                } else {
                    expected = slice.slice(start, length);
                }
            }

            boolean condition1 = actual == expected && expected == null;
            boolean condition2 = actual instanceof Slice && expected instanceof Slice
                && ((Slice) actual).compareTo((Slice) expected) == 0;

            try {
                Assert.assertTrue(
                    MessageFormat.format("actual = {0}, expect = {1}",
                        actual == null ? null : ((Slice) actual).toStringUtf8(),
                        expected == null ? null : ((Slice) expected).toStringUtf8()
                    ),
                    condition1 || condition2);
            } catch (Throwable t) {
                throw t;
            }

        }
    }

    private SliceBlock generateSliceBlock(SliceType sliceType, int positionCount, int nullCount,
                                          int lowerBound, int upperBound) {
        SliceBlockBuilder blockBuilder = new SliceBlockBuilder(sliceType, positionCount, context, false);
        for (int i = 0; i < positionCount; i++) {
            if (RANDOM.nextInt(positionCount) < nullCount) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeSlice(randomSlice());
            }
        }
        return (SliceBlock) blockBuilder.build();
    }

    private Slice randomSlice() {
        final int size = 10;
        final int starAscii = 33;
        final int endAscii = 125;
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (RANDOM.nextInt(endAscii - starAscii) + starAscii);
        }
        return Slices.wrappedBuffer(bytes);
    }
}
