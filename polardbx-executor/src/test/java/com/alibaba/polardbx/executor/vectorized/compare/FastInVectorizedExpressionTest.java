package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FastInVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private final RelDataType longRelType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    private final RelDataType intRelType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    private final RelDataType varcharRelType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    private ExecutionContext context;

    @Before
    public void setUp() {
        this.context = new ExecutionContext();
    }

    @Test
    public void testIntInLong() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues, intRelType), expectBlock);
    }

    @Test
    public void testIntInInt() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        int[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues, intRelType), expectBlock);
    }

    @Test
    public void testIntInIntWithSelection() {
        int[] sel = new int[] {0, 3, 5, 8};
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        int[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, null, 0L, 1L, null, 0L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues, intRelType), expectBlock, sel);
    }

    @Test
    public void testLongInLong() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues, longRelType), expectBlock);
    }

    @Test
    public void testLongInLongWithSelection() {
        int[] sel = new int[] {0, 3, 5, 8};
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, null, 0L, 1L, null, 0L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues, longRelType), expectBlock, sel);
    }

    @Test
    public void testIntInString() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues, intRelType), expectBlock);
    }

    @Test
    public void testIntInStringWithAutoType() {
        context.setParamManager(new ParamManager(new HashMap()));
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_IN_VEC_AUTO_TYPE, "true");

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues, intRelType), expectBlock);
    }

    @Test
    public void testLongInString() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues, longRelType), expectBlock);
    }

    @Test
    public void testLongInStringWithAutoType() {
        context.setParamManager(new ParamManager(new HashMap()));
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_IN_VEC_AUTO_TYPE, "true");

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues, longRelType), expectBlock);
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
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.VarcharType, inputChunk, convertInValues(inValues, varcharRelType), expectBlock);
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
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 1L, null, 1L, 0L);

        doTest(DataTypes.VarcharType, inputChunk, convertInValues(inValues, varcharRelType), expectBlock);
    }

    @Test
    public void testStringInStringWithSelection() {
        int[] sel = new int[] {0, 3, 5, 8};
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
        LongBlock expectBlock = LongBlock.of(1L, 0L, 0L, null, 0L, 1L, null, 0L, 0L);

        doTest(DataTypes.VarcharType, inputChunk, convertInValues(inValues, varcharRelType), expectBlock, sel);
    }

    /**
     * null in (null) is null
     */
    @Test
    public void testLongInLongWithNull() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        Long[] inValues = {1L, 100L, null};
        LongBlock expectBlock = LongBlock.of(1L, 0L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues, longRelType), expectBlock);
    }

    private List<RexNode> convertInValues(long[] inValues, RelDataType dataType) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, dataType));
        for (long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(Long[] inValues, RelDataType dataType) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, dataType));
        for (Long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(int[] inValues, RelDataType dataType) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, dataType));
        for (long inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private List<RexNode> convertInValues(String[] inValues, RelDataType dataType) {
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, dataType));
        for (String inValue : inValues) {
            RexNode rexNode = REX_BUILDER.makeLiteral(inValue,
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }

    private void doTest(DataType inputType, Chunk inputChunk,
                        List<RexNode> rexLiteralList, RandomAccessBlock expectBlock) {
        doTest(inputType, inputChunk, rexLiteralList, expectBlock, null);
    }

    private void doTest(DataType inputType, Chunk inputChunk,
                        List<RexNode> rexLiteralList, RandomAccessBlock expectBlock,
                        int[] sel) {
        boolean enableInAutoTypeConvert = context.getParamManager().getBoolean(ConnectionParams.ENABLE_IN_VEC_AUTO_TYPE);

        int outputIndex = 2;
        VectorizedExpression inputRef = new InputRefVectorizedExpression(inputType, 0, 0);
        VectorizedExpression inValueExpr = InValuesVectorizedExpression.from(rexLiteralList, 2, enableInAutoTypeConvert);
        if (enableInAutoTypeConvert) {
            Assert.assertSame(((InValuesVectorizedExpression) inValueExpr).getInValueSet().getDataType(), inputType);
        }

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(inputChunk.getPositionCount())
            .addSlot((RandomAccessBlock) inputChunk.getBlock(0))
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.LongType,
                DataTypes.LongType
            ))
            .withSelection(sel)
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

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Failed at pos: " + i,
                    resultBlock.elementAt(j), expectBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                Assert.assertEquals("Failed at pos: " + i,
                    resultBlock.elementAt(i), expectBlock.elementAt(i));
            }
        }
    }
}
