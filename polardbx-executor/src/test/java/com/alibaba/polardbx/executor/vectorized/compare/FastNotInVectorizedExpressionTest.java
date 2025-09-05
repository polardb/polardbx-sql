package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.executor.chunk.TimestampBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InValuesVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FastNotInVectorizedExpressionTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void testIntNotInLong() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testIntNotInInt() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        int[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testIntNotInIntWithSelection() {
        int[] sel = new int[] {0, 3, 5, 8};
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        int[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.IntegerType, inputChunk, convertInValues(inValues), expectBlock, sel);
    }

    @Test
    public void testLongNotInLong() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testLongNotInLongWithSelection() {
        int[] sel = new int[] {0, 3, 5, 8};
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock);
        long[] inValues = {1, 100, 1000};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock, sel);
    }

    @Test
    public void testIntNotInString() {
        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testLongNotInString() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        String[] inValues = {"1", "100", "1000"};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testStringNotInLong() {
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
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testStringNotInString() {
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
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testStringNotInStringWithSelection() {
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
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, null, 1L, 0L, null, 0L, 1L);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock, sel);
    }

    @Test
    public void testLongNotInLongWithNull() {
        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);
        Long[] inValues = {1L, 100L, null};
        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTest(DataTypes.LongType, inputChunk, convertInValues(inValues), expectBlock);
    }

    @Test
    public void testDateNotInString() {

        DateBlockBuilder blockBuilder = new DateBlockBuilder(16, DataTypes.DateType, new ExecutionContext());
        blockBuilder.writeByteArray("0000-00-00".getBytes()); // 0
        blockBuilder.writeByteArray("2000-02-01".getBytes()); // 1
        blockBuilder.writeByteArray("2020-12-11".getBytes()); // 0
        blockBuilder.writeByteArray("2013-09-10".getBytes()); // 1
        blockBuilder.writeByteArray("2000-10-10".getBytes()); // 0
        blockBuilder.writeByteArray(null); // null
        blockBuilder.writeByteArray("2003-06-20".getBytes()); // 1
        blockBuilder.writeByteArray(null); // null
        Block block = blockBuilder.build();

        Chunk inputChunk = new Chunk(block.getPositionCount(), block);
        String[] inValues = {"2000-10-10 11:11:11", "2020-12-11", "0000-00-00 00:00:00"};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, 1L, 0L, null, 1L, null);

        doTest(DataTypes.DateType, inputChunk,
            convertInValues(inValues, TYPE_FACTORY.createSqlType(SqlTypeName.DATE)),
            expectBlock);
    }

    @Test
    public void testDatetimeNotInString() {
        TimestampBlockBuilder blockBuilder =
            new TimestampBlockBuilder(16, DateTimeType.DATE_TIME_TYPE_2, new ExecutionContext());
        blockBuilder.writeByteArray("0000-00-00 00:00:00.00".getBytes()); // 0
        blockBuilder.writeByteArray("2000-02-01".getBytes()); // 1
        blockBuilder.writeByteArray("2020-12-11 13:00:00.01".getBytes()); // 0
        blockBuilder.writeByteArray("2013-09-10".getBytes()); // 1
        blockBuilder.writeByteArray("2000-10-10 11:11:11.12".getBytes()); // 0
        blockBuilder.writeByteArray(null); // null
        blockBuilder.writeByteArray("2003-06-20".getBytes()); // 1
        blockBuilder.writeByteArray(null); // null
        Block block = blockBuilder.build();

        Chunk inputChunk = new Chunk(block.getPositionCount(), block);
        String[] inValues = {"2000-10-10 11:11:11.12", "2020-12-11 13:00:00.01", "0000-00-00 00:00:00"};
        LongBlock expectBlock = LongBlock.of(0L, 1L, 0L, 1L, 0L, null, 1L, null);

        doTest(DateTimeType.DATE_TIME_TYPE_2, inputChunk,
            convertInValues(inValues, TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 2)),
            expectBlock);
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
        doTest(inputType, inputChunk, rexLiteralList, expectBlock, null);
    }

    private void doTest(DataType inputType, Chunk inputChunk,
                        List<RexNode> rexLiteralList, RandomAccessBlock expectBlock,
                        int[] sel) {
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
            .withSelection(sel)
            .build();
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(outputIndex);

        VectorizedExpression inExpr = new FastNotInVectorizedExpression(
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
