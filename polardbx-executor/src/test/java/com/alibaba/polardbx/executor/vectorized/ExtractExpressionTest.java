package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

public class ExtractExpressionTest extends BaseVectorizedExpressionTest {

    private final int count = 1024;
    private final DataType outputDataType = DataTypes.LongType;

    private final MysqlDateTime dateTime = new MysqlDateTime(2024, 4, 24, 21, 0,
        38, 4121);

    private int[] getSelection() {
        int[] selection = new int[count / 2];
        for (int i = 0; i < selection.length; i++) {
            selection[i] = i * 2;
        }
        return selection;
    }

    private void setSelection(MutableChunk chunk) {
        int[] sel = getSelection();
        chunk.setBatchSize(sel.length);
        chunk.setSelection(sel);
        chunk.setSelectionInUse(true);
    }

    @Test
    public void testExtractNull() {
        testExtractNull(false);
    }

    @Test
    public void testExtractNullWithSel() {
        testExtractNull(true);
    }

    @Test
    public void testExtractYear() {
        testExtractYear(false);
    }

    @Test
    public void testExtractYearWithSel() {
        testExtractYear(true);
    }

    @Test
    public void testExtractMonth() {
        testExtractMonth(true);
    }

    @Test
    public void testExtractMonthWithSel() {
        testExtractMonth(true);
    }

    @Test
    public void testExtractDay() {
        testExtractDay(false);
    }

    @Test
    public void testExtractDayWithSel() {
        testExtractDay(true);
    }

    @Test
    public void testExtractHour() {
        testExtractHour(false);
    }

    @Test
    public void testExtractHourWithSel() {
        testExtractHour(true);
    }

    private void testExtractNull(boolean withSelection) {
        ExtractVectorizedExpression expr = mockExtractExpr(null);
        Assert.assertNull(expr.getIntervalType());
        ReferenceBlock refBlock = new ReferenceBlock(DataTypes.StringType, count);
        DateBlock dateBlock = new DateBlock(count, TimeZone.getDefault());
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, dateBlock, outputBlock);
        if (withSelection) {
            setSelection(chunk);
        }

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        if (withSelection) {
            for (int i = 0; i < chunk.batchSize(); i++) {
                int j = chunk.selection()[i];
                Assert.assertTrue(outputBlock.isNull(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertTrue(outputBlock.isNull(i));
            }
        }
    }

    private void testExtractYear(boolean withSelection) {
        ExtractVectorizedExpression expr = mockExtractExpr("Year");
        Assert.assertSame(expr.getIntervalType(), MySQLIntervalType.INTERVAL_YEAR);
        ReferenceBlock refBlock = new ReferenceBlock(DataTypes.StringType, count);
        DateBlock dateBlock = new DateBlock(count, TimeZone.getDefault());
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, dateBlock, outputBlock);
        if (withSelection) {
            setSelection(chunk);
        }

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < chunk.batchSize(); i++) {
                int j = chunk.selection()[i];
                Assert.assertEquals(dateTime.getYear(), outputBlock.getLong(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(dateTime.getYear(), outputBlock.getLong(i));
            }
        }
    }

    private void testExtractMonth(boolean withSelection) {
        ExtractVectorizedExpression expr = mockExtractExpr("Month");
        Assert.assertSame(expr.getIntervalType(), MySQLIntervalType.INTERVAL_MONTH);
        ReferenceBlock refBlock = new ReferenceBlock(DataTypes.StringType, count);
        DateBlock dateBlock = new DateBlock(count, TimeZone.getDefault());
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, dateBlock, outputBlock);
        if (withSelection) {
            setSelection(chunk);
        }

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < chunk.batchSize(); i++) {
                int j = chunk.selection()[i];
                Assert.assertEquals(dateTime.getMonth(), outputBlock.getLong(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(dateTime.getMonth(), outputBlock.getLong(i));
            }
        }
    }

    private void testExtractDay(boolean withSelection) {
        ExtractVectorizedExpression expr = mockExtractExpr("Day");
        Assert.assertSame(expr.getIntervalType(), MySQLIntervalType.INTERVAL_DAY);
        ReferenceBlock refBlock = new ReferenceBlock(DataTypes.StringType, count);
        DateBlock dateBlock = new DateBlock(count, TimeZone.getDefault());
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, dateBlock, outputBlock);
        if (withSelection) {
            setSelection(chunk);
        }

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < chunk.batchSize(); i++) {
                int j = chunk.selection()[i];
                Assert.assertEquals(dateTime.getDay(), outputBlock.getLong(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(dateTime.getDay(), outputBlock.getLong(i));
            }
        }
    }

    public void testExtractHour(boolean withSelection) {
        // No fast path for extracting hour
        ExtractVectorizedExpression expr = mockExtractExpr("Hour");
        Assert.assertSame(expr.getIntervalType(), MySQLIntervalType.INTERVAL_HOUR);
        ReferenceBlock refBlock = new ReferenceBlock(DataTypes.StringType, count);
        DateBlock dateBlock = new DateBlock(count, TimeZone.getDefault());
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, dateBlock, outputBlock);
        if (withSelection) {
            setSelection(chunk);
        }

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals(count, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < chunk.batchSize(); i++) {
                int j = chunk.selection()[i];
                Assert.assertEquals(dateTime.getHour(), outputBlock.getLong(j));
            }
        } else {
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(dateTime.getHour(), outputBlock.getLong(i));
            }
        }
    }

    private ExtractVectorizedExpression mockExtractExpr(String intervalType) {
        VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new LiteralVectorizedExpression(DataTypes.StringType, intervalType, 0);
        children[1] = new VectorizedExpression() {
            @Override
            public void eval(EvaluationContext ctx) {
                MutableChunk chunk = ctx.getPreAllocatedChunk();
                DateBlock outputBlock = (DateBlock) chunk.slotIn(getOutputIndex());

                long[] longArray = outputBlock.getPacked();
                if (chunk.isSelectionInUse()) {
                    for (int i = 0; i < chunk.batchSize(); i++) {
                        int j = chunk.selection()[i];
                        longArray[j] = TimeStorage.writeTimestamp(dateTime);
                    }
                } else {
                    for (int i = 0; i < chunk.batchSize(); i++) {
                        longArray[i] = TimeStorage.writeTimestamp(dateTime);
                    }
                }
            }

            @Override
            public VectorizedExpression[] getChildren() {
                return new VectorizedExpression[0];
            }

            @Override
            public DataType<?> getOutputDataType() {
                return DataTypes.DateType;
            }

            @Override
            public int getOutputIndex() {
                return 1;
            }
        };
        return new ExtractVectorizedExpression(outputDataType, 2, children);
    }

    @Test
    public void testParseInterval() {
        MysqlDateTime dateTime = new MysqlDateTime(2024, 4, 24, 21, 0,
            38, 4121);

        Assert.assertEquals(2024L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_YEAR));
        Assert.assertEquals(4L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_MONTH));
        Assert.assertEquals(24L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_DAY));
        Assert.assertEquals(21L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_HOUR));
        Assert.assertEquals(0L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_MINUTE));
        Assert.assertEquals(38L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_SECOND));
        Assert.assertEquals(2L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_QUARTER));
        Assert.assertEquals(16L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_WEEK));
        Assert.assertEquals(4L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_MICROSECOND));
        Assert.assertEquals(202404L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_YEAR_MONTH));
        Assert.assertEquals(2421L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_DAY_HOUR));
        Assert.assertEquals(242100L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_DAY_MINUTE));
        Assert.assertEquals(24210038L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_DAY_SECOND));
        Assert.assertEquals(2100L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_HOUR_MINUTE));
        Assert.assertEquals(210038L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_HOUR_SECOND));
        Assert.assertEquals(38L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_MINUTE_SECOND));
        Assert.assertEquals(24210038000004L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_DAY_MICROSECOND));
        Assert.assertEquals(210038000004L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_HOUR_MICROSECOND));
        Assert.assertEquals(38000004L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_MINUTE_MICROSECOND));
        Assert.assertEquals(38000004L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_SECOND_MICROSECOND));

        // INTERVAL_LAST is not implemented
        Assert.assertEquals(0L,
            ExtractVectorizedExpression.doParseInterval(dateTime, 1, MySQLIntervalType.INTERVAL_LAST));
    }
}
