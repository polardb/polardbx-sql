package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BetweenDatetimeColCharConstCharConstVectorizedExpressionTest {

    private final int count = 10;
    private ExecutionContext context;
    private List<MysqlDateTime> dateList;

    @Before
    public void before() {
        this.context = new ExecutionContext();
        this.dateList = new ArrayList<>(count);
        for (int i = 0; i < count - 1; i++) {
            MysqlDateTime date = new MysqlDateTime();
            date.setYear(2024);
            date.setMonth(5);
            date.setDay(i + 1);
            dateList.add(date);
        }
        dateList.add(null);
    }

    @Test
    public void testTimestamp() {
        TimestampBlockBuilder timestampBlockBuilder = new TimestampBlockBuilder(count, DataTypes.TimestampType, context);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                timestampBlockBuilder.appendNull();
            } else {
                timestampBlockBuilder.writeMysqlDatetime(mysqlDateTime);
            }
        }
        TimestampBlock dateBlock = (TimestampBlock) timestampBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "2024-05-01", "2024-05-03", null);
    }

    @Test
    public void testTimestampWithSelection() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        TimestampBlockBuilder timestampBlockBuilder = new TimestampBlockBuilder(count, DataTypes.TimestampType, context);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                timestampBlockBuilder.appendNull();
            } else {
                timestampBlockBuilder.writeMysqlDatetime(mysqlDateTime);
            }
        }
        TimestampBlock dateBlock = (TimestampBlock) timestampBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "2024-05-01", "2024-05-03", sel);
    }

    @Test
    public void testRef() {
        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(new OriginalTimestamp(mysqlDateTime));
            }
        }
        ReferenceBlock dateBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "2024-05-01", "2024-05-03", null);
    }

    @Test
    public void testRefWithSel() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};

        ObjectBlockBuilder objectBlockBuilder = new ObjectBlockBuilder(count);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                objectBlockBuilder.appendNull();
            } else {
                objectBlockBuilder.writeObject(new OriginalTimestamp(mysqlDateTime));
            }
        }
        ReferenceBlock dateBlock = (ReferenceBlock) objectBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, null);

        doTest(inputChunk, expectBlock, "2024-05-01", "2024-05-03", sel);
    }

    @Test
    public void testNull() {
        TimestampBlockBuilder timestampBlockBuilder = new TimestampBlockBuilder(count, DataTypes.TimestampType, context);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                timestampBlockBuilder.appendNull();
            } else {
                timestampBlockBuilder.writeMysqlDatetime(mysqlDateTime);
            }
        }
        TimestampBlock dateBlock = (TimestampBlock) timestampBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, "2024-05-03", null);
        doTest(inputChunk, expectBlock, "2024-05-01", null, null);
        doTest(inputChunk, expectBlock, null, null, null);
    }

    @Test
    public void testNullWithSel() {
        int[] sel = new int[] {0, 1, 2, 4, 5, 9};

        TimestampBlockBuilder timestampBlockBuilder = new TimestampBlockBuilder(count, DataTypes.TimestampType, context);
        for (MysqlDateTime mysqlDateTime : dateList) {
            if (mysqlDateTime == null) {
                timestampBlockBuilder.appendNull();
            } else {
                timestampBlockBuilder.writeMysqlDatetime(mysqlDateTime);
            }
        }
        TimestampBlock dateBlock = (TimestampBlock) timestampBlockBuilder.build();
        Chunk inputChunk = new Chunk(dateBlock.getPositionCount(), dateBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, "2024-05-03", sel);
        doTest(inputChunk, expectBlock, "2024-05-01", null, sel);
        doTest(inputChunk, expectBlock, null, null, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, String left, String right, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        BetweenDatetimeColCharConstCharConstVectorizedExpression condition =
            new BetweenDatetimeColCharConstCharConstVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.DatetimeType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.CharType, left, 1),
                    new LiteralVectorizedExpression(DataTypes.CharType, right, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.DateType))
            .addEmptySlots(Arrays.asList(DataTypes.CharType, DataTypes.CharType, DataTypes.LongType))
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
