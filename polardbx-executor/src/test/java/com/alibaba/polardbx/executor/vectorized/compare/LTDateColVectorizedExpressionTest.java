package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
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

public class LTDateColVectorizedExpressionTest {

    private final int count = 10;
    private ExecutionContext context;
    private List<MysqlDateTime> dateList1;
    private List<MysqlDateTime> dateList2;

    @Before
    public void before() {
        this.context = new ExecutionContext();
        this.dateList1 = new ArrayList<>(count);
        this.dateList2 = new ArrayList<>(count);
        for (int i = 0; i < count - 1; i++) {
            MysqlDateTime date = new MysqlDateTime();
            date.setYear(2024);
            date.setMonth(5);
            date.setDay(i + 1);
            dateList1.add(date);
        }
        dateList1.add(null);
        dateList2.add(null);
        for (int i = 0; i < count - 1; i++) {
            MysqlDateTime date = new MysqlDateTime();
            date.setYear(2024);
            if (i % 2 == 0) {
                date.setMonth(6);
            } else {
                date.setMonth(4);
            }
            date.setDay(i + 1);
            dateList2.add(date);
        }
    }

    @Test
    public void testDateDate() {
        DateBlock dateBlock1 = getDateBlock1();
        DateBlock dateBlock2 = getDateBlock2();
        Chunk inputChunk = new Chunk(dateBlock1.getPositionCount(), dateBlock1, dateBlock2);

        LongBlock expectBlock = LongBlock.of(null, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, null);

        doTest(inputChunk, expectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testDateRef() {
        DateBlock dateBlock1 = getDateBlock1();
        ReferenceBlock dateBlock2 = getRefBlock2();
        Chunk inputChunk = new Chunk(dateBlock1.getPositionCount(), dateBlock1, dateBlock2);

        LongBlock expectBlock = LongBlock.of(null, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, null);

        doTest(inputChunk, expectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testRefDate() {
        ReferenceBlock dateBlock1 = getRefBlock1();
        DateBlock dateBlock2 = getDateBlock2();
        Chunk inputChunk = new Chunk(dateBlock1.getPositionCount(), dateBlock1, dateBlock2);

        LongBlock expectBlock = LongBlock.of(null, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, null);

        doTest(inputChunk, expectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(inputChunk, expectBlock, sel);
    }

    @Test
    public void testRefRef() {
        ReferenceBlock dateBlock1 = getRefBlock1();
        ReferenceBlock dateBlock2 = getRefBlock2();
        Chunk inputChunk = new Chunk(dateBlock1.getPositionCount(), dateBlock1, dateBlock2);

        LongBlock expectBlock = LongBlock.of(null, 1L, 0L, 1L, 0L, 1L, 0L, 1L, 0L, null);

        doTest(inputChunk, expectBlock, null);

        int[] sel = new int[] {0, 1, 2, 4, 5, 9};
        doTest(inputChunk, expectBlock, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        LTDateColDateColVectorizedExpression condition =
            new LTDateColDateColVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.DateType, 0, 0),
                    new InputRefVectorizedExpression(DataTypes.DateType, 1, 1),
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

    private DateBlock getDateBlock1() {
        DateBlockBuilder dateBlockBuilder1 = new DateBlockBuilder(count, DataTypes.DateType, context);
        for (int i = 0; i < count; i++) {
            MysqlDateTime dateTime = dateList1.get(i);
            if (dateTime == null) {
                dateBlockBuilder1.appendNull();
            } else {
                dateBlockBuilder1.writeMysqlDatetime(dateTime);
            }
        }
        return (DateBlock) dateBlockBuilder1.build();
    }

    private DateBlock getDateBlock2() {
        DateBlockBuilder dateBlockBuilder2 = new DateBlockBuilder(count, DataTypes.DateType, context);
        for (int i = 0; i < count; i++) {
            MysqlDateTime dateTime = dateList2.get(i);
            if (dateTime == null) {
                dateBlockBuilder2.appendNull();
            } else {
                dateBlockBuilder2.writeMysqlDatetime(dateTime);
            }
        }
        return (DateBlock) dateBlockBuilder2.build();
    }

    private ReferenceBlock getRefBlock1() {
        ObjectBlockBuilder refBuilder1 = new ObjectBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            MysqlDateTime dateTime = dateList1.get(i);
            if (dateTime == null) {
                refBuilder1.appendNull();
            } else {
                refBuilder1.writeObject(new OriginalDate(dateTime));
            }
        }
        return (ReferenceBlock) refBuilder1.build();
    }

    private ReferenceBlock getRefBlock2() {
        ObjectBlockBuilder refBuilder2 = new ObjectBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            MysqlDateTime dateTime = dateList2.get(i);
            if (dateTime == null) {
                refBuilder2.appendNull();
            } else {
                refBuilder2.writeObject(new OriginalDate(dateTime));
            }
        }
        return (ReferenceBlock) refBuilder2.build();
    }
}
