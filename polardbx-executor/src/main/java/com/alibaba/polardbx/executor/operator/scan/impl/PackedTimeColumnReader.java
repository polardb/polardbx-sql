package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.TimeBlock;
import com.alibaba.polardbx.executor.chunk.TimeBlockBuilder;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class PackedTimeColumnReader extends AbstractLongColumnReader {
    public PackedTimeColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader,
                                  OrcIndex orcIndex,
                                  RuntimeMetrics metrics,
                                  OrcProto.ColumnEncoding.Kind kind, int indexStride,
                                  boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof DateBlock
            || randomAccessBlock instanceof TimeBlock);
        init();

        long start = System.nanoTime();

        long[] packed = randomAccessBlock instanceof DateBlock
            ? ((DateBlock) randomAccessBlock).getPacked()
            : ((TimeBlock) randomAccessBlock).getPacked();
        boolean[] nulls = randomAccessBlock.nulls();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                long longVal = data.next();
                packed[i] = longVal;
                lastPosition++;
            }

            // destroy null array to save the memory.
            if (randomAccessBlock instanceof Block) {
                ((Block) randomAccessBlock).destroyNulls(true);
            }
        } else {
            randomAccessBlock.setHasNull(true);
            // there are some null values
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    nulls[i] = true;
                    packed[i] = 0;
                } else {
                    // if not null
                    long longVal = data.next();
                    packed[i] = longVal;
                }
                lastPosition++;
            }
        }

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }

    @Override
    public int next(RandomAccessBlock randomAccessBlock, int positionCount, int[] selection, int selSize)
        throws IOException {
        if (selection == null || selSize == 0 || selection.length == 0) {
            next(randomAccessBlock, positionCount);
            return 0;
        }

        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof DateBlock
            || randomAccessBlock instanceof TimeBlock);
        init();

        long start = System.nanoTime();

        long[] packed = randomAccessBlock instanceof DateBlock
            ? ((DateBlock) randomAccessBlock).getPacked()
            : ((TimeBlock) randomAccessBlock).getPacked();
        boolean[] nulls = randomAccessBlock.nulls();

        int totalSkipCount = 0;
        if (present == null) {
            randomAccessBlock.setHasNull(false);

            int lastSelectedPos = -1;
            for (int i = 0; i < selSize; i++) {
                int selectedPos = selection[i];

                int skipPos = selectedPos - lastSelectedPos - 1;
                if (skipPos > 0) {
                    data.skip(skipPos);
                    totalSkipCount += skipPos;
                    lastPosition += skipPos;
                }
                long longVal = data.next();
                packed[selectedPos] = longVal;
                lastPosition++;

                lastSelectedPos = selectedPos;
            }

            // destroy null array to save the memory.
            if (randomAccessBlock instanceof Block) {
                ((Block) randomAccessBlock).destroyNulls(true);
            }

        } else {
            randomAccessBlock.setHasNull(true);
            // there are some null values
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    nulls[i] = true;
                    packed[i] = 0;
                } else {
                    // if not null
                    long longVal = data.next();
                    packed[i] = longVal;
                }
                lastPosition++;
            }
        }

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }

        return totalSkipCount;
    }
}
