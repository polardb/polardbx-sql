package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * An implementation of column reader for Integer Types.
 */
public class LongColumnReader extends AbstractLongColumnReader {
    public LongColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                            RuntimeMetrics metrics, OrcProto.ColumnEncoding.Kind kind, int indexStride,
                            boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof LongBlock
            || randomAccessBlock instanceof DecimalBlock
            || randomAccessBlock instanceof TimestampBlock);

        init();

        long start = System.nanoTime();

        // extract long array from different block implementation.
        long[] vector = null;
        if (randomAccessBlock instanceof LongBlock) {
            vector = ((LongBlock) randomAccessBlock).longArray();
        } else if (randomAccessBlock instanceof DecimalBlock) {
            vector = ((DecimalBlock) randomAccessBlock).getDecimal64Values();
        } else if (randomAccessBlock instanceof TimestampBlock) {
            vector = ((TimestampBlock) randomAccessBlock).getPacked();
        }

        Preconditions.checkArgument(vector != null && vector.length == positionCount);

        boolean[] nulls = randomAccessBlock.nulls();
        if (present == null) {
            randomAccessBlock.setHasNull(false);
            int i = 0;
            for (; i < positionCount && data.hasNext(); i++) {
                // no null value.
                long longVal = data.next();
                vector[i] = longVal;
                lastPosition++;
            }
            if (i < positionCount) {
                throw GeneralUtil.nestedException(MessageFormat.format(
                    "Bad position, positionCount = {0}, workId = {1}", i, metrics.name()
                ));
            }
            ((Block) randomAccessBlock).destroyNulls(true);
        } else {
            randomAccessBlock.setHasNull(true);
            // there are some null values
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    vector[i] = 0;
                    nulls[i] = true;
                } else {
                    // if not null
                    long longVal = data.next();
                    vector[i] = longVal;
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
        Preconditions.checkArgument(randomAccessBlock instanceof LongBlock
            || randomAccessBlock instanceof DecimalBlock
            || randomAccessBlock instanceof TimestampBlock);

        init();

        long start = System.nanoTime();

        // extract long array from different block implementation.
        long[] vector = null;
        if (randomAccessBlock instanceof LongBlock) {
            vector = ((LongBlock) randomAccessBlock).longArray();
        } else if (randomAccessBlock instanceof DecimalBlock) {
            vector = ((DecimalBlock) randomAccessBlock).getDecimal64Values();
        } else if (randomAccessBlock instanceof TimestampBlock) {
            vector = ((TimestampBlock) randomAccessBlock).getPacked();
        }

        Preconditions.checkArgument(vector != null && vector.length == positionCount);

        int totalSkipCount = 0;
        boolean[] nulls = randomAccessBlock.nulls();
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
                vector[selectedPos] = longVal;
                lastPosition++;

                lastSelectedPos = selectedPos;
            }

            ((Block) randomAccessBlock).destroyNulls(true);
        } else {
            randomAccessBlock.setHasNull(true);
            // there are some null values
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    vector[i] = 0;
                    nulls[i] = true;
                } else {
                    // if not null
                    long longVal = data.next();
                    vector[i] = longVal;
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
