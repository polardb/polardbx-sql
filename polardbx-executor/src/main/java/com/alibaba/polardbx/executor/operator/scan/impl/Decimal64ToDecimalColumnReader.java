package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.text.MessageFormat;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;

/**
 * An implementation of column reader for reading Decimal64 values into a normal DecimalBlock
 */
public class Decimal64ToDecimalColumnReader extends AbstractLongColumnReader {
    public Decimal64ToDecimalColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader,
                                          OrcIndex orcIndex,
                                          RuntimeMetrics metrics, OrcProto.ColumnEncoding.Kind kind, int indexStride,
                                          boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof DecimalBlock);

        init();

        long start = System.nanoTime();

        DecimalBlock block = (DecimalBlock) randomAccessBlock;
        boolean[] nulls = block.nulls();
        Slice memorySegments = block.getMemorySegments();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);
        DecimalType decimalType = (DecimalType) block.getType();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            int i = 0;
            for (; i < positionCount && data.hasNext(); i++) {
                // no null value.
                long longVal = data.next();
                int fromIndex = i * DECIMAL_MEMORY_SIZE;
                DecimalStructure d2 = new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                d2.setLongWithScale(longVal, decimalType.getScale());
                nulls[i] = false;
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
                    nulls[i] = true;
                } else {
                    // if not null
                    long longVal = data.next();
                    int fromIndex = i * DECIMAL_MEMORY_SIZE;
                    DecimalStructure d2 = new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                    d2.setLongWithScale(longVal, decimalType.getScale());
                    nulls[i] = false;
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

        init();

        long start = System.nanoTime();

        DecimalBlock block = (DecimalBlock) randomAccessBlock;
        boolean[] nulls = block.nulls();
        Slice memorySegments = block.getMemorySegments();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);
        DecimalType decimalType = (DecimalType) block.getType();

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
                int fromIndex = selectedPos * DECIMAL_MEMORY_SIZE;
                DecimalStructure d2 = new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                d2.setLongWithScale(longVal, decimalType.getScale());
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
                    nulls[i] = true;
                } else {
                    // if not null
                    long longVal = data.next();
                    int fromIndex = i * DECIMAL_MEMORY_SIZE;
                    DecimalStructure d2 = new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                    d2.setLongWithScale(longVal, decimalType.getScale());
                    nulls[i] = false;
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
