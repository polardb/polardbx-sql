package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

/**
 * The dictionary json column reader is responsible for parsing
 * the dictionary-encoding data from orc into string block.
 */
public class DictionaryJsonColumnReader extends AbstractDictionaryColumnReader {

    public DictionaryJsonColumnReader(int columnId, boolean isPrimaryKey,
                                      StripeLoader stripeLoader, OrcIndex orcIndex,
                                      RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                      boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof StringBlock);
        init();

        long start = System.nanoTime();

        StringBlock block = (StringBlock) randomAccessBlock;
        int[] offsets = block.getOffsets();
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(offsets != null && offsets.length == positionCount);
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        long totalLength = 0;

        // if dictionary is null, all the values in this column are null and the present stream must be not null.
        if (dictionary == null) {
            Preconditions.checkArgument(present != null);
            block.setHasNull(true);

            for (int i = 0; i < positionCount; i++) {
                offsets[i] = (int) totalLength;
                nulls[i] = true;

                lastPosition++;
            }
        } else {
            SliceOutput sliceOutput = new DynamicSliceOutput(positionCount);
            if (present == null) {
                block.setHasNull(false);
                for (int i = 0; i < positionCount; i++) {
                    // no null value.
                    int dictId = (int) dictIdReader.next();
                    Slice sliceValue = dictionary.getValue(dictId);
                    sliceOutput.writeBytes(sliceValue);
                    long length = sliceValue.length();
                    totalLength += length;
                    offsets[i] = (int) totalLength;
                    nulls[i] = false;

                    lastPosition++;
                }

                // destroy null array to save the memory.
                block.destroyNulls(true);
            } else {
                block.setHasNull(true);

                // there are some null values
                for (int i = 0; i < positionCount; i++) {
                    if (present.next() != 1) {
                        // for present
                        nulls[i] = true;
                        offsets[i] = (int) totalLength;
                    } else {
                        // if not null
                        int dictId = (int) dictIdReader.next();
                        Slice sliceValue = dictionary.getValue(dictId);
                        sliceOutput.writeBytes(sliceValue);
                        long length = sliceValue.length();
                        totalLength += length;
                        offsets[i] = (int) totalLength;
                        nulls[i] = false;
                    }
                    lastPosition++;
                }
            }
            block.setData(sliceOutput.slice().toStringUtf8().toCharArray());
        }

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }
}
