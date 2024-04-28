package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

/**
 * The dictionary varchar column reader is responsible for parsing
 * the dictionary-encoding data from orc into slice block.
 */
public class DictionaryVarcharColumnReader extends AbstractDictionaryColumnReader {

    private final boolean enableSliceDict;

    public DictionaryVarcharColumnReader(int columnId, boolean isPrimaryKey,
                                         StripeLoader stripeLoader, OrcIndex orcIndex,
                                         RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                         boolean enableMetrics, boolean enableSliceDict) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
        this.enableSliceDict = enableSliceDict;
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof SliceBlock);
        init();

        long start = System.nanoTime();

        SliceBlock block = (SliceBlock) randomAccessBlock;
        if (enableSliceDict) {
            readToSliceDict(block, positionCount);
        } else {
            readToSliceData(block, positionCount);
        }

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }

    private void readToSliceDict(SliceBlock block, int positionCount) throws IOException {
        int[] dictIds = block.getDictIds();
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(dictIds != null && dictIds.length == positionCount);
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        // if dictionary is null, all the value in this column is null and the present stream must be not null.
        if (dictionary == null) {
            Preconditions.checkArgument(present != null);
            block.setDictionary(LocalBlockDictionary.EMPTY_DICTIONARY);
        } else {
            block.setDictionary(dictionary);
        }

        if (present == null) {
            block.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                int dictId = (int) dictIdReader.next();
                dictIds[i] = dictId;
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
                    dictIds[i] = -1;
                } else {
                    // if not null
                    int dictId = (int) dictIdReader.next();
                    dictIds[i] = dictId;
                }
                lastPosition++;
            }
        }
    }

    private void readToSliceData(SliceBlock block, int positionCount) throws IOException {
        int[] offsets = block.getOffsets();
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(offsets != null && offsets.length == positionCount);
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        long totalLength = 0;

        if (dictionary == null) {
            // all values are null
            Preconditions.checkArgument(present != null);
            block.setHasNull(true);

            for (int i = 0; i < positionCount; i++) {
                offsets[i] = (int) totalLength;
                nulls[i] = true;

                lastPosition++;
            }

            // For block with all null value, set empty slice.
            block.setData(Slices.EMPTY_SLICE);
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
            block.setData(sliceOutput.slice());
        }
    }
}
