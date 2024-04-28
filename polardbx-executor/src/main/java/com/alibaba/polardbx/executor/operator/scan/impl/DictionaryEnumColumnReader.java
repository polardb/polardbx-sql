package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.util.Arrays;

public class DictionaryEnumColumnReader extends AbstractDictionaryColumnReader {

    public DictionaryEnumColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                                      RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                      boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof EnumBlock);
        init();

        long start = System.nanoTime();

        EnumBlock block = (EnumBlock) randomAccessBlock;
        int[] offsets = block.getOffsets();
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(offsets != null && offsets.length == positionCount);
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        // if dictionary is null, all the value in this column is null and the present stream must be not null.
        if (dictionary == null) {
            Preconditions.checkArgument(present != null);
        }
        CharArrayList data = new CharArrayList(positionCount * BlockBuilders.EXPECTED_STRING_LEN);

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                int dictId = (int) dictIdReader.next();
                Slice value = dictionary.getValue(dictId);
                String strVal = value.toStringUtf8();
                for (int idx = 0; idx < strVal.length(); idx++) {
                    data.add(strVal.charAt(idx));
                }
                offsets[i] = data.size();
                lastPosition++;
            }
            // destroy null array to save the memory.
            block.destroyNulls(true);
        } else {
            randomAccessBlock.setHasNull(true);
            // there are some null values
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    nulls[i] = true;
                } else {
                    // if not null
                    int dictId = (int) dictIdReader.next();
                    Slice value = dictionary.getValue(dictId);
                    String strVal = value.toStringUtf8();
                    for (int idx = 0; idx < strVal.length(); idx++) {
                        data.add(strVal.charAt(idx));
                    }
                    offsets[i] = data.size();
                }
                lastPosition++;
            }
        }

        block.setData(Arrays.copyOf(data.elements(), data.size()));

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }
}
