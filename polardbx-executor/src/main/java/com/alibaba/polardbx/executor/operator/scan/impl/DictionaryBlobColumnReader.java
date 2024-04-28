package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlobBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class DictionaryBlobColumnReader extends AbstractDictionaryColumnReader {

    public DictionaryBlobColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                                      RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                      boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof BlobBlock);
        init();

        long start = System.nanoTime();

        BlobBlock block = (BlobBlock) randomAccessBlock;
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);
        java.sql.Blob[] blobs = ((BlobBlock) randomAccessBlock).blobArray();

        // if dictionary is null, all the value in this column is null and the present stream must be not null.
        if (dictionary == null) {
            Preconditions.checkArgument(present != null);
        }

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                int dictId = (int) dictIdReader.next();
                Slice value = dictionary.getValue(dictId);
                blobs[i] = new Blob(value.getBytes());
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
                    blobs[i] = new Blob(value.getBytes());
                }
                lastPosition++;
            }
        }

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }
}
