package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.ByteBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ShortBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class ByteColumnReader extends AbstractLongColumnReader {
    public ByteColumnReader(int columnId, boolean isPrimaryKey,
                            StripeLoader stripeLoader, OrcIndex orcIndex,
                            RuntimeMetrics metrics,
                            OrcProto.ColumnEncoding.Kind kind, int indexStride,
                            boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof ByteBlock);
        init();

        long start = System.nanoTime();
        ByteBlock byteBlock = (ByteBlock) randomAccessBlock;
        byte[] array = byteBlock.byteArray();
        boolean[] nulls = byteBlock.nulls();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                long longVal = data.next();
                array[i] = (byte) longVal;
                lastPosition++;
            }

            // destroy null array to save the memory.
            byteBlock.destroyNulls(true);

        } else {
            // there are some null values
            randomAccessBlock.setHasNull(true);
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    nulls[i] = true;
                    array[i] = 0;
                } else {
                    // if not null
                    long longVal = data.next();
                    array[i] = (byte) longVal;
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
