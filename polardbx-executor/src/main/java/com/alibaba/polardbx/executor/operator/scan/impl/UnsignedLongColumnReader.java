package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.datatype.UInt64Utils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class UnsignedLongColumnReader extends AbstractLongColumnReader {
    public UnsignedLongColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader,
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
        Preconditions.checkArgument(randomAccessBlock instanceof ULongBlock);

        init();

        long start = System.nanoTime();
        ULongBlock block = (ULongBlock) randomAccessBlock;
        long[] array = block.longArray();
        boolean[] nulls = block.nulls();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                long longVal = data.next();
                array[i] = longVal ^ UInt64Utils.FLIP_MASK;
                lastPosition++;
            }
            // destroy null array to save the memory.
            if (randomAccessBlock instanceof Block) {
                ((Block) randomAccessBlock).destroyNulls(true);
            }
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
                    array[i] = longVal ^ UInt64Utils.FLIP_MASK;
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
