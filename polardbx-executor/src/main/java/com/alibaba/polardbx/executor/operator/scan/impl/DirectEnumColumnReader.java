package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.apache.orc.customized.ORCDataOutput;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.nio.charset.Charset;

public class DirectEnumColumnReader extends DirectVarcharColumnReader {

    private final DataType dataType;
    private final Charset charset;

    public DirectEnumColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                                  RuntimeMetrics metrics, int indexStride, boolean enableMetrics, DataType inputType) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride, enableMetrics);
        this.dataType = inputType;
        this.charset = Charset.forName(dataType.getCharsetName().getJavaCharset());
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

        long totalLength = 0;
        if (present == null) {
            randomAccessBlock.setHasNull(false);

            if (lengthReader != null) {
                for (int i = 0; i < positionCount; i++) {
                    // no null value.
                    long length = lengthReader.next();
                    totalLength += length;

                    offsets[i] = (int) totalLength;
                    nulls[i] = false;
                    lastPosition++;
                }
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
                    offsets[i] = (int) totalLength;
                } else {
                    // if not null
                    long length = lengthReader.next();
                    totalLength += length;

                    offsets[i] = (int) totalLength;
                    nulls[i] = false;
                }
                lastPosition++;
            }
        }

        // Read all bytes of stream into sliceOutput at once.
        SliceOutput sliceOutput = new DynamicSliceOutput(positionCount);
        ORCDataOutput dataOutput = new SliceOutputWrapper(sliceOutput);
        int len = (int) totalLength;
        while (len > 0) {
            int bytesRead = dataStream.read(dataOutput, len);
            if (bytesRead < 0) {
                throw GeneralUtil.nestedException("Can't finish byte read from " + dataStream);
            }
            len -= bytesRead;
        }
        Slice data = sliceOutput.slice();
        block.setData(data.toStringUtf8().toCharArray());

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }
}
