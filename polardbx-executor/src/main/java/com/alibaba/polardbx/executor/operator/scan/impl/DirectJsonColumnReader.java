/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
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

public class DirectJsonColumnReader extends DirectVarcharColumnReader {

    private final DataType dataType;
    private final Charset charset;

    public DirectJsonColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                                  RuntimeMetrics metrics, int indexStride, boolean enableMetrics, DataType inputType) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride, enableMetrics);
        this.dataType = inputType;
        this.charset = Charset.forName(dataType.getCharsetName().getJavaCharset());
    }

    private String readJsonString(long length) throws IOException {
        byte[] bytes = new byte[(int) length];
        int num = dataStream.read(bytes);
        if (num < length) {
            throw new IOException("Failed to read string with length: " + length);
        }
        return new String(bytes, charset);
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
