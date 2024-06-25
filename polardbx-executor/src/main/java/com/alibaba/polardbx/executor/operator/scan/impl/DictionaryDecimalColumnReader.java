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

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.utils.GeneralUtil;
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
import java.util.Arrays;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;

public class DictionaryDecimalColumnReader extends AbstractDictionaryColumnReader {

    private static final int DEFAULT_BYTE_BUFFER_LENGTH = 64;
    private byte[] byteBuffer = null;

    public DictionaryDecimalColumnReader(int columnId, boolean isPrimaryKey,
                                         StripeLoader stripeLoader, OrcIndex orcIndex,
                                         RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                         boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof DecimalBlock);
        init();

        long start = System.nanoTime();

        DecimalBlock block = (DecimalBlock) randomAccessBlock;
        readToDecimal(block, positionCount);

        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }

    private void readToDecimal(DecimalBlock block, int positionCount) throws IOException {
        boolean[] nulls = block.nulls();
        Slice memorySegments = block.getMemorySegments();
        DecimalType decimalType = (DecimalType) block.getType();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        if (dictionary == null) {
            // all values are null
            Preconditions.checkArgument(present != null);
            block.setHasNull(true);

            for (int i = 0; i < positionCount; i++) {
                nulls[i] = true;
                lastPosition++;
            }

        } else {
            initByteBuffer();
            if (present == null) {
                block.setHasNull(false);
                for (int i = 0; i < positionCount; i++) {
                    // no null value.
                    int dictId = (int) dictIdReader.next();
                    Slice sliceValue = dictionary.getValue(dictId);
                    int len = sliceValue.length();
                    byte[] tmp = getByteBuffer(len);
                    boolean isUtf8FromLatin1 =
                        MySQLUnicodeUtils.utf8ToLatin1(sliceValue, tmp, len);
                    if (!isUtf8FromLatin1) {
                        // in columnar, decimals are stored already in latin1 encoding
                        sliceValue.getBytes(0, tmp, 0, len);
                    }
                    int fromIndex = i * DECIMAL_MEMORY_SIZE;
                    DecimalStructure d2 = new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                    int[] result =
                        DecimalConverter.binToDecimal(tmp, d2, decimalType.getPrecision(), decimalType.getScale());
                    nulls[i] = false;
                    lastPosition++;

                    if (result[1] != DecimalTypeBase.E_DEC_OK) {
                        LOGGER.error(String.format("Decoding dictionary decimal failed, dictId: %d, bytes: [%s]",
                            dictId, Arrays.toString(sliceValue.getBytes())));
                        throw GeneralUtil.nestedException("Error occurs while decoding dictionary decimal");
                    }
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
                    } else {
                        // if not null
                        int dictId = (int) dictIdReader.next();
                        Slice sliceValue = dictionary.getValue(dictId);
                        int len = sliceValue.length();
                        byte[] tmp = getByteBuffer(len);
                        boolean isUtf8FromLatin1 =
                            MySQLUnicodeUtils.utf8ToLatin1(sliceValue, tmp, len);
                        if (!isUtf8FromLatin1) {
                            // in columnar, decimals are stored already in latin1 encoding
                            sliceValue.getBytes(0, tmp, 0, len);
                        }
                        int fromIndex = i * DECIMAL_MEMORY_SIZE;
                        DecimalStructure d2 =
                            new DecimalStructure(memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                        int[] result =
                            DecimalConverter.binToDecimal(tmp, d2, decimalType.getPrecision(), decimalType.getScale());
                        nulls[i] = false;
                        if (result[1] != DecimalTypeBase.E_DEC_OK) {
                            LOGGER.error(String.format("Decoding dictionary decimal failed, dictId: %d, bytes: [%s]",
                                dictId, Arrays.toString(sliceValue.getBytes())));
                            throw GeneralUtil.nestedException("Error occurs while decoding dictionary decimal");
                        }
                    }
                    lastPosition++;
                }
            }
        }
    }

    private void initByteBuffer() {
        if (this.byteBuffer == null) {
            this.byteBuffer = new byte[DEFAULT_BYTE_BUFFER_LENGTH];
        }
    }

    /**
     * @return local byte buffer with ensured capacity
     */
    private byte[] getByteBuffer(int len) {
        if (this.byteBuffer == null) {
            this.byteBuffer = new byte[len];
            return this.byteBuffer;
        }
        if (this.byteBuffer.length < len) {
            this.byteBuffer = new byte[len];
            return this.byteBuffer;
        }
        return byteBuffer;
    }
}
