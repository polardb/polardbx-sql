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

import com.alibaba.polardbx.executor.chunk.BigIntegerBlock;
import com.alibaba.polardbx.executor.chunk.BigIntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

public class BigBitColumnReader extends AbstractLongColumnReader {

    private static final byte[] EMPTY_PACKET = new byte[BigIntegerBlock.LENGTH];

    public BigBitColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                              RuntimeMetrics metrics,
                              OrcProto.ColumnEncoding.Kind kind, int indexStride,
                              boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof BigIntegerBlock);
        init();

        long start = System.nanoTime();
        BigIntegerBlock bigIntegerBlock = (BigIntegerBlock) randomAccessBlock;
        byte[] blockData = bigIntegerBlock.getData();
        boolean[] nulls = bigIntegerBlock.nulls();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                long longVal = data.next();
                final byte[] bytes = toBigIntegerBytes(longVal);
                appendBigIntegerBytes(blockData, i, bytes);
                lastPosition++;
            }

            // destroy null array to save the memory.
            bigIntegerBlock.destroyNulls(true);

        } else {
            // there are some null values
            randomAccessBlock.setHasNull(true);
            for (int i = 0; i < positionCount; i++) {
                if (present.next() != 1) {
                    // for present
                    nulls[i] = true;
                    appendBigIntegerBytes(blockData, i, EMPTY_PACKET);
                } else {
                    // if not null
                    long longVal = data.next();
                    final byte[] bytes = toBigIntegerBytes(longVal);
                    appendBigIntegerBytes(blockData, i, bytes);
                }
                lastPosition++;
            }
        }
        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }

    /**
     * @param bytes from BigInteger with checked length
     */
    private void appendBigIntegerBytes(byte[] blockData, int position, byte[] bytes) {
        int offset = position * BigIntegerBlock.LENGTH;
        if (offset >= blockData.length || offset + BigIntegerBlock.LENGTH > blockData.length) {
            throw new ArrayIndexOutOfBoundsException("BigIntegerBlock data length=" + blockData.length
                + ", offset=" + offset);
        }
        System.arraycopy(bytes, 0, blockData, offset, bytes.length);
        int idx = bytes.length;
        if (bytes.length == BigIntegerBlock.LENGTH) {
            // when it is an empty packet
            return;
        }
        for (; idx < BigIntegerBlock.UNSCALED_LENGTH; idx++) {
            blockData[offset + idx] = (byte) 0;
        }
        blockData[offset + idx] = (byte) bytes.length;
    }

    private byte[] toBigIntegerBytes(long longVal) {
        BigInteger bigInteger = BigInteger.valueOf(longVal);
        final byte[] bytes = bigInteger.toByteArray();
        if (bytes.length > BigIntegerBlock.UNSCALED_LENGTH) {
            throw new AssertionError("decimal with unexpected digits number");
        }
        return bytes;
    }
}
