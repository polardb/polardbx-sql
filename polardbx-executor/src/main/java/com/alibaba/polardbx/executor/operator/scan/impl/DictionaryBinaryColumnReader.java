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

import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;
import java.util.Arrays;

public class DictionaryBinaryColumnReader extends AbstractDictionaryColumnReader {

    public DictionaryBinaryColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader,
                                        OrcIndex orcIndex,
                                        RuntimeMetrics metrics, OrcProto.ColumnEncoding encoding, int indexStride,
                                        boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding, indexStride, enableMetrics);
    }

    /**
     * No dictionary in ByteArrayBlock
     */
    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof ByteArrayBlock);
        init();

        long start = System.nanoTime();

        ByteArrayBlock block = (ByteArrayBlock) randomAccessBlock;
        boolean[] nulls = block.nulls();
        int[] offsets = block.getOffsets();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);
        Preconditions.checkArgument(offsets != null && offsets.length == positionCount);
        ByteArrayList data = new ByteArrayList(positionCount * BlockBuilders.EXPECTED_BYTE_ARRAY_LEN);

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
                data.addElements(data.size(), value.getBytes());
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
                    offsets[i] = data.size();
                } else {
                    // if not null
                    int dictId = (int) dictIdReader.next();
                    Slice value = dictionary.getValue(dictId);
                    data.addElements(data.size(), value.getBytes());
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
