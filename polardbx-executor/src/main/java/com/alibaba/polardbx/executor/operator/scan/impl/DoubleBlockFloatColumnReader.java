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

import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class DoubleBlockFloatColumnReader extends FloatColumnReader {

    public DoubleBlockFloatColumnReader(int columnId, boolean isPrimaryKey,
                                        StripeLoader stripeLoader,
                                        OrcIndex orcIndex,
                                        RuntimeMetrics metrics,
                                        int indexStride, boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof DoubleBlock);
        init();

        long start = System.nanoTime();

        DoubleBlock block = (DoubleBlock) randomAccessBlock;
        double[] vector = block.doubleArray();
        boolean[] nulls = block.nulls();
        Preconditions.checkArgument(nulls != null && nulls.length == positionCount);

        if (present == null) {
            randomAccessBlock.setHasNull(false);

            for (int i = 0; i < positionCount; i++) {
                // no null value.
                float floatVal = utils.readFloat(dataStream);
                vector[i] = floatVal;
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
                    vector[i] = 0;
                    nulls[i] = true;
                } else {
                    // if not null
                    float doubleVal = utils.readFloat(dataStream);
                    vector[i] = doubleVal;
                    nulls[i] = false;
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
