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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.io.IOException;

public class IntegerColumnReader extends AbstractLongColumnReader {
    public IntegerColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                               RuntimeMetrics metrics,
                               OrcProto.ColumnEncoding.Kind kind, int indexStride,
                               boolean enableMetrics) {
        super(columnId, isPrimaryKey, stripeLoader, orcIndex, metrics, kind, indexStride, enableMetrics);
    }

    @Override
    public void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof IntegerBlock);
        init();

        long start = System.nanoTime();
        IntegerBlock integerBlock = (IntegerBlock) randomAccessBlock;
        int[] array = integerBlock.intArray();
        boolean[] nulls = integerBlock.nulls();

        if (present == null) {
            randomAccessBlock.setHasNull(false);
            for (int i = 0; i < positionCount; i++) {
                // no null value.
                long longVal = data.next();
                array[i] = (int) longVal;
                lastPosition++;
            }

            // destroy null array to save the memory.
            integerBlock.destroyNulls(true);

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
                    array[i] = (int) longVal;
                }
                lastPosition++;
            }
        }
        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }
    }

    @Override
    public int next(RandomAccessBlock randomAccessBlock, int positionCount, int[] selection, int selSize)
        throws IOException {
        if (selection == null || selSize == 0 || selection.length == 0) {
            next(randomAccessBlock, positionCount);
            return 0;
        }

        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(randomAccessBlock instanceof IntegerBlock);
        init();

        long start = System.nanoTime();
        IntegerBlock integerBlock = (IntegerBlock) randomAccessBlock;
        int[] array = integerBlock.intArray();
        boolean[] nulls = integerBlock.nulls();

        int totalSkipCount = 0;
        if (present == null) {
            randomAccessBlock.setHasNull(false);
            int lastSelectedPos = -1;
            for (int i = 0; i < selSize; i++) {
                int selectedPos = selection[i];

                int skipPos = selectedPos - lastSelectedPos - 1;
                if (skipPos > 0) {
                    data.skip(skipPos);
                    totalSkipCount += skipPos;
                    lastPosition += skipPos;
                }
                array[selectedPos] = (int) data.next();
                lastPosition++;

                lastSelectedPos = selectedPos;
            }

            ((Block) randomAccessBlock).destroyNulls(true);

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
                    array[i] = (int) longVal;
                }
                lastPosition++;
            }
        }
        // metrics
        if (enableMetrics) {
            parseTimer.inc(System.nanoTime() - start);
        }

        return totalSkipCount;
    }
}
