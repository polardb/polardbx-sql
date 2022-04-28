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

package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.calc.Aggregator;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

public class UnboundedOverFrame extends AbstractOverWindowFrame {
    private boolean changePartition = true;
    private int leftIndex;
    private int rightIndex;

    public UnboundedOverFrame(Aggregator... aggregators) {
        super(Arrays.stream(aggregators).collect(Collectors.toList()));
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        changePartition = true;
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    @Override
    public void processData(int index) {
        if (changePartition) {
            aggregators.forEach(t -> {
                t.resetToInitValue(0);
                for (int j = leftIndex; j <= rightIndex - 1; j++) {
                    Chunk.ChunkRow row = chunksIndex.rowAt(j);
                    t.accumulate(0, row.getChunk(), row.getPosition());
                }
            });
            changePartition = false;
        }
    }
}

