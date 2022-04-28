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

import java.util.List;

/**
 * The sliding window frame.
 */
public abstract class SlidingOverFrame extends AbstractOverWindowFrame {

    // 保存上次处理的当前行的范围，如果完全一致则可直接返回结果，无需重复计算
    // updateIndex时重置，即每计算一个新的partition时重置
    protected int prevLeftIndex = -1;
    protected int prevRightIndex = -1;

    public SlidingOverFrame(List<Aggregator> aggregators) {
        super(aggregators);
    }

    public void process(int leftIndex, int rightIndex) {
        if (leftIndex == prevLeftIndex && rightIndex == prevRightIndex) {
            return;
        }
        prevLeftIndex = leftIndex;
        prevRightIndex = rightIndex;
        aggregators.forEach(t -> {
            t.resetToInitValue(0);
            for (int j = leftIndex; j <= rightIndex; j++) {
                Chunk.ChunkRow row = chunksIndex.rowAt(j);
                t.accumulate(0, row.getChunk(), row.getPosition());
            }
        });
    }

}
