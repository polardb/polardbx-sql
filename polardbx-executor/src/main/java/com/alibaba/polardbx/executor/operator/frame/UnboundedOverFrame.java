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

import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;

public class UnboundedOverFrame implements OverWindowFrame {

    private Aggregator[] aggregators;
    private ChunksIndex chunksIndex;

    public UnboundedOverFrame(
        Aggregator... aggregators) {
        this.aggregators = aggregators;
    }

    @Override
    public void resetChunks(ChunksIndex chunksIndex) {
        this.chunksIndex = chunksIndex;
    }

    @Override
    public void updateIndex(int leftIndex, int rightIndex) {
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            aggregator = aggregator.getNew();
            aggregators[i] = aggregator;
            for (int j = leftIndex; j <= rightIndex - 1; j++) {
                aggregator.aggregate(chunksIndex.rowAt(j));
            }

        }
    }

    @Override
    public List<Object> processData(int index) {
        return Arrays.stream(aggregators).map(aggregator -> aggregator.eval(chunksIndex.rowAt(index)))
            .collect(Collectors.toList());
    }

    @Override
    public List<Aggregator> getAggregators() {
        return newArrayList(aggregators);
    }
}

