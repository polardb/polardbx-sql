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
import com.alibaba.polardbx.executor.calc.Aggregator;

import java.util.List;

public abstract class AbstractOverWindowFrame implements OverWindowFrame {
    protected List<Aggregator> aggregators;
    protected ChunksIndex chunksIndex;

    public AbstractOverWindowFrame(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    @Override
    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    @Override
    public void resetChunks(ChunksIndex chunksIndex) {
        this.chunksIndex = chunksIndex;
    }
}
