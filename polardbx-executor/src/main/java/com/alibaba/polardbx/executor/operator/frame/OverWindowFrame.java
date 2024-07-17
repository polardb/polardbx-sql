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

import java.io.Serializable;
import java.util.List;

public interface OverWindowFrame extends Serializable {

    List<Aggregator> getAggregators();

    // 更新该partition涉及的chunks
    void resetChunks(ChunksIndex chunksIndex);

    // 更新该partition在chunksIndex中的边界索引
    void updateIndex(int leftIndex, int rightIndex);

    // 调用window frame处理当前行
    List<Object> processData(int index);
}

