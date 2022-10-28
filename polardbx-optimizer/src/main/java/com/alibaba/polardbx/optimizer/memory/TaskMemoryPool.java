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

package com.alibaba.polardbx.optimizer.memory;

import com.alibaba.polardbx.optimizer.statis.MemoryStatisticsGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskMemoryPool extends MemoryPool {

    private Map<String, MemoryStatisticsGroup> memoryStatistics = new ConcurrentHashMap<>();

    public TaskMemoryPool(String name, long limit, MemoryPool parent) {
        super(name, limit, parent, MemoryType.TASK);
    }

    public QueryMemoryPool getQueryMemoryPool() {
        if (parent != null && parent instanceof QueryMemoryPool) {
            return (QueryMemoryPool) parent;
        }
        return null;
    }

    public Map<String, MemoryStatisticsGroup> getMemoryStatistics() {
        return memoryStatistics;
    }

    @Override
    public MemoryPool removeChild(String childName) {
        MemoryPool childPool = super.removeChild(childName);
        this.memoryStatistics.put(childPool.getName(),
            new MemoryStatisticsGroup(childPool.getMemoryUsage(), childPool.getMaxMemoryUsage(), 0));
        return childPool;
    }
}
