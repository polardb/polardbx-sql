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

import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

public class QueryMemoryPool extends BlockingMemoryPool {

    protected static final Logger logger = LoggerFactory.getLogger(QueryMemoryPool.class);

    protected MemoryPool planMemPool;

    public QueryMemoryPool(String name, long limit, MemoryPool parent) {
        super(name, limit, parent, MemoryType.QUERY);
        this.planMemPool = this.getOrCreatePool("planner", limit, MemoryType.PLANER);
    }

    @Override
    protected boolean inheritParentFuture() {
        //不阻塞小查询
        return getMemoryUsage() > MppConfig.getInstance().getLessRevokeBytes();
    }

    public MemoryPool getPlanMemPool() {
        return planMemPool;
    }

    @Override
    protected void outOfMemory(String memoryPool, long usage, long allocating, long limit, Boolean reserved) {
        logger.warn("Current Query MemoryPool: " + this.printDetailInfo(0));
        throw new MemoryNotEnoughException(memoryPool, usage, allocating, limit, reserved);
    }
}
