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

package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;

public class OptimizerAlertLoggerPlanCacheImpl extends OptimizerAlertLoggerBaseImpl {
    public OptimizerAlertLoggerPlanCacheImpl() {
        super();
        this.optimizerAlertType = OptimizerAlertType.PLAN_CACHE_FULL;
    }

    @Override
    public void inc() {
        // do nothing
    }

    @Override
    public boolean logDetail(ExecutionContext ec) {
        if (lock.tryLock()) {
            try {
                long lastTime = lastAccessTime.get();
                long currentTime = System.currentTimeMillis();
                if (currentTime >= lastTime + DynamicConfig.getInstance().getOptimizerAlertLogInterval()) {
                    // add here
                    totalCount.increment();
                    lastAccessTime.set(currentTime);
                    logger.info(String.format("%s: current cache size { %d }", optimizerAlertType.name(),
                        PlanCache.getInstance().getCache().size()));
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }
}
