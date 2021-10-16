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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.optimizer.memory.AdaptiveMemoryHandler;
import com.alibaba.polardbx.optimizer.memory.AdaptiveMemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;

public class AdaptiveMemoryHandlerImpl implements AdaptiveMemoryHandler {

    protected MemoryKiller memoryKiller;

    public AdaptiveMemoryHandlerImpl(MemoryKiller memoryKiller) {
        this.memoryKiller = memoryKiller;
    }

    @Override
    public void revokeReleaseMemory() {

//        MemoryRevokingScheduler memoryRevokingScheduler =
//            ServiceProvider.getInstance().getServer().getMemoryRevokingScheduler();
//        if (memoryRevokingScheduler != null) {
//            AdaptiveMemoryPool apMemoryPool = MemoryManager.getInstance().getApMemoryPool();
//            long currentUsage = apMemoryPool.getMemoryUsage();
//            if (currentUsage > apMemoryPool.getMinLimit()) {
//                long releaseMemory = (currentUsage - apMemoryPool.getMinLimit()) / 2;
//                memoryRevokingScheduler.releaseMemory(releaseMemory);
//            }
//        }
    }

    @Override
    public void killApQuery() {
        if (memoryKiller != null && MemorySetting.ENABLE_KILL) {
            AdaptiveMemoryPool apMemoryPool = MemoryManager.getInstance().getApMemoryPool();
            long currentUsage = apMemoryPool.getMemoryUsage();
            if (currentUsage > apMemoryPool.getMinLimit()) {
                long releaseMemory = (currentUsage - apMemoryPool.getMinLimit()) / 2;
                memoryKiller.killMemory(releaseMemory);
            }
        }
    }

    @Override
    public void limitTpRate() {
        //TODO
    }

    @Override
    public void limitApRate() {
        //TODO
//        MemoryManager.getInstance().getApMemoryPool().initApTokens();
    }
}
