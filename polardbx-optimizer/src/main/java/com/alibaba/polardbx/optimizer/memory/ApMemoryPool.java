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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.concurrent.Semaphore;

public class ApMemoryPool extends AdaptiveMemoryPool {

    protected static final Logger logger = LoggerFactory.getLogger(ApMemoryPool.class);

    private Semaphore apSemaphore = null;

    private Long overloadTime = null;

    public ApMemoryPool(String name, long minLimit, long maxLimit, MemoryPool parent) {
        super(name, parent, MemoryType.GENERAL_AP, minLimit, maxLimit);
    }

    public Semaphore getApSemaphore() {
        return apSemaphore;
    }

    public void setApSemaphore(Semaphore apSemaphore) {
        this.apSemaphore = apSemaphore;
    }

    public Long getOverloadTime() {
        return overloadTime;
    }

}
