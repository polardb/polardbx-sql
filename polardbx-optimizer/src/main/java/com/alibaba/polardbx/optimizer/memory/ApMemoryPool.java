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

    @Override
    protected void onMemoryReserved(long mayTotalUsage, boolean allocateSuccessOfParent) {

        if (!allocateSuccessOfParent) {
            if (mayTotalUsage < minLimit) {
                //AP处于低水位，但依然申请失败，需要对TP限流
                adaptiveMemoryHandler.limitTpRate();
            } else {
                //申请失败的原因，是因为AP使用的内存过多导致的
                //AP此时处于高水位， 触发内存释放, 同时对AP限流
                adaptiveMemoryHandler.revokeReleaseMemory();
                adaptiveMemoryHandler.killApQuery();
            }
        } else {
            //虽然申请成功了，但是AP超过了最大阈值，触发内存释放吧, 同时对AP限流
            //这里需要扩展下几个策略，1. 是否自杀 2. 是否触发spill 3. 限流。 其中1、2 分别和3 正交的
            //FIXME 考虑下是否自杀或者限流
            adaptiveMemoryHandler.limitTpRate();
//            if (revocableBytes > 0) {
//                requestMemoryRevoke(this, Math.min(mayTotalUsage - minLimit, revocableBytes));
//            }
        }
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

    public void setOverloadTime(Long overloadTime) {
        this.overloadTime = overloadTime;
    }

    public void initApTokens() {
//        if (apSemaphore == null && children.size() > 1) {
//            Semaphore semaphore = new Semaphore(children.size() / 2);
//            logger.warn("apSemaphore.availablePermits=" + apSemaphore.availablePermits());
//            for (MemoryPool pool : this.children.values()) {
//                if (apSemaphore.availablePermits() == 0) {
//                    break;
//                }
//                if (((QueryMemoryPool) pool).setSemaphore(apSemaphore)) {
//                    try {
//                        apSemaphore.acquire();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//            apSemaphore = semaphore;
//        }
    }

    public Semaphore acquireToken() throws InterruptedException {
        if (apSemaphore != null) {
            apSemaphore.acquire();
        }
        return apSemaphore;
    }

    public void releaseToken(Semaphore semaphore) {
        if (semaphore != null) {
            semaphore.release();
        }

        if (apSemaphore != null && apSemaphore != semaphore) {
            apSemaphore.release();
        }
    }
}
