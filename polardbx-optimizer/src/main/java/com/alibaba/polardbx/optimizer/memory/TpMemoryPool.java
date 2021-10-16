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

public class TpMemoryPool extends AdaptiveMemoryPool {

    public TpMemoryPool(String name, long minLimit, long maxLimit, MemoryPool parent) {
        super(name, parent, MemoryType.GENERAL_AP, minLimit, maxLimit);
    }

    @Override
    protected void onMemoryReserved(long mayTotalUsage, boolean allocateSuccessOfParent) {

        if (!allocateSuccessOfParent) {
            if (mayTotalUsage < minLimit) {
                //TP处于低水位，但依然申请失败，强制杀掉部分AP,直接释放出内存的差值 minLimit - currentTotalUsage
                //同时触发内存释放
                //TODO
            } else if (mayTotalUsage > maxLimit) {
                //TP此时处于高水位水, 没办法必须对TP限流，同时可以启动限流机制
                //触发TP查询最内存释放
                //TODO
            } else {
                //TP此时处于合理水位， 优先触发AP查询做内存释放，再触发TP查询做内存释放, 同时可以启动限流机制
                //TODO
            }
        } else {
            if (mayTotalUsage > maxLimit) {
                //虽然申请成功了，但是TP超过了最大阈值,  考虑对TP限流, 就不触发内存释放了，考虑到spill太浪费时间了
                adaptiveMemoryHandler.limitApRate();
            }
        }
    }
}
