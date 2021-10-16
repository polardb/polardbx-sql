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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

public abstract class AdaptiveMemoryPool extends MemoryPool {

    protected static final Logger logger = LoggerFactory.getLogger(AdaptiveMemoryPool.class);

    protected long minLimit;

    protected AdaptiveMemoryHandler adaptiveMemoryHandler;

    public AdaptiveMemoryPool(String name, MemoryPool parent, MemoryType memoryType, long minLimit,
                              long maxLimit) {
        super(name, maxLimit, parent, memoryType);
        this.minLimit = minLimit;
        Preconditions.checkArgument(
            maxLimit >= minLimit, "HighWaterLevel shouldn't less than LowWaterLevel");
    }

    public long getMinLimit() {
        return minLimit;
    }

    public void setMinLimit(long minLimit) {
        this.minLimit = minLimit;
    }

    public AdaptiveMemoryHandler getAdaptiveMemoryHandler() {
        return adaptiveMemoryHandler;
    }

    public void setAdaptiveMemoryHandler(AdaptiveMemoryHandler adaptiveMemoryHandler) {
        this.adaptiveMemoryHandler = adaptiveMemoryHandler;
    }

    protected void onMemoryReserved(long nowTotalUsage, boolean allocateSuccessOfParent) {

    }

//    @Override
//    public ListenableFuture<?> allocateReserveMemory(long size) {
//        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
//        checkArgument(size >= 0, "bytes is negative");
//        ListenableFuture<?> future = null;
//        synchronized (this) {
//            long nowReservedBytes = reservedBytes + size;
//            long nowTotalUsage = nowReservedBytes + revocableBytes;
//            if (parent != null) {
//                try {
//                    future = parent.allocateReserveMemory(size);
//                } catch (MemoryNotEnoughException t) {
//                    onMemoryReserved(nowTotalUsage, false);
//                    throw t;
//                }
//            }
//            if (nowTotalUsage > maxLimit) {
//                onMemoryReserved(nowTotalUsage, true);
//            }
//
//            if (nowTotalUsage > maxMemoryUsage) {
//                maxMemoryUsage = nowTotalUsage;
//            }
//            reservedBytes = nowReservedBytes;
//        }
//        return future;
//    }
//
//    @Override
//    public boolean tryAllocateReserveMemory(long size, MemoryAllocateFuture allocFuture) {
//        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
//        checkArgument(size >= 0, "bytes is negative");
//        synchronized (this) {
//            long nowReservedBytes = reservedBytes + size;
//            long nowTotalUsage = nowReservedBytes + revocableBytes;
//            if (parent != null) {
//                boolean bSuccess = parent.tryAllocateReserveMemory(size, allocFuture);
//                if (!bSuccess) {
//                    onMemoryReserved(nowTotalUsage, false);
//                    return false;
//                }
//            }
//            //allocate the memory itself firstly.
//            if (nowTotalUsage > maxLimit) {
//                onMemoryReserved(nowTotalUsage, true);
//                return false;
//            }
//            if (nowTotalUsage > maxMemoryUsage) {
//                maxMemoryUsage = nowTotalUsage;
//            }
//            reservedBytes = nowReservedBytes;
//        }
//        return true;
//    }
//
//    @Override
//    public ListenableFuture<?> allocateRevocableMemory(long size) {
//        Preconditions.checkState(!destroyed.get(), fullName + " memory pool already destroyed");
//        checkArgument(size >= 0, "bytes is negative");
//        ListenableFuture<?> future = null;
//        synchronized (this) {
//            long nowRevocableBytes = revocableBytes + size;
//            long nowTotalUsage = reservedBytes + nowRevocableBytes;
//            if (parent != null) {
//                try {
//                    future = parent.allocateRevocableMemory(size);
//                } catch (MemoryNotEnoughException t) {
//                    onMemoryReserved(nowTotalUsage, false);
//                    throw t;
//                }
//            }
//            if (nowTotalUsage > maxLimit) {
//                onMemoryReserved(nowTotalUsage, true);
//            }
//            if (nowTotalUsage > maxMemoryUsage) {
//                maxMemoryUsage = nowTotalUsage;
//            }
//            reservedBytes = nowRevocableBytes;
//        }
//        return future;
//    }
}
