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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;

public abstract class BlockingMemoryPool extends MemoryPool {

    protected static final Logger logger = LoggerFactory.getLogger(BlockingMemoryPool.class);

    @GuardedBy("this")
    private long tryMinRequestSize = 0;

    @GuardedBy("this")
    private long minRequestSize = 0;

    @GuardedBy("this")
    private SettableFuture<?> settableFuture;

    @GuardedBy("this")
    private SettableFuture<?> trySettableFuture;

    @GuardedBy("this")
    private boolean needMemoryRevoking = false;

    protected long maxElasticMemory;

    protected boolean blockFlag;

    public BlockingMemoryPool(String name, long maxLimit, MemoryPool parent, MemoryType memoryType) {
        super(name, maxLimit, parent, memoryType);
        setMaxElasticMemory(MppConfig.getInstance().getMemoryRevokingThreshold());
    }

    public void setMaxElasticMemory(double maxElasticThreshold) {
        this.maxElasticMemory = (long) (this.maxLimit * maxElasticThreshold);
    }

    @Override
    public void setMaxLimit(long memoryLimit) {
        this.maxLimit = memoryLimit;
        setMaxElasticMemory(MppConfig.getInstance().getMemoryRevokingThreshold());
    }

    @Override
    protected ListenableFuture<?> block(ListenableFuture<?> parent, long size) {
        if (parent != null && !parent.isDone()) {
            if (inheritParentFuture()) {
                return parent;
            } else {
                return NOT_BLOCKED;
            }
        } else {
            if (reservedBytes + revocableBytes > maxElasticMemory && MemorySetting.ENABLE_SPILL) {
                logger.info("The query use much more memory for the memory pool: " + name);
                if (minRequestSize <= 0 || minRequestSize > size) {
                    minRequestSize = size;
                }
                if (revocableBytes >= minRequestSize) {
                    //存在可释放的内存的时候，才阻塞
                    //return the blocked future after the allocated memory exceed the maxElasticMemory.
                    if (settableFuture == null || settableFuture.isDone()) {
                        settableFuture = SettableFuture.create();
                        this.blockFlag = true;
                    }
                    checkState(!settableFuture.isDone(), "future is already completed");
                    this.needMemoryRevoking = true;
                    requestMemoryRevoke();
                    return settableFuture;
                } else {
                    // Don't blocked the current query if revocableBytes is less than the minRequestSize.
                    return NOT_BLOCKED;
                }
            } else {
                return NOT_BLOCKED;
            }
        }
    }

    @Override
    protected void tryBlock(MemoryAllocateFuture allocFuture, long size, boolean reserved) {
        if (size > maxElasticMemory) {
            outOfMemory(fullName, getMemoryUsage(), size, maxElasticMemory, reserved);
        }

        if (tryMinRequestSize <= 0 || tryMinRequestSize > size) {
            tryMinRequestSize = size;
        }

        if ((revocableBytes >= tryMinRequestSize || reserved) && MemorySetting.ENABLE_SPILL) {
            logger.info("The query use much more memory for the memory pool: " + name);
            //存在可释放的内存的时候，才阻塞
            if (trySettableFuture == null || trySettableFuture.isDone()) {
                trySettableFuture = SettableFuture.create();
                this.blockFlag = true;
            }
            allocFuture.setAllocateFuture(trySettableFuture);
            this.needMemoryRevoking = true;
            requestMemoryRevoke();
        } else {
            //FIXME here may exist that much hybrid-hash-join try to allocate memory at the same time!
            outOfMemory(fullName, getMemoryUsage(), size, maxElasticMemory, reserved);
        }
    }

    @Override
    protected synchronized void notifyBlockedQuery() {
        if (settableFuture == null && trySettableFuture == null) {
            return;
        }

        long availableBytes = maxElasticMemory - reservedBytes - revocableBytes;
        if (availableBytes >= minRequestSize && settableFuture != null && !settableFuture.isDone()) {
            minRequestSize = 0;
            settableFuture.set(null);
        }
        if (availableBytes >= tryMinRequestSize && trySettableFuture != null && !trySettableFuture.isDone()) {
            tryMinRequestSize = 0;
            trySettableFuture.set(null);
        }
    }

    @Override
    protected long getTryMaxLimit() {
        return maxElasticMemory;
    }

    protected boolean inheritParentFuture() {
        return true;
    }

    @Override
    public void destroy() {
        if (settableFuture != null) {
            settableFuture.set(null);
        }

        if (trySettableFuture != null) {
            trySettableFuture.set(null);
        }
        super.destroy();
    }

    public synchronized boolean isNeedMemoryRevoking() {
        return needMemoryRevoking;
    }

    public synchronized void resetNeedMemoryRevoking() {
        this.needMemoryRevoking = false;
    }

    public ListenableFuture<?> getBlockedFuture() {
        return settableFuture;
    }

    public SettableFuture<?> getTrySettableFuture() {
        return trySettableFuture;
    }

    public long getMinRequestMemory() {
        return tryMinRequestSize + minRequestSize;
    }

    public boolean isBlockFlag() {
        return blockFlag;
    }
}
