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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ReadQueue implements IORequest.OnRead {

    private final long maxPendingBytes;
    private final LinkedBlockingQueue<IORequest.ReadIORequest> requetsQueue;
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private boolean blocked = false;

    public ReadQueue() {
        this.maxPendingBytes = new DataSize(32, DataSize.Unit.MEGABYTE).toBytes();
        this.requetsQueue = new LinkedBlockingQueue<>();
    }

    public void addRequest(IORequest.ReadIORequest request) {
        this.requetsQueue.add(request);
    }

    public IORequest.ReadIORequest take()
        throws InterruptedException {
        return requetsQueue.take();
    }

    @VisibleForTesting
    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    @Override
    public boolean onReady(long bytes) {
        long pBytes = pendingBytes.addAndGet(bytes);
        return pBytes > maxPendingBytes || blocked;
    }

    @Override
    public boolean onRead(long bytes) {
        long pBytes = pendingBytes.addAndGet(-bytes);
        return pBytes > maxPendingBytes || blocked;
    }

    public long getPendingBytes() {
        return this.pendingBytes.get();
    }
}
