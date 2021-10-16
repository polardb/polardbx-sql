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

import io.airlift.units.DataSize;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class WriteQueue {
    private final long maxPendingBytes = new DataSize(32, DataSize.Unit.MEGABYTE).toBytes();
    private final LinkedBlockingQueue<IORequest.WriteIORequest> requestsQueue;
    private final AtomicLong pendingBytes = new AtomicLong(0);

    public WriteQueue() {
        this.requestsQueue = new LinkedBlockingQueue<>();
    }

    public boolean addRequest(IORequest.WriteIORequest request, long bytes) {
        boolean needBlock = false;
        if (bytes > 0) {
            long pBytes = pendingBytes.addAndGet(bytes);
            if (pBytes > maxPendingBytes) {
                needBlock = true;
            }
        }
        requestsQueue.offer(request);
        return needBlock;
    }

    public void requestDone(IORequest.WriteIORequest request, IOException e) {
        boolean needNotify = true;
        if (request.getBytes() > 0) {
            long pBytesBefore = pendingBytes.get();
            long pBytes = pendingBytes.addAndGet(-request.getBytes());
            if (pBytesBefore <= maxPendingBytes || pBytesBefore > maxPendingBytes && pBytes > maxPendingBytes) {
                needNotify = false;
            }
        }
        request.requestDone(e, needNotify);
    }

    public long getPendingBytes() {
        return pendingBytes.get();
    }

    public IORequest.WriteIORequest take()
        throws InterruptedException {
        return requestsQueue.take();
    }
}
