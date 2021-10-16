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

package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;

public interface MemoryRevoker {

    /**
     * After calling this method operator should revoke all reserved revocable memory.
     * As soon as memory is revoked returned future should be marked as done.
     * <p>
     * Spawned threads can not modify OperatorContext because it's not thread safe.
     * For this purpose use implement finishMemoryRevoke
     * <p>
     * After startMemoryRevoke is called on Operator the Driver is disallowed to call any
     * processing methods on it (finish/isFinished/isBlocked/needsInput/addInput/getOutput) until
     * finishMemoryRevoke is called.
     */
    ListenableFuture<?> startMemoryRevoke();

    /**
     * Clean up and release resources after completed memory revoking. Called by driver
     * once future returned by startMemoryRevoke is completed.
     */
    void finishMemoryRevoke();

    OperatorMemoryAllocatorCtx getMemoryAllocatorCtx();
}
