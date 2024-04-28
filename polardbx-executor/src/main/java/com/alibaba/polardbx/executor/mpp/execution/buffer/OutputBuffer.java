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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;

public interface OutputBuffer {
    /**
     * Gets the current state of this buffer.  This method is guaranteed to not block or acquire
     * contended locks, but the stats in the info object may be internally inconsistent.
     */
    OutputBufferInfo getInfo();

    /**
     * A buffer is finished once no-more-pages has been set and all buffers have been closed
     * with an abort call.
     */
    boolean isFinished();

    /**
     * Get the memory utilization percentage.
     */
    double getUtilization();

    /**
     * Add a listener which fires anytime the buffer state changes.
     */
    void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener);

    /**
     * Updates the buffer configuration.
     */
    void setOutputBuffers(OutputBuffers newOutputBuffers, ExecutionContext context);

    /**
     * Gets pages from the output buffer, and acknowledges all pages received from the last
     * request.  The initial token is zero. Subsequent tokens are acquired from the
     * next token field in the BufferResult returned from the previous request.
     * If the buffer result is marked as complete, the client must call abort to acknowledge
     * receipt of the final state.
     */
    ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, boolean preferLocal, long token,
                                       DataSize maxSize);

    /**
     * Closes the specified output buffer.
     */
    void abort(OutputBuffers.OutputBufferId bufferId);

    /**
     * Adds a split-up page to an unpartitioned buffer. If no-more-pages has been set, the enqueue
     * page call is ignored.  This can happen with limit queries.
     */
    ListenableFuture<?> enqueue(List<SerializedChunk> page);

    /**
     * Adds a split-up page to a specific partition.  If no-more-pages has been set, the enqueue
     * page call is ignored.  This can happen with limit queries.
     */
    ListenableFuture<?> enqueue(int partition, List<SerializedChunk> page);

    default ClientBuffer getClientBuffer(int partition) {
        return null;
    }

    /**
     * Notify buffer that no more pages will be added. Any future calls to enqueue a
     * page are ignored.
     */
    void setNoMorePages();

    /**
     * Destroys the buffer, discarding all pages.
     */
    void destroy();

    /**
     * Fail the buffer, discarding all pages, but blocking readers.  It is expected that
     * readers will be unblocked when the failed query is cleaned up.
     * but because system pool memory leak
     * when task abort, replace fail. when task abort, we use doRemoveTask to remove sqlTask,
     * OutputBuffer can not be destroy by HttpPageBufferClient send delete . so system pool
     * will have a memory leak.
     * abortFail will fail the buffer, discarding all pages.
     */
    void fail();

    void checkFlushComplete();
}
