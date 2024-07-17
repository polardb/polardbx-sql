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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Basic interface for consumer-operator which only receive the chunk.
 */
public interface ConsumerExecutor {

    ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

    /**
     * initialize the consumer-operator.
     */
    void openConsume();

    /**
     * receive the chunk and this function maybe invoked by multi threads.
     */
    void consumeChunk(Chunk chunk);

    /**
     * build the consume after it receives all chunks.
     */
    default void buildConsume() {

    }

    /**
     * 如果调用了openConsume，  在consumeChunk过程中报错，需要调用closeConsume
     * 这时候并没用触发到open()方法，所有也不会调用close()来释放资源
     * <p>
     * force = true , 用于在没有openConsume()的情况下，close也会释放consume的资源
     */
    default void closeConsume(boolean force) {
    }

    /**
     * this consumer can receive chunks or not.
     */
    default boolean needsInput() {
        return true;
    }

    /**
     * this consumer is finished or not.
     */
    default boolean consumeIsFinished() {
        return false;
    }

    /**
     * this consumer is blocked or not.
     */
    default ListenableFuture<?> consumeIsBlocked() {
        return NOT_BLOCKED;
    }

}
