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

package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

public interface SplitRunner extends Closeable {
    boolean isFinished();

    ListenableFuture<?> processFor(long duration, long start) throws Exception;

    String getInfo();

    @Override
    void close();

    void recordBlocked();

    void recordBlockedFinished();

    void buildMDC();

    /**
     * @return true - 需要作为低优先级执行
     */
    boolean moveLowPrioritizedQuery(long executeTime);
}
