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

package com.alibaba.polardbx.executor.operator.scan;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface IOStatus<BATCH> {
    /**
     * The unique identifier of the scan work.
     */
    String workId();

    ScanState state();

    ListenableFuture<?> isBlocked();

    void addResult(BATCH batch);

    void addResults(List<BATCH> batches);

    BATCH popResult();

    void addException(Throwable t);

    void throwIfFailed();

    void finish();

    void close();

    long rowCount();
}
