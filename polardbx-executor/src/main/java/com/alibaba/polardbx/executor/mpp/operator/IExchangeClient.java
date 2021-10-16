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

package com.alibaba.polardbx.executor.mpp.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.List;

public interface IExchangeClient extends Closeable {

    void addLocation(TaskLocation taskLocation);

    void addLocations(List<TaskLocation> taskLocations);

    void noMoreLocations();

    boolean isFinished();

    ListenableFuture<?> isBlocked();

    boolean isClosed();

    boolean isNoMoreLocations();

    SerializedChunk pollPage();

    SerializedChunk getNextPageForDagWithDataDivide(Duration maxWaitTime) throws InterruptedException;
}