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
package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.metadata.Split;

public interface RemoteTask {
    TaskId getTaskId();

    String getNodeId();

    TaskInfo getTaskInfo();

    TaskStatus getTaskStatus();

    void noMoreSplits(Integer sourceId);

    void start();

    void addSplits(Multimap<Integer, Split> splits, boolean isNoMoreSplits);

    void setOutputBuffers(OutputBuffers outputBuffers);

    void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener);

    ListenableFuture<?> whenSplitQueueHasSpace(int threshold);

    void cancel();

    void abort();

    boolean isStarted();

    int getPartitionedSplitCount();

    int getQueuedPartitionedSplitCount();
}
