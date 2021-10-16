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
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.gms.node.Node;

import java.util.concurrent.ExecutorService;

public interface RemoteTaskFactory {

    RemoteTask createRemoteTask(Session session,
                                TaskId taskId,
                                Node node,
                                PlanFragment fragment,
                                boolean noMoreSplits,
                                OutputBuffers outputBuffers,
                                NodeTaskMap.PartitionedSplitCountTracker partitionedSplitCountTracker,
                                boolean summarizeTaskInfo,
                                Multimap<Integer, Split> splits,
                                SqlStageExecution sqlStageExecution);

    TaskLocation createLocation(Node node, TaskId taskId);

    ExecutorService getExecutor();
}
