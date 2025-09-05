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

package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Edge;
import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Vertex;
import static com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources.normalizeServerKey;

/**
 * use this class to make sure tasks are executed in right dependency order
 * <p>
 * there are 5 types of task
 * 1. executed task, stores in "executedVertexes"
 * 2. executing task, stores in "executingVertexes"
 * 3. can be but haven't be executed task, stores in "zeroInDegreeVertexes"
 * 4. can't be executed yet the task,  stores in "nonZeroInDegreeVertexes"
 * 5. failed tasks
 * <p>
 * 有5种类型的task
 * 1. 已执行的task，存储在executedVertexes
 * 2. 执行中的task，存储在executingVertexes
 * 3. 可执行(无依赖)但未执行的task，存储在zeroInDegreeVertexes
 * 4. 不可执行(有依赖)的task，存储在nonZeroInDegreeVertexes
 * 5. 执行失败的任务
 *
 * @author guxu
 */
public class TaskScheduler extends AbstractLifecycle {

    final List<Vertex> failedVertexes = new ArrayList<>();
    final List<Vertex> executedVertexes = new ArrayList<>();
    final List<Vertex> executingVertexes = new ArrayList<>();
    final List<Vertex> zeroInDegreeVertexes = new ArrayList<>();
    final List<Vertex> nonZeroInDegreeVertexes = new ArrayList<>();

    private final DirectedAcyclicGraph daGraph;
    private final int count;

    public enum ScheduleStatus {
        SCHEDULED,
        RUNNABLE,
        CANDIDATE,
        WAITING
    }

    public static DdlEngineResources resourceToAllocate = new DdlEngineResources();
    public static Map<String, Integer> runningRemoteTaskNums = new ConcurrentHashMap<>();
    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    public static DdlEngineResources getResourcesToAllocate() {
        return resourceToAllocate;
    }

    private TaskScheduler(DirectedAcyclicGraph daGraph) {
        this.count = daGraph.vertexCount();
        this.daGraph = daGraph;
    }

    public static TaskScheduler create(DirectedAcyclicGraph graph) {
        synchronized (graph) {
            TaskScheduler taskScheduler = new TaskScheduler(graph.clone());
            taskScheduler.init();
            return taskScheduler;
        }
    }

    @Override
    public void doInit() {
        synchronized (daGraph) {
            super.doInit();
            for (Vertex vertex : daGraph.getVertexes()) {
                if (vertex.inDegree == 0) {
                    zeroInDegreeVertexes.add(vertex);
                } else {
                    nonZeroInDegreeVertexes.add(vertex);
                }
            }
        }
    }

    public int taskCount() {
        return count;
    }

    public boolean isAllTaskDone() {
        synchronized (daGraph) {
            return CollectionUtils.isEmpty(failedVertexes)
                && CollectionUtils.isEmpty(executingVertexes)
                && CollectionUtils.isEmpty(zeroInDegreeVertexes)
                && CollectionUtils.isEmpty(nonZeroInDegreeVertexes)
                && count == executedVertexes.size();
        }
    }

    public boolean hasFailedTask() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(failedVertexes);
        }
    }

    public boolean hasExecutingTask() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(executingVertexes);
        }
    }

    public String getFailedTaskNames() {
        synchronized (daGraph) {
            try {
                return Joiner.on(",")
                    .join(failedVertexes.stream().map(e -> e.object.getName()).collect(Collectors.toList()));
            } catch (Exception e) {
                return "";
            }
        }
    }

    public String getExecutingTaskNames() {
        synchronized (daGraph) {
            try {
                return Joiner.on(",")
                    .join(executingVertexes.stream().map(e -> e.object.getName()).collect(Collectors.toList()));
            } catch (Exception e) {
                return "";
            }
        }
    }

    public boolean hasMoreExecutable() {
        synchronized (daGraph) {
            return CollectionUtils.isNotEmpty(zeroInDegreeVertexes);
        }
    }

    public DdlTask poll() {
        synchronized (daGraph) {
            if (CollectionUtils.isEmpty(zeroInDegreeVertexes)) {
                return null;
            }
            Vertex vertex = zeroInDegreeVertexes.remove(0);
            executingVertexes.add(vertex);
            return vertex.object;
        }
    }

    public void releaseResource(DdlTask ddlTask) {
        if (ddlTask == null) {
            return;
        }
        synchronized (daGraph) {
            DdlEngineResources resourcesAcquired = ddlTask.getResourceAcquired();
            synchronized (resourceToAllocate) {
                resourceToAllocate.free(resourcesAcquired);
                if (ddlTask instanceof RemoteExecutableDdlTask) {
                    String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                    runningRemoteTaskNums.put(serverKey,
                        runningRemoteTaskNums.getOrDefault(serverKey, 1) - 1);
                }
            }
        }
    }

    public DdlTask pollByResource() {
        synchronized (daGraph) {
            // which can be scheduled now.
            if (CollectionUtils.isEmpty(zeroInDegreeVertexes)) {
                return null;
            }
            for (Vertex vertex : zeroInDegreeVertexes) {
                DdlEngineResources resourcesAcquired = vertex.object.getResourceAcquired();

                synchronized (resourceToAllocate) {
                    String logInfo;
                    if (vertex.object instanceof RemoteExecutableDdlTask) {
                        String serverKey =
                            ((RemoteExecutableDdlTask) vertex.object).detectServerFromCandidate(runningRemoteTaskNums);
                        resourcesAcquired.setServerKey(serverKey);
                    }
                    if (getResourcesToAllocate().cover(resourcesAcquired, true)) {
//                        logInfo =
//                            String.format("ddl engine resource covered {%s}       {%s} for task", resourcesAcquired,
//                                resourceToAllocate);
//                        LOGGER.info(logInfo);
                        resourceToAllocate.allocate(resourcesAcquired);
                        if (vertex.object instanceof RemoteExecutableDdlTask) {
                            String serverKey = normalizeServerKey(resourcesAcquired.getServerKey());
                            runningRemoteTaskNums.put(serverKey,
                                runningRemoteTaskNums.getOrDefault(serverKey, 0) + 1);
                        }
                        vertex.object.setScheduled(false);
                        executingVertexes.add(vertex);
                        zeroInDegreeVertexes.remove(vertex);
                        return vertex.object;
                    }
                    Long taskId = vertex.object.getTaskId();
//                    if (!DdlEngineResources.markNotCoverredBefore(taskId)) {
//                        logInfo = String.format("ddl engine resource covered not for task {%s}: {%s}",
//                            vertex.object.executionInfo(),
//                            DdlEngineResources.digestCoverInfo(resourcesAcquired, resourceToAllocate));
//                        LOGGER.info(logInfo);
//                    }
                }
            }
            // we need to upgrade task order everytime we update executing task, not always in this order.
            // so we will try to do this soon.
        }
        return null;
    }

    public List<DdlTask> pollBatch() {
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasMoreExecutable()) {
                result.add(poll());
            }
            return result;
        }
    }

    public List<DdlTask> pollBatchByResource() {
        final int PULL_BATCH_SIZE = 3;
        int taskCount = 0;
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasMoreExecutable() && taskCount <= PULL_BATCH_SIZE) {
                DdlTask ddlTask = pollByResource();
                result.add(ddlTask);
                taskCount++;
            }
            return result;
        }
    }

    public Map<ScheduleStatus, Set<DdlTask>> queryActiveTasks() {
        // There would be a spot to call so many stream and HashMap.get, however, it's called by hand, we would tolerate
        // this performance decrease.
        synchronized (daGraph) {
            Set<DdlTask> runnableTasks = executingVertexes.stream().map(o -> o.object).collect(Collectors.toSet());
            Set<DdlTask> candidateTasks = zeroInDegreeVertexes.stream().map(o -> o.object).collect(Collectors.toSet());
            candidateTasks.removeAll(runnableTasks);
            Set<DdlTask> scheduledTasks =
                runnableTasks.stream().filter(o -> o.getScheduled()).collect(Collectors.toSet());
            runnableTasks.removeAll(scheduledTasks);
            Map<DdlTask, Integer> waitingTaskMap = new HashMap<>();
            for (Vertex executingVertex : executingVertexes) {
                List<Edge> outgoingEdges = executingVertex.outgoingEdges;
                for (Edge edge : outgoingEdges) {
                    DdlTask waitingTask = edge.target.object;
                    if (waitingTaskMap.containsKey(waitingTask)) {
                        waitingTaskMap.put(waitingTask, waitingTaskMap.get(waitingTask) - 1);
                    } else {
                        waitingTaskMap.put(waitingTask, edge.target.inDegree - 1);
                    }
                }
            }
            Set<DdlTask> waitingTasks =
                waitingTaskMap.keySet().stream().filter(o -> waitingTaskMap.get(o) == 0).collect(
                    Collectors.toSet());
            Map<ScheduleStatus, Set<DdlTask>> results = new HashMap<>();
            results.put(ScheduleStatus.SCHEDULED, scheduledTasks);
            results.put(ScheduleStatus.RUNNABLE, runnableTasks);
            results.put(ScheduleStatus.CANDIDATE, candidateTasks);
            results.put(ScheduleStatus.WAITING, waitingTasks);
            return results;
        }
    }

    public void markAsDone(DdlTask task) {
        synchronized (daGraph) {
            Optional<Vertex> vertexOptional =
                executingVertexes
                    .stream()
                    .filter(v -> v.object != null && v.object.getTaskId() != null)
                    .filter(v -> v.object.getTaskId().equals(task.getTaskId()))
                    .findAny();
            if (!vertexOptional.isPresent()) {
                return;
            }
            Vertex vertex = vertexOptional.get();
            executingVertexes.remove(vertex);
            executedVertexes.add(vertex);
            for (Edge edge : vertex.outgoingEdges) {
                if (--edge.target.inDegree == 0) {
                    zeroInDegreeVertexes.add(edge.target);
                    nonZeroInDegreeVertexes.remove(edge.target);
                }
            }
        }
    }

    public void markAsFail(DdlTask task) {
        synchronized (daGraph) {
            Optional<Vertex> vertexOptional =
                executingVertexes
                    .stream()
                    .filter(v -> v.object != null && v.object.getTaskId() != null)
                    .filter(v -> v.object.getTaskId().equals(task.getTaskId()))
                    .findAny();
            if (!vertexOptional.isPresent()) {
                return;
            }
            Vertex vertex = vertexOptional.get();
            executingVertexes.remove(vertex);
            failedVertexes.add(vertex);
            for (Edge edge : vertex.outgoingEdges) {
                if (--edge.target.inDegree == 0) {
                    zeroInDegreeVertexes.add(edge.target);
                    nonZeroInDegreeVertexes.remove(edge.target);
                }
            }
        }
    }

}
