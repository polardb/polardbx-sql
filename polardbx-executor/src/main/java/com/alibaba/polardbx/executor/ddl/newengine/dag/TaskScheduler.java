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

import com.google.common.base.Joiner;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Edge;
import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Vertex;

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

    public List<DdlTask> pollBatch() {
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasMoreExecutable()) {
                result.add(poll());
            }
            return result;
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
