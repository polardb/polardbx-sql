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

package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TopologicalSorter;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public abstract class AbstractDdlJob implements DdlJob {

    protected final DirectedAcyclicGraph taskGraph = DirectedAcyclicGraph.create();
    protected final Set<String> excludeResources = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    protected final Set<String> sharedResources = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    protected int maxParallelism = 1;

    @Override
    public DdlJob addTask(DdlTask task) {
        Preconditions.checkNotNull(task, "DdlTask is null");
        synchronized (taskGraph) {
            taskGraph.addVertex(task);
        }
        return this;
    }

    @Override
    public DdlTask getTaskById(long taskId) {
        Set<DirectedAcyclicGraph.Vertex> vertexSet = taskGraph.getVertexes();
        if (CollectionUtils.isEmpty(vertexSet)) {
            return null;
        }
        for (DirectedAcyclicGraph.Vertex v : vertexSet) {
            if (v.object.getTaskId() == taskId) {
                return v.object;
            }
        }
        return null;
    }

    @Override
    public DdlJob addTaskRelationship(DdlTask source, DdlTask target) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);

        synchronized (taskGraph) {
            taskGraph.addEdge(source, target);
        }
        return this;
    }

    @Override
    public DdlJob removeTaskRelationship(DdlTask source, DdlTask target) {
        synchronized (taskGraph) {
            taskGraph.removeEdge(source, target);
        }
        return this;
    }

    @Override
    public DdlJob removeRedundancyRelations() {
        synchronized (taskGraph) {
            taskGraph.removeRedundancyRelations();
        }
        return this;
    }

    @Override
    public int getMaxParallelism() {
        return this.maxParallelism;
    }

    @Override
    public void setMaxParallelism(final int maxParallelism) {
        if (maxParallelism < 1) {
            throw GeneralUtil.nestedException("max parallelism must not less than 1");
        }
        this.maxParallelism = maxParallelism;
    }

    @Override
    public List<DdlTask> getTasksByType(Class<DdlTask> taskType) {
        synchronized (taskGraph) {
            List<DdlTask> result = new ArrayList<>();
            for (DirectedAcyclicGraph.Vertex v : taskGraph.getVertexes()) {
                if (v.object.getClass().equals(taskType)) {
                    result.add(v.object);
                }
            }
            return result;
        }
    }

    @Override
    public List<DdlTask> getAllTasks() {
        synchronized (taskGraph) {
            List<DdlTask> result = new ArrayList<>();
            for (DirectedAcyclicGraph.Vertex v : taskGraph.getVertexes()) {
                result.add(v.object);
            }
            return result;
        }
    }

    @Override
    public DdlJob addSequentialTasksAfter(DdlTask predecessor, List<DdlTask> taskList) {
        if (predecessor == null || CollectionUtils.isEmpty(taskList)) {
            return this;
        }
        synchronized (taskGraph) {
            DdlTask pre = predecessor;
            for (DdlTask curTask : taskList) {
                Preconditions.checkNotNull(curTask, "DdlTask is null");
                taskGraph.addEdge(pre, curTask);
                pre = curTask;
            }
        }
        return this;
    }

    @Override
    public DdlJob addSequentialTasks(final List<DdlTask> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return this;
        }
        synchronized (taskGraph) {
            DdlTask first = taskList.get(0);
            List<DdlTask> rest = new ArrayList<>();
            for (int i = 1; i < taskList.size(); i++) {
                rest.add(taskList.get(i));
            }
            addTask(first);
            return addSequentialTasksAfter(first, rest);
        }
    }

    @Override
    public DdlJob addConcurrentTasksAfter(DdlTask predecessor, List<DdlTask> taskList) {
        if (predecessor == null || CollectionUtils.isEmpty(taskList)) {
            return this;
        }
        synchronized (taskGraph) {
            for (DdlTask curTask : taskList) {
                taskGraph.addEdge(predecessor, curTask);
            }
        }
        return this;
    }

    @Override
    public DdlJob addConcurrentTasks(List<DdlTask> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return this;
        }
        synchronized (taskGraph) {
            for (DdlTask curTask : taskList) {
                taskGraph.addVertex(curTask);
            }
        }
        return this;
    }

    @Override
    public boolean isValid() {
        return !TopologicalSorter.hasCycle(taskGraph.clone());
    }

    @Override
    public TopologicalSorter createTaskIterator() {
        return TopologicalSorter.create(taskGraph);
    }

    @Override
    public TaskScheduler createTaskScheduler() {
        return TaskScheduler.create(taskGraph);
    }

    @Override
    public TaskScheduler createReversedTaskScheduler() {
        synchronized (taskGraph) {
            DirectedAcyclicGraph reversedTaskGraph = taskGraph.clone();
            reversedTaskGraph.reverse();
            return TaskScheduler.create(reversedTaskGraph);
        }
    }

    @Override
    public Set<String> getExcludeResources() {
        return this.excludeResources;
    }

    @Override
    public void addExcludeResources(Set<String> resources) {
        if (CollectionUtils.isEmpty(resources)) {
            return;
        }
        this.excludeResources.addAll(resources);
    }

    @Override
    public Set<String> getSharedResources() {
        return this.sharedResources;
    }

    @Override
    public void addSharedResources(Set<String> resources) {
        if (CollectionUtils.isEmpty(resources)) {
            return;
        }
        this.sharedResources.addAll(resources);
    }

    @Override
    public String visualizeTasks() {
        try {
            return taskGraph.visualize();
        } catch (Throwable t) {
            throw GeneralUtil.nestedException("visualizeTasks failed:  " + t.getMessage(), t);
        }
    }

    @Override
    public String serializeTasks() {
        Map<Long, List<Long>> dag = taskGraph.toAdjacencyListFormat();
        return JSON.toJSONString(dag);
    }

    @Override
    public int getTaskCount() {
        if (taskGraph == null) {
            return 0;
        }
        return taskGraph.vertexCount();
    }
}

