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
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.google.common.base.Preconditions;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public abstract class AbstractDdlJob implements DdlJob {

    protected final DirectedAcyclicGraph taskGraph;
    protected final Set<String> excludeResources = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    protected final Set<String> sharedResources = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    protected int maxParallelism = 1;

    protected String ddlJobFactoryName;

    public AbstractDdlJob() {
        taskGraph = DirectedAcyclicGraph.create();
    }

    public AbstractDdlJob(DirectedAcyclicGraph taskGraph) {
        this.taskGraph = taskGraph;
    }

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
        return !taskGraph.hasCycle();
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

    @Override
    public List<String> getExplainInfo() {
        try {
            List<String> result = new ArrayList<>();
            List<DirectedAcyclicGraph.Vertex> vertexes = taskGraph.clone().getSequentialVertexByTopologyOrder();
            if (vertexes.isEmpty()) {
                return result;
            }

            for (DirectedAcyclicGraph.Vertex vertex : vertexes) {
                DdlTask ddlTask = vertex.object;
                List<String> taskExplainInfos = ddlTask.explainInfo();
                for (String taskExplainInfo : taskExplainInfos) {
                    if (!StringUtils.isEmpty(taskExplainInfo)) {
                        result.add(taskExplainInfo);
                    }
                }
            }
            String excludeResource =
                StringUtil.join(", ", getExcludeResources().stream().collect(Collectors.toList())).toString();
            String shareResource =
                StringUtil.join(", ", getExcludeResources().stream().collect(Collectors.toList())).toString();
            if (!StringUtil.isNullOrEmpty(excludeResource)) {
                result.add(String.format("EXCLUDE_RESOURCE( %s )", excludeResource));
            }
            if (!StringUtil.isNullOrEmpty(shareResource)) {
                result.add(String.format("SHARE_RESOURCE( %s )", shareResource));
            }

            return result;
        } catch (Throwable t) {
            throw GeneralUtil.nestedException("explainTasks failed:  " + t.getMessage(), t);
        }

    }

    public void setDdlJobFactoryName(String ddlJobFactoryName) {
        this.ddlJobFactoryName = ddlJobFactoryName;
    }

    public String getDdlJobFactoryName() {
        return ddlJobFactoryName;
    }
}

