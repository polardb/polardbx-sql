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

import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
import org.apache.commons.collections.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ExecutableDdlJob will store in MetaDB, and executed by leader CN
 */
public class ExecutableDdlJob extends AbstractDdlJob {

    private transient final Map<String, DdlTask> labeledTasks = new ConcurrentHashMap<>();

    public DdlJob combineTasks(ExecutableDdlJob ddlJob) {
        synchronized (taskGraph) {
            taskGraph.addGraph(ddlJob.taskGraph);
            this.addExcludeResources(ddlJob.getExcludeResources());
        }
        return this;
    }

    /**
     * A convenient method to append to the DAG tail
     *
     * @param job the new job
     * @return job self
     */
    @Deprecated
    public DdlJob appendJob(ExecutableDdlJob job) {
        if (job.isEmpty()) {
            return this;
        }
        // combine tasks
        if (taskGraph.isEmpty()) {
            assert labeledTasks.isEmpty();
            taskGraph.addGraph(job.taskGraph);
            if (job.getHead() != null) {
                labelAsHead(job.getHead());
            }
        } else {
            combineTasks(job);
            if (getTail() != null && job.getHead() != null) {
                addTaskRelationship(getTail(), job.getHead());
            }
        }
        if (job.getTail() != null) {
            labelAsTail(job.getTail());
        }

        // combine resources
        this.excludeResources.addAll(job.getExcludeResources());
        return this;
    }

    /**
     * Append a job after the predecessor node
     */
    @Deprecated
    public DdlJob appendJobAfter(DdlTask predecessor, ExecutableDdlJob job) {
        if (job.isEmpty()) {
            return this;
        }

        if (taskGraph.isEmpty()) {
            taskGraph.addGraph(job.taskGraph);
        } else {
            combineTasks(job);
        }
        addTaskRelationship(predecessor, job.getHead());

        this.excludeResources.addAll(job.getExcludeResources());
        return this;
    }


    /**
     * A convenient method to append to the DAG tail
     *
     * @param job the new job
     * @return job self
     */
    public DdlJob appendJob2(ExecutableDdlJob job) {
        if (job == null || job.isEmpty()) {
            return this;
        }
        taskGraph.appendGraph(job.taskGraph);

        // combine resources
        this.excludeResources.addAll(job.getExcludeResources());
        return this;
    }

    /**
     * Append a job after the predecessor node
     */
    public DdlJob appendJobAfter2(@NotNull DdlTask predecessor, ExecutableDdlJob job) {
        if (job == null || job.isEmpty()) {
            return this;
        }

        taskGraph.appendGraphAfter(taskGraph.findVertex(predecessor), job.taskGraph);

        // combine resources
        this.excludeResources.addAll(job.getExcludeResources());
        return this;
    }

    public DdlJob setExceptionActionForAllTasks(DdlExceptionAction action) {
        synchronized (taskGraph) {
            Set<DirectedAcyclicGraph.Vertex> vertexSet = taskGraph.getVertexes();
            if (CollectionUtils.isEmpty(vertexSet)) {
                return this;
            }
            for (DirectedAcyclicGraph.Vertex vertex : vertexSet) {
                vertex.getObject().setExceptionAction(action);
            }
        }
        return this;
    }

    public DdlJob setExceptionActionForAllSuccessor(DdlTask ddlTask, DdlExceptionAction action) {
        List<DdlTask> successorTasks = taskGraph.getTransitiveSuccessors(ddlTask);
        if (CollectionUtils.isEmpty(successorTasks)) {
            return this;
        }
        for (DdlTask t : successorTasks) {
            t.setExceptionAction(action);
        }
        return this;
    }

    @Deprecated
    public void labelTask(String label, DdlTask task) {
        labeledTasks.put(label, task);
    }

    @Deprecated
    public DdlTask getTaskByLabel(String label) {
        return labeledTasks.get(label);
    }

    //used for connect DAGs in different DDL JOBs

    public static final String HEAD = "head";
    public static final String TAIL = "tail";
    public static final String VALIDATE = "validate";
    public static final String BACKFILL = "backfill";

    @Deprecated
    public void labelAsHead(DdlTask task) {
        labelTask(HEAD, task);
    }

    @Deprecated
    public DdlTask getHead() {
        return getTaskByLabel(HEAD);
    }

    @Deprecated
    public void labelAsTail(DdlTask task) {
        labelTask(TAIL, task);
    }

    @Deprecated
    public DdlTask getTail() {
        return getTaskByLabel(TAIL);
    }

    public DdlJob overrideTasks(ExecutableDdlJob ddlJob) {
        synchronized (taskGraph) {
            taskGraph.clear();
            taskGraph.addGraph(ddlJob.taskGraph);
        }
        return this;
    }

    public boolean isEmpty() {
        return this.taskGraph.isEmpty();
    }

}
