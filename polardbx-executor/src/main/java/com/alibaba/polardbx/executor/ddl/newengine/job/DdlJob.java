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

import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;

import java.util.List;
import java.util.Set;

public interface DdlJob {

    /**
     * Add a DDL task into this job.
     *
     * @param task A new DDL task
     * @return Current job
     */
    DdlJob addTask(DdlTask task);

    DdlTask getTaskById(long taskId);

    /**
     * Add a relationship between a source DDL task and a target DDL task into this job.
     * For example, task1 -> task2.
     *
     * @param source A new/existing DDL task as source
     * @param target A new/existing DDL task as target
     * @return Current job
     */
    DdlJob addTaskRelationship(DdlTask source, DdlTask target);

    DdlJob removeTaskRelationship(DdlTask source, DdlTask target);

    DdlJob removeRedundancyRelations();

    /**
     * getMaxParallelism. default 1
     */
    int getMaxParallelism();

    /**
     * setMaxParallelism
     */
    void setMaxParallelism(int maxParallelism);

    /**
     * Add a relationship between a source DDL task and a target DDL task into this job.
     * For example, task1 -> task2.
     */
    List<DdlTask> getTasksByType(Class<DdlTask> sourceTaskType);

    List<DdlTask> getAllTasks();

    /**
     * Append a series of sequential tasks to a predecessor task
     * <p>
     * For example:
     * addSequentialTasksAfter(A, Lists.newArrayList(B,C,D));
     * <p>
     * The corresponding DAG:
     * A -> B -> C -> D
     *
     * @param predecessor A specified predecessor task
     * @param taskList Sequential tasks appended
     * @return Current job
     */
    DdlJob addSequentialTasksAfter(DdlTask predecessor, List<DdlTask> taskList);

    /**
     * Add all tasks sequentially.
     *
     * @param taskList Sequential tasks
     * @return Current job
     */
    DdlJob addSequentialTasks(List<DdlTask> taskList);

    /**
     * Append a set of concurrent tasks to a predecessor task
     * <p>
     * For example:
     * addConcurrentTasksAfter(A, Lists.newArrayList(B,C,D));
     * <p>
     * The corresponding DAG:
     * A -> B
     * |-> C
     * |-> D
     *
     * @param predecessor A specified predecessor task
     * @param taskList Concurrent tasks appended
     * @return Current job
     */
    DdlJob addConcurrentTasksAfter(DdlTask predecessor, List<DdlTask> taskList);

    /**
     * Add all tasks concurrently
     *
     * @param taskList Concurrent tasks
     * @return Current job
     */
    DdlJob addConcurrentTasks(List<DdlTask> taskList);

    /**
     * Check if the job is valid.
     *
     * @return valid or not
     */
    boolean isValid();

    /**
     * create a new taskScheduler everytime
     *
     * @return TaskScheduler
     */
    TaskScheduler createTaskScheduler();

    /**
     * for rollback
     * create a new reversed taskScheduler everytime
     *
     * @return TaskScheduler
     */
    TaskScheduler createReversedTaskScheduler();

    /**
     * Get all resource names that the DDL job depends on, typically table names.
     *
     * @return A set of resource names
     */
    Set<String> getExcludeResources();

    /**
     * add resources to ddl job
     */
    void addExcludeResources(Set<String> resources);

    /**
     * Get all shared resource names that the DDL job depends on, typically table names.
     *
     * @return A set of resource names
     */
    Set<String> getSharedResources();

    /**
     * add shared resources to ddl job
     */
    void addSharedResources(Set<String> resources);

    /**
     * Visualize the tasks and the dependency among them as a DAG.
     *
     * @return A string representation that can be shown as a DAG
     */
    String visualizeTasks();

    /**
     * serialize the task DAG to an adjacency list
     */
    String serializeTasks();

    int getTaskCount();

    List<String> getExplainInfo();

}
