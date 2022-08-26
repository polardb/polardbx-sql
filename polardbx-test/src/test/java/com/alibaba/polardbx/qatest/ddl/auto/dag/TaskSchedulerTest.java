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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class TaskSchedulerTest {

    @Test
    public void test1() throws InterruptedException {
        TestEmptyTask e1 = new TestEmptyTask("e1");
        TestEmptyTask e2 = new TestEmptyTask("e2");
        TestEmptyTask e3 = new TestEmptyTask("e3");
        TestEmptyTask e4 = new TestEmptyTask("e4");
        TestEmptyTask e5 = new TestEmptyTask("e5");
        TestEmptyTask e6 = new TestEmptyTask("e6");
        TestEmptyTask e7 = new TestEmptyTask("e7");
        TestEmptyTask e8 = new TestEmptyTask("e8");
        TestEmptyTask e9 = new TestEmptyTask("e9");

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addTask(e1);
        executableDdlJob.addTask(e2);
        executableDdlJob.addTask(e3);
        executableDdlJob.addTask(e4);
        executableDdlJob.addTask(e5);
        executableDdlJob.addTask(e6);
        executableDdlJob.addTask(e7);
        executableDdlJob.addTask(e8);
        executableDdlJob.addTask(e9);

        executableDdlJob.addTaskRelationship(e1, e9);
        executableDdlJob.addTaskRelationship(e2, e9);
        executableDdlJob.addTaskRelationship(e3, e9);
        executableDdlJob.addTaskRelationship(e4, e9);
        executableDdlJob.addTaskRelationship(e5, e9);
        executableDdlJob.addTaskRelationship(e6, e9);
        executableDdlJob.addTaskRelationship(e7, e9);
        executableDdlJob.addTaskRelationship(e8, e9);

        TaskScheduler taskScheduler = executableDdlJob.createTaskScheduler();
        while (!taskScheduler.isAllTaskDone()) {
            taskScheduler.markAsDone(taskScheduler.poll());
        }
        Assert.assertFalse(taskScheduler.hasMoreExecutable());
        Assert.assertFalse(taskScheduler.hasFailedTask());
        Assert.assertNull(taskScheduler.poll());
        Assert.assertTrue(taskScheduler.pollBatch().isEmpty());
    }

    /**
     * make sure the execution order follows the DAG dependency
     */
    @Test
    public void test2() {
        TestEmptyTask e1 = TestEmptyTask.createSleep1sTask("e1");
        TestEmptyTask e2 = TestEmptyTask.createSleep1sTask("e2");
        TestEmptyTask e3 = TestEmptyTask.createSleep1sTask("e3");
        TestEmptyTask e4 = TestEmptyTask.createSleep1sTask("e4");
        TestEmptyTask e5 = TestEmptyTask.createSleep1sTask("e5");
        TestEmptyTask e6 = TestEmptyTask.createSleep1sTask("e6");
        TestEmptyTask e7 = TestEmptyTask.createSleep1sTask("e7");
        TestEmptyTask e8 = TestEmptyTask.createSleep1sTask("e8");
        TestEmptyTask e9 = TestEmptyTask.createSleep1sTask("e9");

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addTask(e1);
        executableDdlJob.addTask(e2);
        executableDdlJob.addTask(e3);
        executableDdlJob.addTask(e4);
        executableDdlJob.addTask(e5);
        executableDdlJob.addTask(e6);
        executableDdlJob.addTask(e7);
        executableDdlJob.addTask(e8);
        executableDdlJob.addTask(e9);

        executableDdlJob.addTaskRelationship(e1, e9);
        executableDdlJob.addTaskRelationship(e2, e9);
        executableDdlJob.addTaskRelationship(e3, e9);
        executableDdlJob.addTaskRelationship(e4, e9);
        executableDdlJob.addTaskRelationship(e5, e9);
        executableDdlJob.addTaskRelationship(e6, e9);
        executableDdlJob.addTaskRelationship(e7, e9);
        executableDdlJob.addTaskRelationship(e8, e9);

        AtomicReference atomicReference = new AtomicReference();

        Executor executor = Executors.newFixedThreadPool(10);
        TaskScheduler taskScheduler = executableDdlJob.createTaskScheduler();

        long beginTs = System.currentTimeMillis();
        while (!taskScheduler.isAllTaskDone()) {
            DdlTask ddlTask = taskScheduler.poll();
            if (ddlTask == null) {
                try {
                    //mock task execute cost 1s ...
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            System.out.println(ddlTask.getTaskId() + " poll");
            executor.execute(() -> {
                try {
                    //mock task execute cost 1s ...
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                atomicReference.set(ddlTask);
                taskScheduler.markAsDone(ddlTask);
                System.out.println(ddlTask.getTaskId() + " mark as done");
            });
        }
        long endTs = System.currentTimeMillis();

        System.out.println("cost time: " + (endTs - beginTs) + "ms");
        Assert.assertTrue((endTs - beginTs) >= 2000l);
        //make sure e9 is the last task
        Assert.assertEquals(e9, atomicReference.get());
        Assert.assertFalse(taskScheduler.hasMoreExecutable());
        Assert.assertFalse(taskScheduler.hasFailedTask());
        Assert.assertNull(taskScheduler.poll());
        Assert.assertTrue(taskScheduler.pollBatch().isEmpty());

    }

}
