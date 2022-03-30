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

package com.alibaba.polardbx.executor.ddl.job;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class MockDdlJob extends DdlJobFactory {

    int expectNodeCount;
    int maxOutEdgeCount;
    int edgeRate;
    boolean mockSubJob;

    public MockDdlJob(int expectNodeCount, int maxOutEdgeCount, int edgeRate, boolean mockSubJob) {
        this.expectNodeCount = expectNodeCount;
        this.maxOutEdgeCount = maxOutEdgeCount;
        this.edgeRate = edgeRate;
        this.mockSubJob = mockSubJob;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = generateRandomDag(expectNodeCount, maxOutEdgeCount, edgeRate, mockSubJob);
        FailPoint.inject(FailPointKey.FP_HIJACK_DDL_JOB_FORMAT, (k, v)->{
            if(StringUtils.equalsIgnoreCase(v, "SEQUELTIAL")){
                executableDdlJob.overrideTasks(generateSequentialDag(expectNodeCount, mockSubJob));
            }else {
                executableDdlJob.overrideTasks(generateRandomDag(expectNodeCount, maxOutEdgeCount, edgeRate, mockSubJob));
            }
        });
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        if (!mockSubJob) {
            String res = "mock_resource_" + ID_GENERATOR.nextId();
            resources.add(res);
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @TaskName(name = "MockDdlTask")
    @Getter
    public static class MockDdlTask extends BaseGmsTask {

        private long duration;

        @JSONCreator
        public MockDdlTask(String schemaName, long duration) {
            super(schemaName, "");
            this.duration = duration;
        }

        @Override
        protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
//            FailPoint.throwException();
            try {
                System.out.println(schemaName + " sleep " + duration + "ms");
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
            try {
                System.out.println(schemaName + " sleep " + duration + "ms");
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static Random random = new Random();

    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    private static ExecutableDdlJob generateRandomDag(int expectNodeCount, int maxOutEdgeCount, int edgeRate,
                                                      boolean mockSubJob) {
        int nodeCount = expectNodeCount * 2 / 3 + random.nextInt(expectNodeCount * 4 / 3) + 1;

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<Integer> oldNode = new ArrayList<>(nodeCount);
        List<DdlTask> foobar = new ArrayList<>(nodeCount);
        StringBuilder sb = new StringBuilder();
        sb.append("digraph {\n");
        for (int i = 0; i < nodeCount; i++) {
            sb.append(String.format("  %d;\n", i));

            oldNode.add(i);
            DdlTask task;
            if (!mockSubJob) {
                task = new MockDdlTask(String.valueOf(i), 1000L);
            } else {
                task = new SubJobTask(String.valueOf(i), FailPointKey.FP_INJECT_SUBJOB, FailPointKey.FP_INJECT_SUBJOB);
            }
            task.setTaskId(ID_GENERATOR.nextId());
            foobar.add(task);
            executableDdlJob.addTask(task);
        }

        for (int i = 0; i < nodeCount; i++) {
            int from = oldNode.get(i);
            for (int j = 0; j < maxOutEdgeCount; j++) {
                if (random.nextInt(100) >= edgeRate) {
                    continue;
                }
                int to = from + random.nextInt(nodeCount - from);
                if (from == to) {
                    continue;
                }
                sb.append(String.format("  %d -> %d;\n", from, to));
                executableDdlJob.addTaskRelationship(foobar.get(from), foobar.get(to));
            }
        }

        sb.append("}\n");
        return executableDdlJob;
    }

    private static ExecutableDdlJob generateSequentialDag(int expectNodeCount, boolean mockSubJob) {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();
        for (int i = 0; i < expectNodeCount; i++) {
            DdlTask task;
            if (!mockSubJob) {
                task = new MockDdlTask(String.valueOf(i), 1000L);
            } else {
                task = new SubJobTask(String.valueOf(i), FailPointKey.FP_INJECT_SUBJOB, FailPointKey.FP_INJECT_SUBJOB);
            }
            task.setTaskId(ID_GENERATOR.nextId());
            taskList.add(task);
        }

        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

}
