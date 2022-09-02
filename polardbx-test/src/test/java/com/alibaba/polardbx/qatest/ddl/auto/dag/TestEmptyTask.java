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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;

@TaskName(name = "TestEmptyTask")
public class TestEmptyTask extends BaseGmsTask {

    static IdGenerator idGenerator = IdGenerator.getIdGenerator();

    public TestEmptyTask(String schemaName) {
        super(schemaName, "");
        this.taskId = idGenerator.nextId();
        runnable = null;
    }

    final Runnable runnable;

    private TestEmptyTask(String schemaName, Runnable runnable) {
        super(schemaName, "");
        this.taskId = idGenerator.nextId();
        this.runnable = runnable;
    }

    public static TestEmptyTask createSleep1sTask(String schemaName) {
        return new TestEmptyTask(schemaName, () -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    protected void executeImpl(Connection connection, ExecutionContext executionContext) {
        if (runnable != null) {
            runnable.run();
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {

    }

    @Override
    public String toString() {
        return schemaName;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TestEmptyTask)) {
            return false;
        }
        return schemaName.equals(((TestEmptyTask) obj).schemaName);
    }

    @Override
    public int hashCode() {
        return schemaName.hashCode();
    }
}