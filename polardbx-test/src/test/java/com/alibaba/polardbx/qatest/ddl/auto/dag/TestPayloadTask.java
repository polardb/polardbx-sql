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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "TestPayloadTask")
public class TestPayloadTask extends BaseValidateTask {

    static IdGenerator idGenerator = IdGenerator.getIdGenerator();

    private String payload;

    @JSONCreator
    public TestPayloadTask(String schemaName, String payload) {
        super(schemaName);
        this.jobId = idGenerator.nextId();
        this.taskId = idGenerator.nextId();
        this.payload = payload;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {

    }

    @Override
    public String getName() {
        return "TestPayloadTask";
    }
}
