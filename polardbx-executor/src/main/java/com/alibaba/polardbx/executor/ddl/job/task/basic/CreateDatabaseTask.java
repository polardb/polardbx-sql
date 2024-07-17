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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

/**
 * Created by zhuqiwei.
 */

@Getter
@TaskName(name = "CreateDatabaseTask")
public class CreateDatabaseTask extends BaseDdlTask {

    private String createDatabaseSql;

    @JSONCreator
    public CreateDatabaseTask(String schemaName, String createDatabaseSql) {
        super(schemaName);
        this.createDatabaseSql = createDatabaseSql;
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl() {
        DdlHelper.getServerConfigManager().executeBackgroundSql(createDatabaseSql, schemaName, null);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl();
    }

}
