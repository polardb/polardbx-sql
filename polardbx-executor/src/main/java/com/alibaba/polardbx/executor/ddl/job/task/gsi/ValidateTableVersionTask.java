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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Map;

@TaskName(name = "ValidateTableVersionTask")
@Getter
public class ValidateTableVersionTask extends BaseValidateTask {

    Map<String, Long> tableVersions;

    @JSONCreator
    public ValidateTableVersionTask(String schemaName, Map<String, Long> tableVersions) {
        super(schemaName);
        this.tableVersions = tableVersions;

    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        doValidate(executionContext);
    }

    public void doValidate(ExecutionContext executionContext) {
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        for (Map.Entry<String, Long> tableVersion : tableVersions.entrySet()) {
            long oldVersion = tableVersion.getValue();
            if (oldVersion > 0) {
                long curVersion = sm.getTable(tableVersion.getKey()).getVersion();
                if (curVersion > oldVersion) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, tableVersion.getKey());
                }
            }
        }
    }
}