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
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.LikeTableInfo;
import com.alibaba.polardbx.rule.TableRule;
import lombok.Getter;

@Getter
@TaskName(name = "CreateTableValidateTask")
public class CreateTableValidateTask extends BaseValidateTask {

    private String logicalTableName;
    private TablesExtRecord tablesExtRecord;
    private LikeTableInfo likeTableInfo;

    @JSONCreator
    public CreateTableValidateTask(String schemaName,
                                   String logicalTableName,
                                   TablesExtRecord tablesExtRecord,
                                   LikeTableInfo likeTableInfo) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.tablesExtRecord = tablesExtRecord;
        this.likeTableInfo = likeTableInfo;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableNonExistence(schemaName, logicalTableName, executionContext);
        if (likeTableInfo != null) {
            TableValidator.validateTableExistence(likeTableInfo.getSchemaName(), likeTableInfo.getTableName(),
                executionContext);
        }

        TableRule newTableRule = DdlJobDataConverter.buildTableRule(tablesExtRecord);

        TableValidator.validatePhysicalTableNames(schemaName, logicalTableName, newTableRule,
            executionContext.isUseHint());
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + logicalTableName;
    }

}
