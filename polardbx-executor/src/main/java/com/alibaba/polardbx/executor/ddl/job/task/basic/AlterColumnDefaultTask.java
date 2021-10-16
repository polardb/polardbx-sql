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
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

/**
 * Set flag for alter column default value
 *
 * @author moyi
 * @since 2021/08
 */
@TaskName(name = "AlterColumnDefaultTask")
@Getter
public class AlterColumnDefaultTask extends BaseGmsTask {

    /**
     * Begin or finish
     */
    private boolean begin;
    private List<String> columnNames;

    @JSONCreator
    public AlterColumnDefaultTask(String schemaName, String logicalTableName, List<String> columnNames,
                                  boolean begin) {
        super(schemaName, logicalTableName);
        this.columnNames = columnNames;
        this.begin = begin;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        for (String columnName : columnNames) {
            if (begin) {
                TableMetaChanger.beginAlterColumnDefaultValue(metaDbConnection, schemaName, logicalTableName,
                    columnName);
            } else {
                TableMetaChanger.endAlterColumnDefaultValue(metaDbConnection, schemaName, logicalTableName,
                    columnName);
            }
        }
        SQLRecorderLogger.ddlEngineLogger.info(String.format("%s column backfill_default_flag for %s.%s.%s",
            begin ? "set" : "unset",
            schemaName, logicalTableName, StringUtils.join(columnNames, ",")));

    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        this.begin = !this.begin;
        executeImpl(metaDbConnection, executionContext);
        this.begin = !this.begin;
    }

    @Override
    public String remark() {
        return String.format("|%s backfill %s.%s.%s",
            begin ? "set" : "unset",
            schemaName, logicalTableName, StringUtils.join(columnNames, ","));
    }
}
