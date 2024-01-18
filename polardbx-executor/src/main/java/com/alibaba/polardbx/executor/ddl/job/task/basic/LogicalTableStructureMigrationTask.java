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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.migration.TableMigrationTaskInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "LogicalTableStructureMigrationTask")
public class LogicalTableStructureMigrationTask extends BaseDdlTask {
    final private String createTableSqlSrc;
    final private String createTableSqlDst;
    final private String tableSchemaNameSrc;
    final private String tableSchemaNameDst;
    final private String tableName;

    boolean createTableErrorHappened;
    String errorMessage;

    @JSONCreator
    public LogicalTableStructureMigrationTask(String tableSchemaNameSrc, String tableSchemaNameDst, String tableName,
                                              String createTableSqlSrc, String createTableSqlDst) {
        super(tableSchemaNameDst);

        this.createTableSqlSrc = createTableSqlSrc;
        this.createTableSqlDst = createTableSqlDst;
        this.tableSchemaNameSrc = tableSchemaNameSrc;
        this.tableSchemaNameDst = tableSchemaNameDst;
        this.tableName = tableName;
        this.createTableErrorHappened = false;
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl(ExecutionContext executionContext) {
        try {
            DdlHelper.getServerConfigManager().executeBackgroundSql(createTableSqlDst, tableSchemaNameDst, null);
        } catch (Throwable e) {
            errorMessage = e.getMessage();
            createTableErrorHappened = true;
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //log the error info into system table
        DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        ddlEngineTaskAccessor.setConnection(metaDbConnection);
        TableMigrationTaskInfo extraInfo;
        if (createTableErrorHappened) {
            extraInfo = new TableMigrationTaskInfo(
                tableSchemaNameSrc,
                tableSchemaNameDst,
                tableName,
                false,
                errorMessage,
                createTableSqlSrc,
                createTableSqlDst
            );
        } else {
            extraInfo = new TableMigrationTaskInfo(
                tableSchemaNameSrc,
                tableSchemaNameDst,
                tableName,
                true,
                null,
                createTableSqlSrc,
                createTableSqlDst
            );
        }

        ddlEngineTaskAccessor.updateExtraInfoForCreateDbAsLike(
            jobId,
            taskId,
            JSON.toJSONString(extraInfo)
        );
    }

}
