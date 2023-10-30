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
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "ChangeDatabaseReadWriteStatusTask")
public class ChangeDatabaseReadWriteStatusTask extends BaseDdlTask {
    protected Map<String, Integer> dbNameAndReadWriteStatus;

    @JSONCreator
    public ChangeDatabaseReadWriteStatusTask(String schemaName, Map<String, Integer> dbNameAndReadWriteStatus) {
        super(schemaName);
        this.dbNameAndReadWriteStatus = dbNameAndReadWriteStatus;
        onExceptionTryRecoveryThenPause();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
        dbInfoAccessor.setConnection(metaDbConnection);

        for (Map.Entry<String, Integer> entry : this.dbNameAndReadWriteStatus.entrySet()) {
            dbInfoAccessor.updateDbReadWriteStatusByName(entry.getKey(), entry.getValue());
        }
        //increase the dbInfo's opVersion
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbInfoDataId(), metaDbConnection);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //force every CN node to reload dnInfo immediately
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
        dbInfoAccessor.setConnection(metaDbConnection);
        dbNameAndReadWriteStatus.keySet().forEach(
            dbName -> {
                dbInfoAccessor.updateDbReadWriteStatusByName(dbName, DbInfoRecord.DB_READ_WRITE);
            }
        );

        //increase the dbInfo's opVersion
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbInfoDataId(), metaDbConnection);

        //force everyone to reload dnInfo immediately
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());
    }

}
