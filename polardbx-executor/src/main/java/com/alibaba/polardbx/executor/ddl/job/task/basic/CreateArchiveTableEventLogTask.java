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
import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.CreateDatabaseEventLogTask.convertSecondsToHhMmSs;

@Getter
@TaskName(name = "CreateArchiveTableEventLogTask")
public class CreateArchiveTableEventLogTask extends BaseDdlTask {
    final String tableName;
    final LocalPartitionDefinitionInfo localPartitionDefinitionInfo;
    final Boolean isOssTable;
    final String srcSchemaName;
    final String srcTableName;
    final ArchiveMode archiveMode;
    final Engine engine;

    @JSONCreator
    public CreateArchiveTableEventLogTask(String schemaName, String tableName,
                                          LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                          Boolean isOssTable, String srcSchemaName, String srcTableName,
                                          ArchiveMode archiveMode, Engine engine) {
        super(schemaName);
        this.tableName = tableName;
        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
        this.isOssTable = isOssTable;
        this.srcSchemaName = srcSchemaName;
        this.srcTableName = srcTableName;
        this.archiveMode = archiveMode;
        this.engine = engine;
    }

    /* ctor for TTL table */
    public CreateArchiveTableEventLogTask(String schemaName, String tableName,
                                          LocalPartitionDefinitionInfo localPartitionDefinitionInfo) {
        this(schemaName, tableName, localPartitionDefinitionInfo, false, null, null, null, null);
    }

    /* ctor for OSS table */
    public CreateArchiveTableEventLogTask(String schemaName, String tableName, String srcSchemaName,
                                          String srcTableName, ArchiveMode archiveMode, Engine engine) {
        this(schemaName, tableName, null, true, srcSchemaName, srcTableName, archiveMode, engine);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (isOssTable) {
            log4OssTable(metaDbConnection, executionContext);
        } else {
            log4TtlTable(metaDbConnection, executionContext);
        }
    }

    private void log4TtlTable(Connection metaDbConnection, ExecutionContext executionContext) {
        EventLogger.log(EventType.CREATE_TTL_TABLE,
            String.format("create ttl table finished, schema: %s, table: %s, local partition: [%s]",
                schemaName,
                tableName,
                localPartitionDefinitionInfo
            )
        );
    }

    private void log4OssTable(Connection metaDbConnection, ExecutionContext executionContext) {
        long timeElapseInSeconds = -1;

        try {
            DdlEngineAccessor engineAccessor = new DdlEngineAccessor();
            engineAccessor.setConnection(metaDbConnection);
            DdlEngineRecord jobsRecord = engineAccessor.query(getJobId());
            if (jobsRecord != null) {
                timeElapseInSeconds = (System.currentTimeMillis() - jobsRecord.gmtCreated) / 1000;
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("error occurs while query ddl job %s", getJobId()));
        }

        String timeCostString = timeElapseInSeconds == -1 ? "none" : convertSecondsToHhMmSs(timeElapseInSeconds);
        EventLogger.log(EventType.CREATE_OSS_TABLE,
            String.format(
                "create oss table finished, schema: %s, table: %s, source schema: %s, source table: %s, archive mode: %s, table engine: %s, time cost: [%s]",
                schemaName,
                tableName,
                srcSchemaName,
                srcTableName,
                archiveMode,
                engine,
                timeCostString
            )
        );
    }
}
