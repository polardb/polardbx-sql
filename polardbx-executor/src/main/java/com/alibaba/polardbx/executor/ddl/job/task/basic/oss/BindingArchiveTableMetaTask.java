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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.text.MessageFormat;

@Getter
@TaskName(name = "BindingArchiveTableMetaTask")
public class BindingArchiveTableMetaTask extends BaseGmsTask {
    private String sourceTableSchema;
    private String sourceTableName;
    private String archiveTableSchema;
    private String archiveTableName;
    private ArchiveMode archiveMode;

    private String oldArchiveTableSchema;
    private String oldArchiveTableName;

    @JSONCreator
    public BindingArchiveTableMetaTask(String schemaName, String logicalTableName,
                                       String sourceTableSchema, String sourceTableName,
                                       String archiveTableSchema, String archiveTableName,
                                       ArchiveMode archiveMode) {
        super(schemaName, logicalTableName);
        this.sourceTableSchema = sourceTableSchema;
        this.sourceTableName = sourceTableName;
        this.archiveTableSchema = archiveTableSchema;
        this.archiveTableName = archiveTableName;
        this.archiveMode = archiveMode;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (archiveMode == ArchiveMode.TTL && sourceTableSchema != null && sourceTableName != null
            && archiveTableSchema != null && archiveTableName != null) {

            // check old archive table name
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecord(sourceTableSchema, sourceTableName);

            // not a local partition table
            if (record == null) {
                throw GeneralUtil.nestedException(
                    MessageFormat.format("{0}.{1} is not a local partition table.",
                        sourceTableSchema, sourceTableName));
            }

            // already has archive table but don't allow replace it.
            if (record.getArchiveTableName() != null || record.getArchiveTableSchema() != null) {
                this.oldArchiveTableName = record.getArchiveTableName();
                this.oldArchiveTableSchema = record.getArchiveTableSchema();
                boolean allowReplace
                    = executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_REPLACE_ARCHIVE_TABLE);
                if (!allowReplace) {
                    throw GeneralUtil.nestedException(
                        MessageFormat.format("The table {0}.{1} already has archive table {2}.{3}, "
                                + "please use connection param: ALLOW_REPLACE_ARCHIVE_TABLE=true to allow replace archive table.",
                            sourceTableSchema, sourceTableName, oldArchiveTableSchema, oldArchiveTableName));
                }
            }
            TableMetaChanger
                .updateArchiveTable(metaDbConnection, sourceTableSchema, sourceTableName, archiveTableSchema,
                    archiveTableName);
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (archiveMode == ArchiveMode.TTL && sourceTableSchema != null && sourceTableName != null
            && archiveTableSchema != null && archiveTableName != null) {
            TableMetaChanger.updateArchiveTable(metaDbConnection, sourceTableSchema, sourceTableName,
                oldArchiveTableSchema, oldArchiveTableName);
        }
    }
}
