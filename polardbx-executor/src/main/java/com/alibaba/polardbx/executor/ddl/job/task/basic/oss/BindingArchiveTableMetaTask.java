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
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import lombok.Getter;

import java.sql.Connection;
import java.text.MessageFormat;

@Getter
@TaskName(name = "BindingArchiveTableMetaTask")
public class BindingArchiveTableMetaTask extends BaseGmsTask {

    /**
     * The schemaName of ttl table (oss archive source)
     */
    private String sourceTableSchema;
    /**
     * The tableName of ttl table (oss archive source)
     */
    private String sourceTableName;

    /**
     * The new schemaName of oss table (oss archive table)
     */
    private String archiveTableSchema;
    /**
     * The new tableName of oss table (oss archive table)
     */
    private String archiveTableName;

    private String tableEngine;

    private ArchiveMode archiveMode;

    /**
     * The old schemaName of oss table (oss archive table) bound to ttl table
     */
    private String oldArchiveTableSchema;
    /**
     * The old tableName of oss table (oss archive table) bound to ttl table
     */
    private String oldArchiveTableName;

    @JSONCreator
    public BindingArchiveTableMetaTask(String schemaName, String logicalTableName,
                                       String sourceTableSchema, String sourceTableName,
                                       String archiveTableSchema, String archiveTableName,
                                       String tableEngine,
                                       ArchiveMode archiveMode) {
        super(schemaName, logicalTableName);

        this.sourceTableSchema = sourceTableSchema;
        this.sourceTableName = sourceTableName;

        this.archiveTableSchema = archiveTableSchema;
        this.archiveTableName = archiveTableName;

        this.tableEngine = tableEngine;
        this.archiveMode = archiveMode;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (archiveMode == ArchiveMode.TTL && sourceTableSchema != null && sourceTableName != null
            && archiveTableSchema != null && archiveTableName != null) {

            // check old archive table name
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            TableLocalPartitionRecord localPartRecord =
                tableInfoManager.getLocalPartitionRecord(sourceTableSchema, sourceTableName);
            TtlInfoRecord ttlRecord =
                tableInfoManager.getTtlInfoRecord(sourceTableSchema, sourceTableName);

            String oldArcTblSchema = null;
            String oldArcTblName = null;

            boolean useTtl = ttlRecord != null;
            boolean useLocalPart = localPartRecord != null;

            if (!useTtl) {
                // not a local partition table
                if (!useLocalPart) {
                    throw GeneralUtil.nestedException(
                        MessageFormat.format("{0}.{1} is not a local partition table.",
                            sourceTableSchema, sourceTableName));
                }
                oldArcTblSchema = localPartRecord.getArchiveTableSchema();
                oldArcTblName = localPartRecord.getArchiveTableName();
            } else {
                oldArcTblSchema = ttlRecord.getArcTblSchema();
                oldArcTblName = ttlRecord.getArcTblName();
            }

            // already has archive table but don't allow replace it.
            if (oldArcTblSchema != null || oldArcTblName != null) {
                this.oldArchiveTableName = oldArcTblName;
                this.oldArchiveTableSchema = oldArcTblSchema;
                boolean allowReplace
                    = executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_REPLACE_ARCHIVE_TABLE);
                if (!allowReplace) {
                    throw GeneralUtil.nestedException(
                        MessageFormat.format("The table {0}.{1} already has archive table {2}.{3}, "
                                + "please use connection param: ALLOW_REPLACE_ARCHIVE_TABLE=true to allow replace archive table.",
                            sourceTableSchema, sourceTableName, oldArchiveTableSchema, oldArchiveTableName));
                }
            }

            if (!useTtl) {
                TableMetaChanger
                    .updateArchiveTable(metaDbConnection, sourceTableSchema, sourceTableName, archiveTableSchema,
                        archiveTableName);
            } else {
                Integer arcKind = TtlDefinitionInfo.getArcKindByEngine(tableEngine);
                String arcTmpTblName = TtlUtil.buildArcTmpNameByArcTblName(archiveTableName);
                String arcTmpTblSchema = archiveTableSchema;
                TableMetaChanger
                    .updateArchiveTableForTtlInfo(metaDbConnection, arcKind, sourceTableSchema, sourceTableName,
                        archiveTableSchema,
                        archiveTableName, arcTmpTblSchema, arcTmpTblName);
            }
            try {
                TableInfoManager.updateTableVersion(sourceTableSchema, sourceTableName, metaDbConnection);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        TtlInfoRecord ttlRecord =
            tableInfoManager.getTtlInfoRecord(sourceTableSchema, sourceTableName);
        boolean useRowLevelTtl = ttlRecord != null;

        if (archiveMode == ArchiveMode.TTL && sourceTableSchema != null && sourceTableName != null
            && archiveTableSchema != null && archiveTableName != null) {
            if (!useRowLevelTtl) {
                TableMetaChanger.updateArchiveTable(metaDbConnection, sourceTableSchema, sourceTableName,
                    oldArchiveTableSchema, oldArchiveTableName);
            } else {
                Integer arcKind = TtlDefinitionInfo.getArcKindByEngine("");
                String oldArcTmpTblName = TtlUtil.buildArcTmpNameByArcTblName(oldArchiveTableName);
                String oldArcTmpTblSchema = oldArchiveTableSchema;
                TableMetaChanger.updateArchiveTableForTtlInfo(metaDbConnection, arcKind, sourceTableSchema,
                    sourceTableName,
                    oldArchiveTableSchema, oldArchiveTableName, oldArcTmpTblSchema, oldArcTmpTblName);
            }

            try {
                TableInfoManager.updateTableVersion(sourceTableSchema, sourceTableName, metaDbConnection);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        }
    }
}
