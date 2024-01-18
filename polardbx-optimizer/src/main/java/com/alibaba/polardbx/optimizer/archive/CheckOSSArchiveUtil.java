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

package com.alibaba.polardbx.optimizer.archive;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 * check whether the ddl should unarchive oss table first
 *
 * @author Shi Yuxuan
 */
public class CheckOSSArchiveUtil {

    /**
     * check all tables in the table group has no archive table, based on record in memory
     *
     * @param schema schema of the table group
     * @param tableGroup the table group to be checked
     * @return true if tablegroup has not oss table
     */
    public static boolean checkTableGroupWithoutOSS(String schema, String tableGroup) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return true;
        }
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schema).getTableGroupInfoManager();
        if (tableGroupInfoManager == null) {
            return true;
        }
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
        if (tableGroupConfig == null) {
            return true;
        }
        SchemaManager sm = OptimizerContext.getContext(schema).getLatestSchemaManager();

        // check table one by one
        try {
            for (TablePartRecordInfoContext table : tableGroupConfig.getAllTables()) {
                LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
                    sm.getTable(table.getLogTbRec().getTableName())
                        .getLocalPartitionDefinitionInfo();
                if (localPartitionDefinitionInfo == null) {
                    continue;
                }
                if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
                    StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
                    continue;
                }
                return false;
            }
        } catch (TableNotFoundException ignore) {
            // ignore
        }
        return true;
    }

    /**
     * check if the table's archive table support ddl
     *
     * @param schema schema of the table
     * @param table the table to be tested
     * @return true if can perform ddl
     */
    public static boolean withoutOldFileStorage(String schema, String table) {
        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(table);
        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = tableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo == null) {
            return true;
        }
        if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
            StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
            return true;
        }
        tableMeta =
            OptimizerContext.getContext(localPartitionDefinitionInfo.getArchiveTableSchema()).getLatestSchemaManager()
                .getTableWithNull(localPartitionDefinitionInfo.getArchiveTableName());
        return !tableMeta.isOldFileStorage();
    }


    /**
     * check the table has no archive table
     *
     * @param schema schema of the table
     * @param table the table to be tested
     */
    public static boolean checkWithoutOSS(String schema, String table) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(table)) {
            return true;
        }
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return true;
        }
        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(table);
        if (tableMeta == null) {
            return true;
        }
        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = tableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo == null) {
            return true;
        }
        if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
            StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
            return true;
        }

        return false;
    }


    /**
     * get the archive table schema and name of the primary table
     *
     * @param schemaName the schema of primary table
     * @param tableName the name of primary table
     * @return the schema, table pair. null if any of schema/table is empty
     */
    public static Optional<Pair<String, String>> getArchive(String schemaName, String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            return Optional.empty();
        }
        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (oc == null) {
            return Optional.empty();
        }
        TableMeta tableMeta;
        try {
            tableMeta = oc.getLatestSchemaManager().getTable(tableName);
        } catch (TableNotFoundException ignore) {
            return Optional.empty();
        }
        LocalPartitionDefinitionInfo definitionInfo = tableMeta.getLocalPartitionDefinitionInfo();
        if (definitionInfo == null) {
            return Optional.empty();
        }
        if (StringUtils.isEmpty(definitionInfo.getArchiveTableSchema())
            || StringUtils.isEmpty(definitionInfo.getArchiveTableName())) {
            return Optional.empty();
        }
        return Optional.of(new Pair<>(definitionInfo.getArchiveTableSchema(), definitionInfo.getArchiveTableName()));
    }

    public static void checkTTLSource(String schemaName, String tableName) {
        // forbid ddl on ttl table
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConn);

            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecordByArchiveTable(schemaName, tableName);
            if (record != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "DDL on file storage table " + schemaName + "." + tableName + " bound to "
                        + record.getTableSchema() + "." + record.getTableName());
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    public static Optional<Pair<String, String>> getTTLSource(String schemaName, String tableName) {
        // get ttl source for oss table
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConn);

            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecordByArchiveTable(schemaName, tableName);
            return record != null ?
                Optional.of(new Pair<>(record.getTableSchema(), record.getTableName()))
                : Optional.empty();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }
}
