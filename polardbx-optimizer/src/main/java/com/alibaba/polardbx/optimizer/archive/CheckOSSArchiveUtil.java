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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
            for (String tableName : tableGroupConfig.getAllTables()) {
                LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
                    sm.getTable(tableName)
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

        TtlDefinitionInfo ttlDefInfo = tableMeta.getTtlDefinitionInfo();
        if (ttlDefInfo != null && ttlDefInfo.alreadyBoundArchiveTable()) {
            return false;
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

            TtlInfoRecord ttlRec = tableInfoManager.getTtlInfoRecordByArchiveTable(schemaName, tableName);
            if (ttlRec != null) {
                return Optional.of(new Pair<>(ttlRec.getTableSchema(), ttlRec.getTableName()));
            }

            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecordByArchiveTable(schemaName, tableName);
            return record != null ?
                Optional.of(new Pair<>(record.getTableSchema(), record.getTableName()))
                : Optional.empty();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    public static Optional<TtlSourceInfo> getTtlSourceInfo(String schemaName, String tableName) {
        // get ttl source for oss table

        TtlSourceInfo sourceInfo = new TtlSourceInfo();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConn);

            TtlInfoRecord ttlRec = tableInfoManager.getTtlInfoRecordByArchiveTable(schemaName, tableName);
            if (ttlRec != null) {
                sourceInfo.setTtlInfoRecord(ttlRec);
                sourceInfo.setUseRowLevelTtl(true);
                sourceInfo.setTtlTable(true);
                return Optional.of(sourceInfo);
            }

            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecordByArchiveTable(schemaName, tableName);
            if (record != null) {
                sourceInfo.setTableLocalPartitionRecord(record);
                sourceInfo.setUseRowLevelTtl(false);
                sourceInfo.setTtlTable(true);
                return Optional.of(sourceInfo);
            } else {
                return Optional.empty();
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    public static void validateArchivePartitions(String schemaName, String tableName, List<String> partNames,
                                                 ExecutionContext executionContext) {
        final TableMeta ossTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

        if (ossTableMeta == null) {
            throw new TddlNestableRuntimeException(String.format(
                "The ttl tmp table %s.%s does not exist.", schemaName, tableName));
        }

        boolean checkArchivePartitionReady =
            executionContext.getParamManager().getBoolean(ConnectionParams.CHECK_ARCHIVE_PARTITION_READY);

        PartitionInfo partitionInfo = ossTableMeta.getPartitionInfo();
        for (String partName : partNames) {
            PartitionSpec ps = partitionInfo.getPartSpecSearcher().getPartSpecByPartName(partName);
            if (ps == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "The archive partition %s does not exists", partName));
            }

            if (checkArchivePartitionReady && !ps.isReadyForArchiving()) {
                throw new TddlNestableRuntimeException(String.format(
                    "The archive partition %s is not ready for archiving", partName));
            }
        }
    }

    public static void checkColumnConsistency(String ossSchemaName, String ossTableName, String tmpSchemaName,
                                              String tmpTableName) {
        final TableMeta ossTableMeta =
            OptimizerContext.getContext(ossSchemaName).getLatestSchemaManager().getTable(ossTableName);
        final TableMeta tmpTableMeta =
            OptimizerContext.getContext(tmpSchemaName).getLatestSchemaManager().getTable(tmpTableName);

        if (ossTableMeta == null) {
            throw new TddlNestableRuntimeException(String.format(
                "The oss table %s.%s does not exist.", ossSchemaName, ossTableName));
        }

        if (tmpTableMeta == null) {
            throw new TddlNestableRuntimeException(String.format(
                "The ttl tmp table bound to the oss table %s.%s does not exist.", ossSchemaName, ossTableName));
        }

        checkColumnConsistency(ossTableMeta, tmpTableMeta);
    }

    public static void checkColumnConsistency(TableMeta sourceTable, TableMeta targetTable) {
        // check columns
        List<ColumnMeta> sortedSourceColumns =
            sourceTable.getPhysicalColumns().stream().sorted(Comparator.comparing(ColumnMeta::getOriginColumnName))
                .collect(Collectors.toList());

        List<ColumnMeta> sortedTargetColumns =
            targetTable.getPhysicalColumns().stream().sorted(Comparator.comparing(ColumnMeta::getOriginColumnName))
                .collect(Collectors.toList());

        if (sortedSourceColumns.size() != sortedTargetColumns.size()) {
            throwMetaInconsistentError(sourceTable, targetTable);
        }

        for (int i = 0; i < sortedSourceColumns.size(); i++) {
            ColumnMeta c1 = sortedSourceColumns.get(i);
            ColumnMeta c2 = sortedTargetColumns.get(i);
            // column name inconsistent
            if (!c1.getOriginColumnName().equalsIgnoreCase(c2.getOriginColumnName())) {
                throwMetaInconsistentError(sourceTable, targetTable);
            }
            // column type inconsistent
            if (!DataTypeUtil.equals(c1.getDataType(), c2.getDataType(), true)) {
                throwMetaInconsistentError(sourceTable, targetTable);
            }
        }
    }

    private static void throwMetaInconsistentError(TableMeta sourceTable, TableMeta targetTable) {
        throw GeneralUtil.nestedException(MessageFormat.format(
            "the column metas of source table {0} and target table {1} are not consistent, "
                + "please create a new archive table for source table {0}", sourceTable.getTableName(),
            targetTable.getTableName()));
    }
}
