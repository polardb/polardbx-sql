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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.charset.MySQLCharsetDDLValidator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableRepartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TableValidator {
    private static final String GOD_USER_NAME = "polardbx_root";

    public static void validateTableInfo(String schemaName, String logicalTableName, SqlCreateTable sqlCreateTable,
                                         ParamManager paramManager) {
        validateTableName(logicalTableName);

        validateTableNameLength(logicalTableName);

        LimitValidator.validateTableCount(schemaName);

        validateTableComment(logicalTableName, sqlCreateTable.getComment());

        validateCollationImplemented(sqlCreateTable);

        // Check the number of table partitions per physical database.
        if (sqlCreateTable.getTbpartitionBy() != null && sqlCreateTable.getTbpartitions() != null) {
            Integer tbPartitionsDefined = ((SqlLiteral) sqlCreateTable.getTbpartitions()).intValue(false);
            // The limit by default or user defines.
            LimitValidator.validateTablePartitionNum(tbPartitionsDefined, paramManager);
        }
    }

    public static void validateTableName(String logicalTableName) {
        if (TStringUtil.isEmpty(logicalTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Empty table name is invalid");
        }
    }

    public static void validateTableNameLength(String logicalTableName) {
        LimitValidator.validateTableNameLength(logicalTableName);
    }

    public static void validateTableComment(String logicalTableName, String tableComment) {
        LimitValidator.validateTableComment(logicalTableName, tableComment);
    }

    /**
     * Expect the logical table to not exist, such as CREATE TABLE.
     */
    public static void validateTableNonExistence(String schemaName, String logicalTableName,
                                                 ExecutionContext executionContext) {
        if (executionContext.isUseHint()) {
            return;
        }

        if (checkIfTableExists(schemaName, logicalTableName)) {
            // Terminate and rollback the DDL job.
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, logicalTableName);
        }

        validateViewExistence(schemaName, logicalTableName);
    }

    /**
     * Check table group existence
     */
    public static void validateTableGroupExistence(String schemaName, List<Long> tableGroupIds,
                                                   ExecutionContext executionContext) {
        if (executionContext.isUseHint() || tableGroupIds == null || tableGroupIds.isEmpty()) {
            return;
        }

        for (Long tableGroupId : tableGroupIds) {
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
            TableGroupConfig tableGroupConfigFromMetaDb = TableGroupUtils.getTableGroupInfoByGroupId(tableGroupId);

            if (tableGroupConfig == null || tableGroupConfigFromMetaDb == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                    "table group: " + tableGroupId + " not exist");
            }
        }
    }

    /**
     * Expect the logical table in the target table group, such as DROP TABLE, DROP GSI.
     */
    public static void validateTableInTableGroup(String schemaName, String tbName, List<Long> tableGroupIds,
                                                 ExecutionContext executionContext) {
        if (executionContext.isUseHint() || tableGroupIds == null || tableGroupIds.isEmpty()) {
            return;
        }

        if (!StringUtils.isEmpty(tbName)) {
            tbName = tbName.toLowerCase();
        }

        assert tableGroupIds.size() == 1;
        Long tableGroupId = tableGroupIds.get(0);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        TableGroupConfig tableGroupConfigFromMetaDb = TableGroupUtils.getTableGroupInfoByGroupId(tableGroupId);

        if (tableGroupConfig == null || tableGroupConfigFromMetaDb == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "table group: " + tableGroupId + " not exist");
        }

        if (!tableGroupConfig.containsTable(tbName) || !tableGroupConfigFromMetaDb.containsTable(tbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_CHANGED,
                "table group: " + tableGroupId + " has already been changed by another ddl.");
        }
    }

    public static void validateTableGroupChange(String schemaName, TableGroupConfig saveTableGroupConfig) {
        if (saveTableGroupConfig != null && GeneralUtil.isNotEmpty(saveTableGroupConfig.getPartitionGroupRecords())) {
            /*
             * 1、create table with empty tablegroup
             * 2、create table with non-empty tablegroup
             * 3、create table without specify tablegroup, but match existing tablegroup
             * 4、create table without specify tablegroup, and not match existing tablegroup
             * 5、drop table
             * */
            List<PartitionGroupRecord> partitionGroupRecords = saveTableGroupConfig.getPartitionGroupRecords();
            Long tgId = partitionGroupRecords.get(0).tg_id;
            Long firstPgId = partitionGroupRecords.get(0).id;
            boolean needValidTableGroup = (tgId != TableGroupRecord.INVALID_TABLE_GROUP_ID);
            if (needValidTableGroup) {
                OptimizerContext oc =
                    Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                TableGroupConfig curTableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tgId);
                validateTableGroupChange(curTableGroupConfig, saveTableGroupConfig);
            }

            Set<String> physicalGroups = new HashSet<>();
            for (PartitionGroupRecord record : GeneralUtil
                .emptyIfNull(saveTableGroupConfig.getPartitionGroupRecords())) {
                if (record.id == TableGroupRecord.INVALID_TABLE_GROUP_ID) {
                    physicalGroups.add(GroupInfoUtil.buildGroupNameFromPhysicalDb(record.phy_db));
                }
            }
            for (String group : physicalGroups) {
                TableGroupValidator.validatePhysicalGroupIsNormal(schemaName, group);
            }
        }
    }

    public static void validateTableGroupChange(TableGroupConfig curTableGroupConfig,
                                                TableGroupConfig saveTableGroupConfig) {
        List<PartitionGroupRecord> partitionGroupRecords = saveTableGroupConfig.getPartitionGroupRecords();
        Long tgId = partitionGroupRecords.get(0).tg_id;
        Long firstPgId = partitionGroupRecords.get(0).id;
        boolean invalid =
            (curTableGroupConfig == null) || (curTableGroupConfig.getPartitionGroupRecords().isEmpty()
                && firstPgId > 0) || (!curTableGroupConfig.getPartitionGroupRecords().isEmpty()
                && firstPgId <= 0) || (curTableGroupConfig.getPartitionGroupRecords().size()
                != partitionGroupRecords.size() && firstPgId > 0);
        if (invalid) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                    tgId.toString()));
        } else if (!curTableGroupConfig.getPartitionGroupRecords().isEmpty()) {
            List<PartitionGroupRecord> curPartitionGroupRecords =
                curTableGroupConfig.getPartitionGroupRecords();
            assert curPartitionGroupRecords.size() == partitionGroupRecords.size();
            for (int i = 0; i < curPartitionGroupRecords.size(); i++) {
                PartitionGroupRecord curParGroupRecord = curPartitionGroupRecords.get(i);
                PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(curParGroupRecord.partition_name))
                    .findFirst().orElse(null);
                invalid = (partitionGroupRecord == null) || (partitionGroupRecord.id.longValue()
                    != curParGroupRecord.id.longValue()) || (!partitionGroupRecord.phy_db
                    .equalsIgnoreCase(curParGroupRecord.phy_db));
                if (invalid) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                            tgId.toString()));
                }
            }
        }
    }

    public static void validateTableGroupNoExists(String schemaName, String tableGroupName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(connection);
            List<TableGroupRecord> tableGroupRecords =
                tableGroupAccessor.getTableGroupsBySchemaAndName(schemaName, tableGroupName, false);
            if (GeneralUtil.isNotEmpty(tableGroupRecords)) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                        tableGroupName));
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static void validUnexpectedColumnType(String schemaName, String tableName, String unexpectedType) {
        Preconditions.checkNotNull(unexpectedType);
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnsAccessor columnsAccessor = new ColumnsAccessor();
            columnsAccessor.setConnection(connection);
            List<ColumnsRecord> columnsRecords = columnsAccessor.query(schemaName, tableName);
            for (ColumnsRecord columnsRecord : columnsRecords) {
                if(unexpectedType.equalsIgnoreCase(columnsRecord.dataType)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                        String.format("unexpected column [%s] in table [%s] with data type: [%s]",
                            columnsRecord.columnName,
                            columnsRecord.tableName,
                            unexpectedType));
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    /**
     * see checkDdlOnGsi()
     */
    public static void validateTableIsNotGsi(String schemaName,
                                             String logicalTableName,
                                             ErrorCode errorCode,
                                             String... params) {
        if (checkTableIsGsi(schemaName, logicalTableName)) {
            throw new TddlRuntimeException(errorCode, params);
        }
    }

    public static void validateTableIsGsi(String schemaName,
                                          String logicalTableName,
                                          ErrorCode errorCode,
                                          String... params) {
        if (!checkTableIsGsi(schemaName, logicalTableName)) {
            throw new TddlRuntimeException(errorCode, params);
        }
    }

    public static boolean checkTableIsGsi(String schemaName, String logicalTableName) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        if (tableMeta == null) {
            return false;
        }
        return tableMeta.isGsi();
    }

    public static boolean checkTableWithGsi(String schemaName, String logicalTableName) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        if (tableMeta == null) {
            return false;
        }
        return tableMeta.withGsi();
    }

    /**
     * Expect the logical table to exist, such as DROP TABLE.
     */
    public static void validateTableExistence(String schemaName,
                                              String logicalTableName,
                                              ExecutionContext executionContext) {
        if (executionContext.isUseHint()) {
            return;
        }

        if (!checkIfTableExists(schemaName, logicalTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, logicalTableName);
        }
    }

    public static boolean checkIfTableExists(String schemaName, String logicalTableName) {
        return new TableInfoManagerDelegate<Boolean>(new TableInfoManager()) {
            @Override
            protected Boolean invoke() {
                return tableInfoManager.checkIfTableExistsWithAnyStatus(schemaName, logicalTableName);
            }
        }.execute();
    }

    public static void validateViewExistence(String schemaName, String logicalTableName) {
        SystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(logicalTableName);
        if (row != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, logicalTableName);
        }
    }

    /**
     * Check if physical table names in new logical table topology have been occupied by existing logical tables.
     **/
    public static void validatePhysicalTableNames(String schemaName, String logicalTableName, TableRule newTableRule,
                                                  boolean withHint) {
        if (newTableRule == null || withHint) {
            return;
        }

        TableRule existingRule =
            OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);

        if (existingRule != null) {
            // The logical table being created already exists, so we skip
            // the check and let original logic handle the scenario:
            // When newly created table has the same rule/topology with
            // existing table, the behavior depends on whether
            // "IF NOT EXISTS" is specified. Otherwise, newly created table
            // with different rule will fail.
            return;
        }

        // Check each physical table with fully qualified name.
        boolean isSingleOrBroadcast =
            (newTableRule.getDbPartitionKeys() == null || newTableRule.getDbPartitionKeys().isEmpty()) && (
                newTableRule.getTbPartitionKeys() == null || newTableRule.getTbPartitionKeys().isEmpty());

        if (isSingleOrBroadcast) {
            // The group and physical table names are incomplete in the
            // topology for single or broadcast table, so we have to build
            // them manually.
            String physicalTableName = newTableRule.getTbNamePattern();
            List<String> groupNames = ExecutorContext.getContext(schemaName).getTopologyHandler().getGroupNames();
            for (String groupName : groupNames) {
                validateFullyQualifiedPhysicalTableName(schemaName, groupName + "." + physicalTableName);
            }
        } else {
            // We can get all group and physical table names from the
            // topology for sharding table.
            Map<String, Set<String>> topology = newTableRule.getActualTopology();
            for (Map.Entry<String, Set<String>> groupAndPhysicalTableNames : topology.entrySet()) {
                String groupName = groupAndPhysicalTableNames.getKey();
                for (String physicalTableName : groupAndPhysicalTableNames.getValue()) {
                    validateFullyQualifiedPhysicalTableName(schemaName, groupName + "." + physicalTableName);
                }
            }
        }
    }

    private static void validateFullyQualifiedPhysicalTableName(String schemaName,
                                                                String fullyQualifiedPhysicalTableName) {
        fullyQualifiedPhysicalTableName = TStringUtil.remove(fullyQualifiedPhysicalTableName, '`').toLowerCase();

        Set<String> logicalTableNames = OptimizerContext.getContext(schemaName).getRuleManager()
            .getLogicalTableNames(fullyQualifiedPhysicalTableName, schemaName);

        if (logicalTableNames != null && logicalTableNames.size() > 0) {
            // If multiple logical tables exists, we just report one to warn.
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "The physical table '" + fullyQualifiedPhysicalTableName
                    + "' already exists and is associated with logical table '" + logicalTableNames.iterator().next()
                    + "'.");
        }
    }

    public static void validateTableNamesForRename(String schemaName, String sourceTableName, String targetTableName) {
        validateTableName(sourceTableName);
        validateTableName(targetTableName);

        Set<String> allTables = OptimizerContext.getContext(schemaName).getRuleManager().mergeTableRule(null);
        if (allTables.contains(targetTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, targetTableName);
        }

        SequenceValidator.validateExistenceForRename(schemaName, targetTableName);
    }


    public static void validateLocality(String schemaName, LocalityDesc localityDesc){
        Long dbId = DbInfoManager.getInstance().getDbInfo(schemaName).id;
        LocalityInfo localityInfo = LocalityManager.getInstance().getLocalityOfDb(dbId);
        if(localityInfo != null && localityDesc != null){
            LocalityDesc dbLocality = LocalityDesc.parse(localityInfo.getLocality());
            if (!dbLocality.compactiableWith(localityDesc)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        " Table locality definition is not compatible with database locality! ");
            }
        }
    }
    public static void validateTruncatePartition(String schemaName, String tableName, SqlAlterTable sqlAlterTable) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        boolean withGsi = tableMeta.withGsi();
        for (SqlAlterSpecification item : sqlAlterTable.getAlters()) {
            if ((item instanceof SqlAlterTableTruncatePartition) && withGsi) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_TRUNCATE_PRIMARY_TABLE, tableName);
            }
        }
    }

    public static void validateCollationImplemented(SqlCreateTable sqlCreateTable) {
        for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
            SqlColumnDeclaration colDef = pair.getValue();
            doValidateCollation(colDef);
        }
    }

    public static void validateCollationImplemented(SqlModifyColumn sqlModifyColumn) {
        SqlColumnDeclaration colDef = sqlModifyColumn.getColDef();
        doValidateCollation(colDef);
    }

    private static void doValidateCollation(SqlColumnDeclaration colDef) {
        SqlDataTypeSpec typeSpec = colDef.getDataType();
        if (typeSpec != null) {
            boolean isSupported = MySQLCharsetDDLValidator
                .checkCharsetSupported(typeSpec.getCharSetName(), typeSpec.getCollationName(), true);
            if (!isSupported) {
                if (typeSpec.getCollationName() == null) {
                    throw GeneralUtil.nestedException(
                        MessageFormat.format("the column {0} with character set {1} is unsupported",
                            colDef.getName().getLastName(), typeSpec.getCharSetName()));
                } else {
                    throw GeneralUtil.nestedException(
                        MessageFormat
                            .format("the column {0} with character set {1} collate {2} is unsupported",
                                colDef.getName().getLastName(), typeSpec.getCharSetName(),
                                typeSpec.getCollationName()));
                }
            }
        }
    }

    public static void validateTableEngine(BaseDdlOperation ddlOperation, ExecutionContext executionContext) {
        if (ddlOperation instanceof LogicalAlterTable) {
            String schemaName = ddlOperation.getSchemaName();
            String logicalTableName = ddlOperation.getTableName();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(logicalTableName);
            if (tableMeta == null) {
                return;
            }
            if (Engine.isFileStore(tableMeta.getEngine())) {
                LogicalAlterTable logicalAlterTable = (LogicalAlterTable) ddlOperation;
                if (logicalAlterTable.isAlterAsOfTimeStamp()
                    || logicalAlterTable.isAlterPurgeBeforeTimeStamp()
                    || logicalAlterTable.isAlterEngine()
                    || logicalAlterTable.isExchangePartition()
                    || logicalAlterTable.isDropFile()) {
                    // support
                } else {
                    throwEngineNotSupport(schemaName, logicalTableName, tableMeta.getEngine());
                }
            }
        } else if (ddlOperation instanceof LogicalDropIndex
            || ddlOperation instanceof LogicalCreateIndex
            || ddlOperation instanceof LogicalAlterTableRepartition
            || ddlOperation instanceof LogicalTruncateTable) {
            String schemaName = ddlOperation.getSchemaName();
            String logicalTableName = ddlOperation.getTableName();
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(logicalTableName);
            if (tableMeta == null) {
                return;
            }
            if (Engine.isFileStore(tableMeta.getEngine())) {
                throwEngineNotSupport(schemaName, logicalTableName, tableMeta.getEngine());
            }
        }
    }
    private static void throwEngineNotSupport(String schemaName, String logicalTableName, Engine engine) {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
            "Engine of " + schemaName + "." + logicalTableName + " is " + engine);
    }
    public static void checkCompatibleWithOss(TableMeta sourceTable, TableMeta targetTable) {
        checkTopologyConsistency(sourceTable, targetTable);
        checkColumnConsistency(sourceTable, targetTable);
    }
    public static void checkTopologyConsistency(TableMeta sourceTable, TableMeta targetTable) {
        boolean isShard = false;
        if (TableTopologyUtil.isBroadcast(sourceTable) != TableTopologyUtil.isBroadcast(targetTable)
            || TableTopologyUtil.isSingle(sourceTable) != TableTopologyUtil.isSingle(targetTable)
            || (isShard = TableTopologyUtil.isShard(sourceTable)) != TableTopologyUtil.isShard(targetTable)
        ) {
            throwTopologyInconsistentError(sourceTable, targetTable);
        }
        if (isShard) {
            PartitionInfo sourcePartitionInfo = OptimizerContext.getContext(sourceTable.getSchemaName())
                .getRuleManager().getPartitionInfoManager().getPartitionInfo(sourceTable.getTableName());
            PartitionInfo targetPartitionInfo = OptimizerContext.getContext(targetTable.getSchemaName())
                .getRuleManager().getPartitionInfoManager().getPartitionInfo(targetTable.getTableName());
            PartitionByDefinition sourceDef = sourcePartitionInfo.getPartitionBy();
            PartitionByDefinition targetDef = targetPartitionInfo.getPartitionBy();
            if (!sourceDef.equals(targetDef)) {
                throwTopologyInconsistentError(sourceTable, targetTable);
            }
        }
    }
    /**
     * Check if source table is enabled to migrated to target table.
     */
    public static void checkColumnConsistency(TableMeta sourceTable, TableMeta targetTable) {
        // check columns
        List<ColumnMeta> sortedSourceColumns = sourceTable.getPhysicalColumns()
            .stream()
            .sorted(Comparator.comparing(ColumnMeta::getOriginColumnName))
            .collect(Collectors.toList());
        List<ColumnMeta> sortedTargetColumns = targetTable.getPhysicalColumns()
            .stream()
            .sorted(Comparator.comparing(ColumnMeta::getOriginColumnName))
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
        throw GeneralUtil.nestedException(
            MessageFormat
                .format("the column metas of source table {0} and target table {1} are not consistent, "
                        + "please create a new archive table for source table {0}", sourceTable.getTableName(),
                    targetTable.getTableName()));
    }
    private static void throwTopologyInconsistentError(TableMeta sourceTable, TableMeta targetTable) {
        throw GeneralUtil.nestedException(
            MessageFormat
                .format("the table topology of source table {0} and target table {1} are not consistent, "
                        + "please create a new archive table for source table {0}", sourceTable.getTableName(),
                    targetTable.getTableName()));
    }

    public static void checkGodPrivilege(ExecutionContext context) {
        if (!context.isPrivilegeMode()) {
            return;
        }

        PrivilegeContext pc = context.getPrivilegeContext();
        PolarAccountInfo user = pc.getPolarUserInfo();
        if (!GOD_USER_NAME.equalsIgnoreCase(user.getAccount().getUsername())) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "Execute this sql in low-privilege account: " + user.getAccount().getUsername());
        }
    }
}
