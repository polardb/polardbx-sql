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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlLiteral;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableValidator {

    public static void validateTableInfo(String schemaName, String logicalTableName, SqlCreateTable sqlCreateTable,
                                         ParamManager paramManager) {
        validateTableName(logicalTableName);

        validateTableNameLength(logicalTableName);

        LimitValidator.validateTableCount(schemaName);

        validateTableComment(logicalTableName, sqlCreateTable.getComment());

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
        validateSystemTables(logicalTableName);
    }

    public static void validateSystemTables(String logicalTableName) {
        if (GmsSystemTables.contains(logicalTableName) && !GmsSystemTables.systemIgnoreTablescontains(logicalTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_SYSTEM_TABLE, logicalTableName);
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

        SequenceValidator.validateSequenceExistence(schemaName, targetTableName);
    }

}
