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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlKind;

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
     * check whether the table group contains local partition table binding to oss table
     *
     * @param preparedData the table group to be tested
     */
    public static void checkWithoutOSS(AlterTableGroupBasePreparedData preparedData) {
        checkTableGroupWithoutOSS(preparedData.getSchemaName(), preparedData.getTableGroupName());
    }

    public static void checkWithoutOSS(AlterTableGroupRenamePartitionPreparedData preparedData) {
        checkTableGroupWithoutOSS(preparedData.getSchemaName(), preparedData.getTableGroupName());
    }

    /**
     * check all tables in the table group has no archive table, based on record in memory
     *
     * @param schema schema of the table group
     * @param tableGroup the table group to be checked
     */
    static void checkTableGroupWithoutOSS(String schema, String tableGroup) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return;
        }
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schema).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "Tablegroup: " + tableGroup + " doesn't exists");
        }
        SchemaManager sm = OptimizerContext.getContext(schema).getLatestSchemaManager();

        // check table one by one
        for (TablePartRecordInfoContext table : tableGroupConfig.getAllTables()) {
            LocalPartitionDefinitionInfo localPartitionDefinitionInfo = sm.getTable(table.getLogTbRec().getTableName())
                .getLocalPartitionDefinitionInfo();
            if (localPartitionDefinitionInfo == null) {
                return;
            }
            if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
                StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
                return;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
        }
    }

    /**
     * check the alter table sql doesn't modify column
     *
     * @param logicalAlterTable the ddl to be executed
     */
    public static void checkWithoutOSS(LogicalAlterTable logicalAlterTable) {
        String schema = logicalAlterTable.getSchemaName();
        String table = logicalAlterTable.getTableName();
        // don't check if the table don't have archive table
        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = OptimizerContext.getContext(schema)
            .getLatestSchemaManager().getTable(table).getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo == null) {
            return;
        }
        if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
            StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
            return;
        }

        for (SqlAlterSpecification alter : ((SqlAlterTable) logicalAlterTable.getNativeSqlNode()).getAlters()) {
            SqlKind kind = alter.getKind();
            //if the sql alters column
            if (kind == SqlKind.ADD_COLUMN
                || kind == SqlKind.DROP_COLUMN
                || kind == SqlKind.MODIFY_COLUMN
                || kind == SqlKind.ALTER_COLUMN_DEFAULT_VAL
                || kind == SqlKind.CHANGE_COLUMN) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
                    "unarchive table " + schema + "." + table);
            }
        }
    }

    /**
     * check the table has no archive table, based on record in memory
     *
     * @param schema schema of the table
     * @param table the table to be tested
     */
    public static void checkWithoutOSS(String schema, String table) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return;
        }
        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(table);
        if (table == null) {
            return;
        }
        LocalPartitionDefinitionInfo localPartitionDefinitionInfo = tableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo == null) {
            return;
        }
        if (StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableSchema()) ||
            StringUtils.isEmpty(localPartitionDefinitionInfo.getArchiveTableName())) {
            return;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive table " + schema + "." + table);
    }

    /**
     * check whether the table has archive table, based on record in GMS
     *
     * @param schema schema of the table
     * @param table the table to be tested
     */
    public static void checkWithoutOSSGMS(String schema, String table) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return;
        }
        try (Connection conn = MetaDbDataSource.getInstance().getDataSource().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(conn);
            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecord(schema, table);
            // not local partition
            if (record == null) {
                return;
            }
            // without archive table
            if (StringUtils.isEmpty(record.getArchiveTableName()) && StringUtils.isEmpty(
                record.getArchiveTableName())) {
                return;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive table " + schema + "." + table);
        } catch (SQLException e) {
            e.printStackTrace();
            throw GeneralUtil.nestedException(e);
        }
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
        TableMeta tableMeta = oc.getLatestSchemaManager().getTable(tableName);
        if (tableMeta == null) {
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
}
