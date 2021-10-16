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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TablesExtAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TablesExtAccessor.class);

    private static final String TABLES_EXT_TABLE = wrap(GmsSystemTables.TABLES_EXT);

    private static final String INSERT_TABLES_EXT =
        "insert into " + TABLES_EXT_TABLE
            + "(`table_schema`, `table_name`, `new_table_name`, `table_type`, `db_partition_key`, "
            + "`db_partition_policy`, `db_partition_count`, `db_name_pattern`, `db_rule`, `db_meta_map`, "
            + "`tb_partition_key`, `tb_partition_policy`, `tb_partition_count`, `tb_name_pattern`, `tb_rule`, "
            + "`tb_meta_map`, `ext_partitions`, `full_table_scan`, `broadcast`, `version`, `status`, `flag`) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_SCHEMA = " where `table_schema` = ?";

    private static final String WHERE_SCHEMA_TABLE = WHERE_SCHEMA + " and `table_name` = ?";

    private static final String WHERE_SCHEMA_NEW_TABLE = WHERE_SCHEMA + " and `new_table_name` = ?";

    private static final String SELECT_TABLES_EXT =
        "select `table_schema`, `table_name`, `new_table_name`, `table_type`, `db_partition_key`, "
            + "`db_partition_policy`, `db_partition_count`, `db_name_pattern`, `db_rule`, `db_meta_map`, "
            + "`tb_partition_key`, `tb_partition_policy`, `tb_partition_count`, `tb_name_pattern`, `tb_rule`, "
            + "`tb_meta_map`, `ext_partitions`, `full_table_scan`, `broadcast`, `version`, `status`, `flag` from "
            + TABLES_EXT_TABLE;

    private static final String SELECT_TABLES_EXT_ALL = SELECT_TABLES_EXT + WHERE_SCHEMA;

    private static final String SELECT_TABLES_EXT_ONE = SELECT_TABLES_EXT + WHERE_SCHEMA_TABLE;

    private static final String SELECT_TABLES_EXT_NEW = SELECT_TABLES_EXT + WHERE_SCHEMA_NEW_TABLE;

    private static final String SELECT_TABLES_EXT_COUNT = "select count(*) from " + TABLES_EXT_TABLE + WHERE_SCHEMA;

    private static final String UPDATE_TABLES_EXT = "update " + TABLES_EXT_TABLE + " set ";

    private static final String UPDATE_TABLES_EXT_VERSION =
        UPDATE_TABLES_EXT + "`version` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_STATUS =
        UPDATE_TABLES_EXT + "`status` = ?, `version` = `version` + 1" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_NEW_NAME =
        UPDATE_TABLES_EXT + "`new_table_name` = ?, `version` = `version` + 1" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_RENAME =
        UPDATE_TABLES_EXT + "`table_name` = ?, `new_table_name` = '', tb_name_pattern = ?, `version` = `version` + 1"
            + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_PROPS =
        UPDATE_TABLES_EXT
            + "`full_table_scan` = ?, `broadcast` = ?, `ext_partitions` = ?, `flag` = ?, `version` = `version` + 1"
            + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_FLAG =
        UPDATE_TABLES_EXT + " `flag` = ? , `version` = `version` + 1"
            + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_SWITCH_NAME =
        UPDATE_TABLES_EXT + "`table_name` = ?, `version` = `version` + 1" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_TABLE_TYPE =
        UPDATE_TABLES_EXT + "`table_type` = ?, `version` = `version` + 1" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_EXT_SWITCH_NAME_TYPE_FLAG =
        UPDATE_TABLES_EXT + "`table_name` = ?, `table_type` = ?, `flag` = ?, `version` = `version` + 1"
            + WHERE_SCHEMA_TABLE;

    private static final String DELETE_TABLES_EXT = "delete from " + TABLES_EXT_TABLE + WHERE_SCHEMA_TABLE;

    private static final String DELETE_TABLES_EXT_ALL = "delete from " + TABLES_EXT_TABLE + WHERE_SCHEMA;

    public int insert(TablesExtRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_TABLES_EXT, record.buildParams());
            return MetaDbUtil.insert(INSERT_TABLES_EXT, record.buildParams(), connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                TablesExtRecord existingRecord = query(record.tableSchema, record.tableName, false);
                if (compare(record, existingRecord)) {
                    // Ignore new and use existing record.
                    return 1;
                } else {
                    extraMsg = ". New and existing records don't match";
                }
            }
            LOGGER.error("Failed to insert a new record into " + TABLES_EXT_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(TablesExtRecord newRecord, TablesExtRecord existingRecord) {
        if (newRecord != null && existingRecord != null) {
            return newRecord.tableType == existingRecord.tableType &&
                TStringUtil.equalsIgnoreCase(newRecord.dbPartitionKey, existingRecord.dbPartitionKey) &&
                TStringUtil.equalsIgnoreCase(newRecord.dbPartitionPolicy, existingRecord.dbPartitionPolicy) &&
                newRecord.dbPartitionCount == existingRecord.dbPartitionCount &&
                TStringUtil.equalsIgnoreCase(newRecord.dbNamePattern, existingRecord.dbNamePattern) &&
                TStringUtil.equalsIgnoreCase(newRecord.dbRule, existingRecord.dbRule) &&
                TStringUtil.equalsIgnoreCase(newRecord.dbMetaMap, existingRecord.dbMetaMap) &&
                TStringUtil.equalsIgnoreCase(newRecord.tbPartitionKey, existingRecord.tbPartitionKey) &&
                TStringUtil.equalsIgnoreCase(newRecord.tbPartitionPolicy, existingRecord.tbPartitionPolicy) &&
                newRecord.tbPartitionCount == existingRecord.tbPartitionCount &&
                TStringUtil.equalsIgnoreCase(newRecord.tbNamePattern, existingRecord.tbNamePattern) &&
                TStringUtil.equalsIgnoreCase(newRecord.tbRule, existingRecord.tbRule) &&
                TStringUtil.equalsIgnoreCase(newRecord.tbMetaMap, existingRecord.tbMetaMap) &&
                TStringUtil.equalsIgnoreCase(newRecord.extPartitions, existingRecord.extPartitions) &&
                newRecord.broadcast == existingRecord.broadcast &&
                newRecord.fullTableScan == existingRecord.fullTableScan;
        }
        return false;
    }

    public List<TablesExtRecord> query(String tableSchema) {
        return query(SELECT_TABLES_EXT_ALL, TABLES_EXT_TABLE, TablesExtRecord.class, tableSchema);
    }

    public TablesExtRecord query(String tableSchema, String tableName, boolean isNewTableForRename) {
        String selectSql = isNewTableForRename ? SELECT_TABLES_EXT_NEW : SELECT_TABLES_EXT_ONE;
        List<TablesExtRecord> records =
            query(selectSql, TABLES_EXT_TABLE, TablesExtRecord.class, tableSchema, tableName);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public int count(String tableSchema) {
        List<CountRecord> records = query(SELECT_TABLES_EXT_COUNT, TABLES_EXT_TABLE, CountRecord.class, tableSchema);
        if (records != null && records.size() > 0) {
            return records.get(0).count;
        }
        return 0;
    }

    public int updateVersion(String tableSchema, String tableName, long newOpVersion) {
        return update(UPDATE_TABLES_EXT_VERSION, TABLES_EXT_TABLE, tableSchema, tableName, newOpVersion);
    }

    public int updateStatus(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_TABLES_EXT_STATUS, TABLES_EXT_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateNewName(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_TABLES_EXT_NEW_NAME, TABLES_EXT_TABLE, tableSchema, tableName, newTableName);
    }

    public int rename(String tableSchema, String tableName, String newTableName, String newTbNamePattern) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {newTableName, newTbNamePattern, tableSchema, tableName});
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_RENAME, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_RENAME, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update " + TABLES_EXT_TABLE + " with new table name " + newTableName + " and pattern "
                    + newTbNamePattern + " for table " + wrap(tableName), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "rename", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int alter(TablesExtRecord record) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, record.fullTableScan);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, record.broadcast);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, record.extPartitions);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, record.flag);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, record.tableSchema);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, record.tableName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_PROPS, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_PROPS, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update " + TABLES_EXT_TABLE + " with full table scan = " + record.fullTableScan
                    + ", broadcast = " + record.broadcast + " and extPartitions = " + record.extPartitions
                    + " for table " + wrap(record.tableName), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int alterNameAndTypeAndFlag(
        String tableSchema,
        String originName,
        String newName,
        int tableType,
        long flag) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(12);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, tableType);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, flag);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, originName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_SWITCH_NAME_TYPE_FLAG, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_SWITCH_NAME_TYPE_FLAG, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int alterTableExtFlag(
        String tableSchema,
        String tableName,
        long flag) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(12);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, flag);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_FLAG, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_FLAG, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int alterTableType(String tableSchema, String tableName, int tableType) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, tableType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_TABLE_TYPE, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_TABLE_TYPE, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int alterName(String tableSchema, String originName, String newName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, originName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_EXT_SWITCH_NAME, params);
            return MetaDbUtil.update(UPDATE_TABLES_EXT_SWITCH_NAME, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", TABLES_EXT_TABLE,
                e.getMessage());
        }
    }

    public int delete(String tableSchema, String tableName) {
        return delete(DELETE_TABLES_EXT, TABLES_EXT_TABLE, tableSchema, tableName);
    }

    public int delete(String tableSchema) {
        return delete(DELETE_TABLES_EXT_ALL, TABLES_EXT_TABLE, tableSchema);
    }

}
