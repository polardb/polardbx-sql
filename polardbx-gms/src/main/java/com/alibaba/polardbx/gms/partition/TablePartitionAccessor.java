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

package com.alibaba.polardbx.gms.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handle all the config change in metadb of partition tables
 *
 * @author chenghui.lch
 */
public class TablePartitionAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(TablePartitionAccessor.class);

    private static final String ALL_COLUMNS =
        "`id`,`parent_id`,`create_time`,`update_time`,`table_schema`,`table_name`,`sp_temp_flag`,`group_id`,`meta_version`,`auto_flag`,`tbl_type`,`part_name`,`part_temp_name`,`part_level`,`next_level`,`part_status`,`part_position`,`part_method`,`part_expr`,`part_desc`,`part_comment`,`part_engine`,`part_extras`,`part_flags`,`phy_table`";

    private static final String ALL_VALUES = "(null,?,null,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_IGNORE_TABLE_PARTITIONS =
        "insert ignore into table_partitions (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String UPSERT_TABLE_PARTITIONS =
        "insert ignore into table_partitions (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES +
            " on duplicate key update group_id = ?, part_position = ?, part_desc=?, phy_table=? ";

    private static final String GET_TABLE_PARTITIONS_BY_DB =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? order by table_name, part_level, part_position asc";

    private static final String GET_TABLE_PARTITIONS_BY_DB_TB =
        "select " + ALL_COLUMNS + " from table_partitions where table_schema=? and table_name=?";

    private static final String GET_TABLE_PARTITIONS_BY_DB_TB_PT =
        "select " + ALL_COLUMNS + " from table_partitions where table_schema=? and table_name=? and part_name = ?";

    private static final String GET_TABLE_PARTITIONS_BY_DB_TB_LEVEL =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and table_name=? and part_level=? order by parent_id, part_position asc";

    private static final String GET_TABLE_PARTITIONS_BY_DB_LEVEL =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and part_level=? order by parent_id, part_position asc";

    private static final String GET_PUBLIC_TABLE_PARTITIONS_BY_DB_TB_LEVEL =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and table_name=? and part_level=? and part_status=1 order by parent_id, part_position asc";

    private static final String DELETE_TABLE_PARTITIONS_BY_TABLE_NAME =
        "delete from table_partitions where table_schema=? and table_name=?";

    private static final String DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME =
        "delete from table_partitions where table_schema=?";

    private static final String UPDATE_STATUS_FOR_LOGICAL_TABLE =
        "update table_partitions set part_status=? where table_schema=? and table_name=? and part_level=0";

    private static final String UPDATE_PART_BOUND_DESC_FOR_ONE_PARTITION =
        "update table_partitions set part_desc=? where table_schema=? and table_name=? and part_name=?";

    private static final String UPDATE_META_VERSION_FOR_LOGICAL_TABLE =
        "update table_partitions set meta_version=? where part_level=0 and table_schema=? and table_name=? ";

    private static final String UPDATE_TABLE_PARTITIONS_SWITCH_NAME_TYPE =
        "update table_partitions set table_name=? , tbl_type = ? where table_schema=? and table_name=? ";

    private static final String DELETE_TABLE_PARTITIONS_BY_TABLE_AND_PARTITION =
        "delete from table_partitions where (table_schema=? and table_name=? and part_name=?) "
            + " or parent_id = ?";

    private static final String DELETE_TABLE_PARTITIONS_BY_ID =
        "delete from table_partitions where id=?";

    private static final String UPDATE_TABLE_PARTITIONS_ADD_SHARD_COLUMNS =
        "update table_partitions set part_expr=? , part_desc = ? where id = ? ";

    private static final String UPDATE_TABLE_PARTITIONS_CHANGE_GROUP_ID =
        "update table_partitions set group_id =? where id = ? ";

    private static final String GET_SUBPARTITIONS_BY_SCHEMA_NAME_GROUP_ID =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and part_level<>0 and (group_id=? or parent_id in (select id from table_partitions where part_level<>0 and group_id=?))";

    private static final String RENEW_GROUP_ID = "update table_partitions set group_id = ? where group_id = ?";

    private static final String RENEW_GROUP_ID_BY_ID = "update table_partitions set group_id = ? where id = ?";

    private static final String GET_TABLE_PARTITIONS_BY_DB_GROUP_ID =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and group_id=? and part_level=0 and part_status=1 order by parent_id, part_position asc";

    private static final String GET_ALL_TABLE_PARTITIONS_BY_DB_GROUP_ID =
        "select " + ALL_COLUMNS
            + " from table_partitions where parent_id in (select id from table_partitions where table_schema=? and group_id=? and part_level=0 and part_status=1)"
            + " union all select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and group_id=? and part_level=0 and part_status=1";

    private static final String GET_TABLE_PARTITIONS_BY_DB_PART_GROUP_ID =
        "select " + ALL_COLUMNS
            + " from table_partitions where table_schema=? and group_id=? and part_level<>0 order by parent_id, part_position asc";

    // for TABLE_PARTITIONS_DELTA
    private static final String UPSERT_TABLE_PARTITIONS_TO_DELTA_TABLE =
        "insert ignore into " + GmsSystemTables.TABLE_PARTITIONS_DELTA + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES +
            " on duplicate key update group_id = ?, part_position = ?, part_desc=?, phy_table=? ";

    private static final String INSERT_IGNORE_TABLE_PARTITIONS_TO_DELTA_TABLE =
        "insert ignore into " + GmsSystemTables.TABLE_PARTITIONS_DELTA + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String GET_TABLE_PARTITIONS_BY_DB_TB_LEVEL_FROM_DELTA_TABLE =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.TABLE_PARTITIONS_DELTA
            + " where table_schema=? and table_name=? and part_level=? order by parent_id, part_position asc";

    private static final String DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME_FROM_DELTA_TABLE =
        "delete from " + GmsSystemTables.TABLE_PARTITIONS_DELTA + " where table_schema=?";

    private static final String DELETE_TABLE_PARTITIONS_BY_TABLE_NAME_FROM_DELTA_TABLE =
        "delete from " + GmsSystemTables.TABLE_PARTITIONS_DELTA + " where table_schema=? and table_name=?";

    private static final String UPDATE_PARTITION_NAME_BY_GROUPID =
        "update " + GmsSystemTables.TABLE_PARTITIONS + " set part_name=? where group_id=? and part_name=?";

    private static final String UPDATE_TABLES_RENAME =
        "update table_partitions set `table_name` = ? where table_schema=? and table_name=? ";

    private static final String DELETE_TABLE_PARTITIONS_BY_GID_AND_PART_FROM_DELTA_TABLE =
        "delete from " + GmsSystemTables.TABLE_PARTITIONS_DELTA
            + " where group_id=? and part_name=? and part_level<>0";

    public List<TablePartitionRecord> getTablePartitionsByDbNameTbNameLevel(String dbName, String tbName, int level,
                                                                            boolean from_delta_table) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, level);
            records =
                MetaDbUtil.query(from_delta_table ? GET_TABLE_PARTITIONS_BY_DB_TB_LEVEL_FROM_DELTA_TABLE :
                    GET_TABLE_PARTITIONS_BY_DB_TB_LEVEL, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getTablePartitionsByDbNameLevel(String dbName, int level) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, level);
            records =
                MetaDbUtil.query(GET_TABLE_PARTITIONS_BY_DB_LEVEL, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getPublicTablePartitionsByDbNameTbNameLevel(String dbName, String tbName,
                                                                                  int level) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, level);
            records =
                MetaDbUtil
                    .query(GET_PUBLIC_TABLE_PARTITIONS_BY_DB_TB_LEVEL, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getTablePartitionsByDbNameGroupId(String dbName, Long groupId) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);
            records =
                MetaDbUtil.query(GET_TABLE_PARTITIONS_BY_DB_GROUP_ID, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getAllTablePartitionsByDbNameGroupId(String dbName, Long groupId) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, groupId);
            records =
                MetaDbUtil
                    .query(GET_ALL_TABLE_PARTITIONS_BY_DB_GROUP_ID, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getTablePartitionsByDbNamePartGroupId(String dbName, Long partGroupId) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, partGroupId);
            records =
                MetaDbUtil
                    .query(GET_TABLE_PARTITIONS_BY_DB_PART_GROUP_ID, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getTablePartitionsByDbNameTbName(String dbName, String tbName,
                                                                       boolean fromDelta) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);

            if (tbName == null) {
                records = MetaDbUtil.query(GET_TABLE_PARTITIONS_BY_DB, params, TablePartitionRecord.class, connection);
            } else {
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
                records =
                    MetaDbUtil.query(GET_TABLE_PARTITIONS_BY_DB_TB, params, TablePartitionRecord.class, connection);
            }

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TablePartitionRecord> getTablePartitionsByDbNameTbNamePtName(String dbName, String tbName,
                                                                             String ptName) {
        try {

            List<TablePartitionRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, ptName);

            records =
                MetaDbUtil.query(GET_TABLE_PARTITIONS_BY_DB_TB_PT, params, TablePartitionRecord.class, connection);

            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateGroupId(Long oldGroupId, Long newGroupId) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, newGroupId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, oldGroupId);

            DdlMetaLogUtil.logSql(RENEW_GROUP_ID, params);

            MetaDbUtil.update(RENEW_GROUP_ID, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateGroupIdById(Long newGroupId, Long id) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, newGroupId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(RENEW_GROUP_ID_BY_ID, params);

            MetaDbUtil.update(RENEW_GROUP_ID_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateStatusForPartitionedTable(String dbName, String tbName, Integer newStatus) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tbName);

            DdlMetaLogUtil.logSql(UPDATE_STATUS_FOR_LOGICAL_TABLE, params);

            MetaDbUtil.update(UPDATE_STATUS_FOR_LOGICAL_TABLE, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePartBoundDescForOnePartition(String dbName, String tbName, String partName, String partBndDesc) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, partBndDesc);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tbName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, partName);

            DdlMetaLogUtil.logSql(UPDATE_PART_BOUND_DESC_FOR_ONE_PARTITION, params);

            MetaDbUtil.update(UPDATE_PART_BOUND_DESC_FOR_ONE_PARTITION, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePartitionNameByGroupId(Long groupId, String oldPartitionName, String newPartitionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newPartitionName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, oldPartitionName);

            DdlMetaLogUtil.logSql(UPDATE_PARTITION_NAME_BY_GROUPID, params);

            MetaDbUtil.update(UPDATE_PARTITION_NAME_BY_GROUPID, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateVersion(String dbName, String tbName, long newOpVersion) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, newOpVersion);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tbName);

            DdlMetaLogUtil.logSql(UPDATE_META_VERSION_FOR_LOGICAL_TABLE, params);

            MetaDbUtil.update(UPDATE_META_VERSION_FOR_LOGICAL_TABLE, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void alterNameAndType(String dbName, String tbName, String newTbName, int tbType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, tbType);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tbName);
            DdlMetaLogUtil.logSql(UPDATE_TABLE_PARTITIONS_SWITCH_NAME_TYPE, params);
            MetaDbUtil.update(UPDATE_TABLE_PARTITIONS_SWITCH_NAME_TYPE, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void addColumnForPartExprAndPartDesc(String dbName, String tbName, List<String> changeColumns) {
        // todo: support subpartition wumu
        List<TablePartitionRecord> partitionRecords = getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
            TablePartitionRecord.PARTITION_LEVEL_PARTITION, false);
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();

            for (TablePartitionRecord partitionRecord : partitionRecords) {
                Map<Integer, ParameterContext> params = new HashMap<>();
                List<String> partDescList = new ArrayList<>();

                // build new partExpr
                String partExpr = StringUtils.join(changeColumns, ",");

                // build new partDesc
                String[] partDescArray = partitionRecord.getPartDesc().split(",");

                partDescList.add(partDescArray[0]);
                for (int i = 0; i < changeColumns.size() - 1; ++i) {
                    partDescList.add(String.valueOf(Long.MAX_VALUE));
                }
                String partDesc = StringUtils.join(partDescList, ",");

                int index = 1;
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partExpr);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partDesc);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partitionRecord.id);
                paramsBatch.add(params);
            }
            MetaDbUtil.update(UPDATE_TABLE_PARTITIONS_ADD_SHARD_COLUMNS, paramsBatch, this.connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to update the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateTablePartitionsGroupInfo(String dbName, String tbName,
                                               TablePartitionRecord logicalTableRecord,
                                               List<TablePartitionRecord> partitionRecords,
                                               Map<String, List<TablePartitionRecord>> subPartitionInfos) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();

            // get record id of logical table
            List<TablePartitionRecord> results =
                getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
                    TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, false);
            assert results.size() == 1;
            TablePartitionRecord logTbRec = results.get(0);

            Map<Integer, ParameterContext> param = new HashMap<>();
            MetaDbUtil.setParameter(1, param, ParameterMethod.setLong, logicalTableRecord.groupId);
            MetaDbUtil.setParameter(2, param, ParameterMethod.setLong, logTbRec.id);
            paramsBatch.add(param);

            // get records of partitions
            List<TablePartitionRecord> partRecList = getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
                TablePartitionRecord.PARTITION_LEVEL_PARTITION, false);
            assert partRecList.size() == partitionRecords.size();
            for (int i = 0; i < partRecList.size(); i++) {
                TablePartitionRecord partRec = partRecList.get(i);
                TablePartitionRecord logicalPartRec = partitionRecords.get(i);
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, logicalPartRec.groupId);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, partRec.id);
                paramsBatch.add(params);
            }

            // todo: support subpartition wumu

            MetaDbUtil.update(
                UPDATE_TABLE_PARTITIONS_CHANGE_GROUP_ID, paramsBatch,
                this.connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to update the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteTablePartitionConfigs(String dbName, String tbName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            if (tbName == null) {

                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME, params, connection);

                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME_FROM_DELTA_TABLE, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME_FROM_DELTA_TABLE, params, connection);
            } else {
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);

                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME, params, connection);

                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME_FROM_DELTA_TABLE, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME_FROM_DELTA_TABLE, params, connection);
            }
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteTablePartitionConfigsForDeltaTable(String dbName, String tbName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            assert dbName != null;
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            if (tbName == null) {
                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME_FROM_DELTA_TABLE, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_SCHEMA_NAME_FROM_DELTA_TABLE, params, connection);
            } else {
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
                DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME_FROM_DELTA_TABLE, params);

                MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_TABLE_NAME_FROM_DELTA_TABLE, params, connection);
            }
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteTablePartitionsById(Long id) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_ID, params);

            MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deletePartitionConfigs(String dbName, String tbName, String ptName) {
        try {

            List<TablePartitionRecord> tablePartitionRecords =
                getTablePartitionsByDbNameTbNamePtName(dbName, tbName, ptName);
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, ptName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, tablePartitionRecords.get(0).id);

            DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_TABLE_AND_PARTITION, params);

            MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_TABLE_AND_PARTITION, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteTablePartitionConfigsByDbName(String dbName) {
        deleteTablePartitionConfigs(dbName, null);
    }

    public void addNewTablePartitionConfigs(TablePartitionRecord logicalTableRecord,
                                            List<TablePartitionRecord> partitionRecords,
                                            Map<String, List<TablePartitionRecord>> subPartitionInfos,
                                            boolean isUpsert, boolean toDeltaTable) {
        try {
            // insert record for logical table
            List<TablePartitionRecord> records = new ArrayList<>();
            records.add(logicalTableRecord);

            addNewTablePartitions(records, isUpsert, toDeltaTable);

            String dbName = logicalTableRecord.tableSchema;
            String tbName = logicalTableRecord.tableName;

            // get record id of logical table
            List<TablePartitionRecord> results =
                getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
                    TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, toDeltaTable);
            assert results.size() == 1;
            TablePartitionRecord logTbRec = results.get(0);

            for (int i = 0; i < partitionRecords.size(); i++) {
                TablePartitionRecord partRec = partitionRecords.get(i);
                partRec.parentId = logTbRec.id;
                /*update the position incase there is some duplicate position
                for case:
                p1 less than 10: pos=1
                p2 less than 20: pos=2
                p3 less than 30: pos=3
                p4 less than 40: pos=4
                drop partition p3, and then add p5 less than 50, now p5's position is 4
                we need update the p4's position here
                */
                partRec.partPosition = Long.valueOf(i + 1);
            }

            // insert records for all partitions
            records = partitionRecords;
            addNewTablePartitions(records, isUpsert, toDeltaTable);

            // get records of partitions
            results =
                getTablePartitionsByDbNameTbNameLevel(dbName, tbName, TablePartitionRecord.PARTITION_LEVEL_PARTITION,
                    toDeltaTable);
            List<TablePartitionRecord> partRecList = results;
            Map<String, Long> partNameIdMap = new HashMap<>();
            for (int i = 0; i < partRecList.size(); i++) {
                TablePartitionRecord partRec = partRecList.get(i);
                partNameIdMap.put(partRec.partName, partRec.id);
            }

            // insert records for all subpartitions
            for (Map.Entry<String, List<TablePartitionRecord>> subPartInfoItem : subPartitionInfos.entrySet()) {
                String partName = subPartInfoItem.getKey();
                List<TablePartitionRecord> subPartList = subPartInfoItem.getValue();
                Long parentId = partNameIdMap.get(partName);
                for (int i = 0; i < subPartList.size(); i++) {
                    TablePartitionRecord subPart = subPartList.get(i);
                    subPart.parentId = parentId;
                }
                addNewTablePartitions(subPartList, isUpsert, toDeltaTable);
            }
        } catch (Throwable e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }

    }

    public int[] addNewTablePartitions(List<TablePartitionRecord> tablePartitionsRecordList, boolean isUpsert,
                                       boolean toDeltaTable)
        throws SQLException {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
        for (int i = 0; i < tablePartitionsRecordList.size(); i++) {
            TablePartitionRecord tpRecord = tablePartitionsRecordList.get(i);
            Map<Integer, ParameterContext> params = new HashMap<>();

            int j = 1;
            //-----id and time------
            // id
            //MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, null);
            // parent_id
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.parentId);

            // create_time
            //MetaDbUtil.setParameter(3, params, ParameterMethod.setTimestamp1, null);
            // update_time
            //MetaDbUtil.setParameter(4, params, ParameterMethod.setTimestamp1, null);

            //-----logical table------
            // table_schema
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.tableSchema);
            // table_name
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.tableName);
            // sp_temp_flag
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.spTempFlag);
            // table_group_id
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.groupId);
            // meta_version
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.metaVersion);
            // auto_flag
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.autoFlag);
            // tbl_type
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.tblType);

            //-----partition info------
            // partName
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partName);
            // partTempName
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partTempName);
            // part_level
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.partLevel);
            // next_level
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.nextLevel);
            // part_status
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setInt, tpRecord.partStatus);
            // part_position
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.partPosition);
            // part_method
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partMethod);
            // part_expr
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partExpr);
            // part_desc
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partDesc);
            // part_comment
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partComment);
            // part_engine
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partEngine);
            // part_extras
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString,
                ExtraFieldJSON.toJson(tpRecord.partExtras));
            // part_flags
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.partFlags);

            //-----location------
            MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.phyTable);

            if (isUpsert) {
                MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.groupId);
                MetaDbUtil.setParameter(j++, params, ParameterMethod.setLong, tpRecord.partPosition);
                MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.partDesc);
                MetaDbUtil.setParameter(j++, params, ParameterMethod.setString, tpRecord.phyTable);
            }

            paramsBatch.add(params);
        }

        String sql = isUpsert ? UPSERT_TABLE_PARTITIONS : INSERT_IGNORE_TABLE_PARTITIONS;
        if (toDeltaTable) {
            sql = isUpsert ? UPSERT_TABLE_PARTITIONS_TO_DELTA_TABLE : INSERT_IGNORE_TABLE_PARTITIONS_TO_DELTA_TABLE;
        }

        DdlMetaLogUtil.logSql(sql, paramsBatch);

        int[] ret = MetaDbUtil.insert(sql, paramsBatch, this.connection);
        return ret;
    }

    /**
     * Get partition info for table with dbName and tbName
     */
    public TablePartitionConfig getTablePartitionConfig(String dbName, String tbName, boolean fromDeltaTable) {
        TablePartitionConfig partitionConfig = null;
        partitionConfig = getTablePartitionConfigInner(dbName, tbName, this.connection, fromDeltaTable, false);
        return partitionConfig;
    }

    public TablePartitionConfig getPublicTablePartitionConfig(String dbName, String tbName) {
        TablePartitionConfig partitionConfig = null;
        partitionConfig = getTablePartitionConfigInner(dbName, tbName, this.connection, false, true);
        return partitionConfig;
    }

    /**
     * Get all partition infos for db with dbName
     */
    public List<TablePartitionConfig> getAllTablePartitionConfigs(String dbName) {
        List<TablePartitionConfig> partitionConfigList = null;
        partitionConfigList = getAllTablePartitionConfigsInner(dbName, this.connection);
        return partitionConfigList;
    }

    public List<TablePartitionConfig> getAllTablePartitionConfigsInner(String dbName, Connection metaDbConn) {

        List<TablePartitionConfig> partitionConfigArr = new ArrayList<>();
        List<TablePartitionRecord> records = getTablePartitionsByDbNameTbName(dbName, null, false);
        Map<String, TablePartRecordInfoContext> tbPartRecInfoMap = new HashMap<>();
        for (int i = 0; i < records.size(); i++) {
            TablePartitionRecord partRec = records.get(i);
            String tbName = partRec.tableName;
            TablePartRecordInfoContext confCtx = tbPartRecInfoMap.get(tbName);
            if (confCtx == null) {
                confCtx = new TablePartRecordInfoContext();
                tbPartRecInfoMap.put(tbName, confCtx);
            }
            int level = partRec.partLevel;
            switch (level) {
            case TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE: {
                confCtx.setLogTbRec(partRec);
            }
            break;
            case TablePartitionRecord.PARTITION_LEVEL_PARTITION: {
                confCtx.getPartitionRecList().add(partRec);
            }
            break;
            case TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION: {
                confCtx.getSubPartitionRecList().add(partRec);
            }
            break;
            default: {

            }
            break;
            }
        }

        for (Map.Entry<String, TablePartRecordInfoContext> configCtxItem : tbPartRecInfoMap.entrySet()) {
            TablePartRecordInfoContext ctx = configCtxItem.getValue();
            TablePartitionConfig config = buildPartitionConfByPartitionRecords(ctx);
            partitionConfigArr.add(config);
        }
        return partitionConfigArr;
    }

    public List<TablePartRecordInfoContext> getAllTablePartRecordInfoContextsByGroupId(String dbName, Long groupId,
                                                                                       Connection metaDbConn) {

        List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
        List<TablePartitionRecord> tablePartitionRecords = getAllTablePartitionsByDbNameGroupId(dbName, groupId);
        Map<String, TablePartRecordInfoContext> allTablePartInfoCotexts = new HashMap<>();
        for (TablePartitionRecord tablePartitionRecord : tablePartitionRecords) {
            allTablePartInfoCotexts
                .computeIfAbsent(tablePartitionRecord.getTableName(), o -> new TablePartRecordInfoContext());
            switch (tablePartitionRecord.getPartLevel()) {
            case TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE:
                allTablePartInfoCotexts.get(tablePartitionRecord.getTableName()).setLogTbRec(tablePartitionRecord);
                break;
            case TablePartitionRecord.PARTITION_LEVEL_PARTITION:
                allTablePartInfoCotexts.get(tablePartitionRecord.getTableName()).getPartitionRecList()
                    .add(tablePartitionRecord);
                break;
            case TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION:
                allTablePartInfoCotexts.get(tablePartitionRecord.getTableName()).getSubPartitionRecList()
                    .add(tablePartitionRecord);
                break;
            }
        }
        for (Map.Entry<String, TablePartRecordInfoContext> entry : allTablePartInfoCotexts.entrySet()) {
            Collections.sort(entry.getValue().getPartitionRecList(),
                (o1, o2) -> o1.getPartPosition().compareTo(o2.getPartPosition()));

            Collections.sort(entry.getValue().getSubPartitionRecList(),
                (o1, o2) -> o1.getPartPosition().compareTo(o2.getPartPosition()));
            tablePartRecordInfoContexts.add(entry.getValue());

        }
        return tablePartRecordInfoContexts;
    }

    public TablePartRecordInfoContext getTablePartRecordInfoContextsByDbNameAndTableName(String dbName,
                                                                                         String tableName) {

        TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
        List<TablePartitionRecord> tablePartitionRecords = getTablePartitionsByDbNameTbName(dbName, tableName, false);
        Map<String, TablePartRecordInfoContext> allTablePartInfoCotexts = new HashMap<>();
        for (TablePartitionRecord tablePartitionRecord : tablePartitionRecords) {
            switch (tablePartitionRecord.getPartLevel()) {
            case TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE:
                tablePartRecordInfoContext.setLogTbRec(tablePartitionRecord);
                break;
            case TablePartitionRecord.PARTITION_LEVEL_PARTITION:
                tablePartRecordInfoContext.getPartitionRecList()
                    .add(tablePartitionRecord);
                break;
            case TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION:
                tablePartRecordInfoContext.getSubPartitionRecList()
                    .add(tablePartitionRecord);
                break;
            }
        }
        return tablePartRecordInfoContext;
    }

    public void rename(String tableSchema, String tableName, String newTableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTableName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            MetaDbUtil.update(UPDATE_TABLES_RENAME, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    private TablePartitionConfig getTablePartitionConfigInner(String dbName, String tbName, Connection metaDbConn,
                                                              boolean fromDeltaTable, boolean publicOnly) {

        TablePartitionConfig partitionConfig = null;

        // Fetch logical config
        TablePartitionRecord logTbRec = null;
        List<TablePartitionRecord> result = null;
        result = !publicOnly ? getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
            TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, fromDeltaTable) :
            getPublicTablePartitionsByDbNameTbNameLevel(dbName, tbName,
                TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
        if (result.size() == 0) {
            return null;
        }
        logTbRec = result.get(0);

        // Fetch partition configs
        List<TablePartitionRecord> partitionRecList = null;
        partitionRecList =
            getTablePartitionsByDbNameTbNameLevel(dbName, tbName, TablePartitionRecord.PARTITION_LEVEL_PARTITION,
                fromDeltaTable);

        // Fetch subpartition configs
        List<TablePartitionRecord> subPartitionRecList = null;
        subPartitionRecList = getTablePartitionsByDbNameTbNameLevel(dbName, tbName,
            TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION, fromDeltaTable);

        TablePartRecordInfoContext confCtx = new TablePartRecordInfoContext();
        confCtx.setLogTbRec(logTbRec);
        confCtx.setPartitionRecList(partitionRecList);
        confCtx.setSubPartitionRecList(subPartitionRecList);
        partitionConfig = buildPartitionConfByPartitionRecords(confCtx);

        return partitionConfig;
    }

    public static TablePartitionConfig buildPartitionConfByPartitionRecords(TablePartRecordInfoContext confContext) {

        TablePartitionRecord logTbRec = confContext.getLogTbRec();
        List<TablePartitionRecord> partitionRecList = confContext.getPartitionRecList();
        List<TablePartitionRecord> subPartitionRecList = confContext.getSubPartitionRecList();

        TablePartitionConfig partitionConfig = new TablePartitionConfig();

        // set config for logical table
        partitionConfig.tableConfig = logTbRec;

        // set config for logical table
        List<TablePartitionSpecConfig> partSpecConfList = new ArrayList<>(partitionRecList.size());
        Map<Long, List<TablePartitionSpecConfig>> subTbPartRecMap = new HashMap<>();
        for (int i = 0; i < subPartitionRecList.size(); i++) {
            TablePartitionRecord subTbPartRec = subPartitionRecList.get(i);
            Long parentId = subTbPartRec.parentId;
            List<TablePartitionSpecConfig> subPartConfList = subTbPartRecMap.get(parentId);
            if (subPartConfList == null) {
                subPartConfList = new ArrayList<>();
                subTbPartRecMap.put(parentId, subPartConfList);
            }
            TablePartitionSpecConfig subPartSpecConf = new TablePartitionSpecConfig();
            subPartSpecConf.specConfigInfo = subTbPartRec;
            subPartConfList.add(subPartSpecConf);
        }

        // set configs of subpartitions for each partition
        for (int i = 0; i < partitionRecList.size(); i++) {
            TablePartitionRecord partSpecRec = partitionRecList.get(i);
            TablePartitionSpecConfig partSpecConf = new TablePartitionSpecConfig();
            partSpecConf.specConfigInfo = partSpecRec;
            if (partSpecRec.partLevel == TablePartitionRecord.PARTITION_LEVEL_NO_SUBPARTITION) {
                partSpecConf.subPartitionSpecConfigs = new ArrayList<>();
            } else {
                Long partId = partSpecRec.id;
                List<TablePartitionSpecConfig> subPartConfList = subTbPartRecMap.get(partId);
                partSpecConf.subPartitionSpecConfigs = subPartConfList;
            }
            partSpecConfList.add(partSpecConf);
        }

        // set configs for partitions
        partitionConfig.partitionSpecConfigs = partSpecConfList;

        return partitionConfig;
    }

    public void deleteTablePartitionByGidAndPartNameFromDelta(Long groupId, String partName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, groupId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, partName);

            DdlMetaLogUtil.logSql(DELETE_TABLE_PARTITIONS_BY_GID_AND_PART_FROM_DELTA_TABLE, params);

            MetaDbUtil.delete(DELETE_TABLE_PARTITIONS_BY_GID_AND_PART_FROM_DELTA_TABLE, params, connection);
            return;
        } catch (Exception e) {
            logger.error("Failed to query the system table 'table_partitions'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

}
