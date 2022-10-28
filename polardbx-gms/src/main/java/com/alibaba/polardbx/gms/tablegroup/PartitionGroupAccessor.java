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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PartitionGroupAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupAccessor.class);
    private static final String ALL_COLUMNS =
        "`id`,`partition_name`,`gmt_create`,`gmt_modified`,`phy_db`,`locality`,`primary_zone`,`tg_id`,`pax_group_id`, `meta_version`, `visible`";

    private static final String ALL_VALUES = "(null,?,null,now(),?,?,?,?,?,?,?)";
    private static final String ALL_VALUES_WITH_ID = "(?,?,?,now(),?,?,?,?,?,?,?)";

    //  phy_db    | locality | primary_zone | pax_group_id | meta_version | visible
    private static final String UPSERT_PART =
        " on duplicate key update phy_db=?, locality=?, primary_zone=?, pax_group_id=?, meta_version=?, visible=?, gmt_modified=now()";

    private static final String INSERT_IGNORE_PARTITION_GROUP =
        "insert ignore into " + GmsSystemTables.PARTITION_GROUP + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String INSERT_IGNORE_PARTITION_GROUP_DELTA =
        "insert ignore into " + GmsSystemTables.PARTITION_GROUP_DELTA + " (" + ALL_COLUMNS + ",`type`) VALUES "
            + "(null,?,null,now(),?,?,?,?,?,?,?,?)";

    private static final String INSERT_OUTDATE_PATITION_INTO_PARTITION_GROUP_DELTA =
        "insert into " + GmsSystemTables.PARTITION_GROUP_DELTA + " (" + ALL_COLUMNS + " ,`type`) VALUES "
            + "(?,?,null,now(),?,?,?,?,?,?,?,?)";

    private static final String UPSERT_PARTITION_GROUP =
        "insert into " + GmsSystemTables.PARTITION_GROUP + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES + UPSERT_PART;

    private static final String UPSERT_PARTITION_GROUP_DELTA =
        "insert into " + GmsSystemTables.PARTITION_GROUP_DELTA + " (" + ALL_COLUMNS + " ,`type`) VALUES " + ALL_VALUES
            + UPSERT_PART;

    private static final String INSERT_INTO_PARTITION_GROUP_WITH_ID =
        "insert into " + GmsSystemTables.PARTITION_GROUP + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES_WITH_ID;

    private static final String GET_TABLE_GROUP_BY_TG_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP + " where tg_id=? and visible=1";

    private static final String GET_UNVISIABLE_TABLE_GROUP_BY_TG_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP + " where tg_id=? and visible=0";

    private static final String GET_UNVISIABLE_PARTITION_GROUP_FROM_DELTA_BY_TG_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP_DELTA
            + " where tg_id=? and visible=0 and type = 1";

    private static final String GET_TABLE_GROUP_BY_PG_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP + " where id=?";

    private static final String GET_TABLE_GROUP_BY_PHY_DB =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP + " where phy_db=?";

    private static final String GET_ALL_PARTITION_GROUP =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP;

    private static final String GET_PHYSICAL_TB_CNT_PER_PG =
        "select a.phy_db, a.tg_id, count(1) phy_tb_cnt from partition_group a inner join table_partitions b on a.id=b.group_id where b.next_level = -1 and a.visible=1 group by phy_db";

    private static final String DELETE_PART_GROUP_BY_TG_ID =
        "delete from " + GmsSystemTables.PARTITION_GROUP + " where tg_id=?";

    private static final String DELETE_PART_GROUP_DELTA_BY_TG_ID =
        "delete from " + GmsSystemTables.PARTITION_GROUP_DELTA + " where tg_id=?";

    private static final String DELETE_PART_GROUP_BY_ID =
        "delete from " + GmsSystemTables.PARTITION_GROUP + " where id=?";

    private static final String DELETE_OUTDATE_PART_GROUP_BY_ID =
        "delete from " + GmsSystemTables.PARTITION_GROUP_DELTA + " where id=? and type=0";

    private static final String DELETE_NEW_PART_GROUP_BY_TG_ID_AND_PART_NAME =
        "delete from " + GmsSystemTables.PARTITION_GROUP_DELTA + " where tg_id=? and partition_name=? and type=1";

    private static final String UPDATE_PART_GROUP_TO_VISIABLE_BY_TG_ID =
        " update " + GmsSystemTables.PARTITION_GROUP + " set visible=1 where tg_id=? and visible=0";

    private static final String UPDATE_PART_GROUP_TO_VISIBILITY_BY_TG_ID =
        " update " + GmsSystemTables.PARTITION_GROUP + " set visible=? where tg_id=?";

    private static final String UPDATE_PART_GROUP_LOCALITY_BY_TG_ID =
        " update " + GmsSystemTables.PARTITION_GROUP + " set locality=? where id=?";

    private static final String UPDATE_PHY_DB_BY_ID =
        " update " + GmsSystemTables.PARTITION_GROUP + " set phy_db=? where id=?";

    private static final String UPDATE_PARTITION_NAME_BY_ID =
        " update " + GmsSystemTables.PARTITION_GROUP + " set partition_name=? where id=?";

    private static final String GET_OUTDATED_PARTITION_GROUP_BY_TGID =
        " select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP_DELTA + " where tg_id=? and type=0";

    private static final String GET_TEMP_PARTITION_GROUP_BY_TGID_AND_NAME =
        " select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP_DELTA
            + " where tg_id=? and partition_name=? and type=1";

    private static final String DELETE_TEMP_PART_GROUP_BY_TID_AND_NAME_FROM_DELTA =
        "delete from " + GmsSystemTables.PARTITION_GROUP_DELTA + " where tg_id=? and partition_name=? and type=1";

    private static final String GET_PARTITION_GROUP_BY_SCHEMA =
        " select " + ALL_COLUMNS + " from " + GmsSystemTables.PARTITION_GROUP
            + " where tg_id in ( select id from " + GmsSystemTables.TABLE_GROUP
            + " where schema_name = ? )";

    public List<PartitionGroupRecord> getAllPartitionGroups() {
        try {

            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            records =
                MetaDbUtil.query(GET_ALL_PARTITION_GROUP, params, PartitionGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    /**
     * unVisiableOnly = false, get all the visiable partitiongroup from partition_group
     * unVisiableOnly = true, get all the unvisiable partitiongroup from partition_group_delta
     */
    public List<PartitionGroupRecord> getPartitionGroupsByTableGroupId(Long tableGroupId, boolean unVisiableOnly) {
        try {

            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableGroupId);
            records =
                MetaDbUtil
                    .query(
                        unVisiableOnly ? GET_UNVISIABLE_PARTITION_GROUP_FROM_DELTA_BY_TG_ID : GET_TABLE_GROUP_BY_TG_ID,
                        params,
                        PartitionGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public PartitionGroupRecord getPartitionGroupById(Long pgId) {
        try {

            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, pgId);
            records =
                MetaDbUtil.query(GET_TABLE_GROUP_BY_PG_ID, params, PartitionGroupRecord.class, connection);

            if (GeneralUtil.isNotEmpty(records)) {
                return records.get(0);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PartitionGroupRecord> getPartitionGroupsByPhyDb(String phyDb) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, phyDb);

            return MetaDbUtil.query(GET_TABLE_GROUP_BY_PHY_DB, params, PartitionGroupRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public int[] addNewPartitionGroups(List<PartitionGroupRecord> partitionGroupRecords)
        throws SQLException {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
        for (int i = 0; i < partitionGroupRecords.size(); i++) {
            PartitionGroupRecord partRecord = partitionGroupRecords.get(i);
            Map<Integer, ParameterContext> params = new HashMap<>();

            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.partition_name);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.phy_db);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.locality);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.primary_zone);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.tg_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.pax_group_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.meta_version);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, partRecord.visible);

            paramsBatch.add(params);
        }
        DdlMetaLogUtil.logSql(INSERT_IGNORE_PARTITION_GROUP, paramsBatch);

        int[] ret = MetaDbUtil.insert(INSERT_IGNORE_PARTITION_GROUP, paramsBatch, this.connection);
        return ret;
    }

    public Long addNewPartitionGroup(PartitionGroupRecord partRecord, boolean toDeltaTable, boolean useUpsert) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();

            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.partition_name);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.phy_db);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.locality);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.primary_zone);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.tg_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.pax_group_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.meta_version);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, partRecord.visible);
            if (toDeltaTable) {
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, 1);
            }
            if (useUpsert) {
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.phy_db);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.locality);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.primary_zone);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.pax_group_id);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.meta_version);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, partRecord.visible);
            }
            paramsBatch.add(params);

            String stmtStr = "";
            if (toDeltaTable) {
                stmtStr = useUpsert ? UPSERT_PARTITION_GROUP_DELTA : INSERT_IGNORE_PARTITION_GROUP_DELTA;
            } else {
                stmtStr = useUpsert ? UPSERT_PARTITION_GROUP : INSERT_IGNORE_PARTITION_GROUP;
            }

            DdlMetaLogUtil.logSql(stmtStr, paramsBatch);

            return MetaDbUtil.insertAndRetureLastInsertId(
                stmtStr, paramsBatch,
                this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public Long addNewPartitionGroup(PartitionGroupRecord partRecord, boolean toDeltaTable) {
        return addNewPartitionGroup(partRecord, toDeltaTable, false);
    }

    public Long addNewPartitionGroupWithId(PartitionGroupRecord partRecord) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();

            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.partition_name);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setTimestamp1, partRecord.gmt_create);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.phy_db);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.locality);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.primary_zone);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.tg_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.pax_group_id);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.meta_version);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, partRecord.visible);
            paramsBatch.add(params);

            DdlMetaLogUtil.logSql(INSERT_INTO_PARTITION_GROUP_WITH_ID, paramsBatch);

            return MetaDbUtil.insertAndRetureLastInsertId(
                INSERT_INTO_PARTITION_GROUP_WITH_ID, paramsBatch,
                this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PartitionGroupExtRecord> getGetPhysicalTbCntPerPg() {
        try {
            List<PartitionGroupExtRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            records =
                MetaDbUtil.query(GET_PHYSICAL_TB_CNT_PER_PG, params, PartitionGroupExtRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deletePartitionGroupsByTableGroupId(Long tgId, boolean fromDeltaTable) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tgId);
            String sql = fromDeltaTable ? DELETE_PART_GROUP_DELTA_BY_TG_ID : DELETE_PART_GROUP_BY_TG_ID;
            DdlMetaLogUtil.logSql(sql, params);
            MetaDbUtil.update(sql, params,
                connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deletePartitionGroupById(Long id) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
            DdlMetaLogUtil.logSql(DELETE_PART_GROUP_BY_ID, params);

            MetaDbUtil.update(DELETE_PART_GROUP_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePartGroupVisiabilityByTgId(Long tgId, Integer visibility) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, visibility);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tgId);
            MetaDbUtil.update(UPDATE_PART_GROUP_TO_VISIBILITY_BY_TG_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePhyDbById(Long id, String newPhyDb) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newPhyDb);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_PHY_DB_BY_ID, params);

            MetaDbUtil.update(UPDATE_PHY_DB_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePartitioNameById(Long id, String newPartitionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newPartitionName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_PARTITION_NAME_BY_ID, params);

            MetaDbUtil.update(UPDATE_PARTITION_NAME_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updatePartitionGroupLocality(Long id, String locality) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, locality);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_PART_GROUP_LOCALITY_BY_TG_ID, params);

            MetaDbUtil.update(UPDATE_PART_GROUP_LOCALITY_BY_TG_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PartitionGroupRecord> getOutDatedPartitionGroupsByTableGroupIdFromDelta(Long tableGroupId) {
        try {

            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableGroupId);
            records =
                MetaDbUtil
                    .query(
                        GET_OUTDATED_PARTITION_GROUP_BY_TGID,
                        params,
                        PartitionGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP_DELTA, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PartitionGroupRecord> getTempPartitionGroupsByTableGroupIdAndNameFromDelta(Long tableGroupId,
                                                                                           String partName) {
        try {

            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableGroupId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, partName);

            records =
                MetaDbUtil
                    .query(
                        GET_TEMP_PARTITION_GROUP_BY_TGID_AND_NAME,
                        params,
                        PartitionGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP_DELTA, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deletePartitionGroupByIdFromDelta(Long gid, String partitionName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, gid);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, partitionName);
            DdlMetaLogUtil.logSql(DELETE_TEMP_PART_GROUP_BY_TID_AND_NAME_FROM_DELTA, params);

            MetaDbUtil.update(DELETE_TEMP_PART_GROUP_BY_TID_AND_NAME_FROM_DELTA, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public int[] insertOldDatedPartitionGroupToDelta(List<PartitionGroupRecord> partRecords) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();

            for (PartitionGroupRecord partRecord : partRecords) {
                Map<Integer, ParameterContext> params = new HashMap<>();

                int index = 1;
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.id);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.partition_name);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.phy_db);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.locality);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, partRecord.primary_zone);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.tg_id);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.pax_group_id);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partRecord.meta_version);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, partRecord.visible);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, 0);

                paramsBatch.add(params);
            }
            DdlMetaLogUtil.logSql(INSERT_OUTDATE_PATITION_INTO_PARTITION_GROUP_DELTA, paramsBatch);

            return MetaDbUtil.insert(
                INSERT_OUTDATE_PATITION_INTO_PARTITION_GROUP_DELTA, paramsBatch,
                this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PARTITION_GROUP_DELTA, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public int[] deleteOldDatedPartitionGroupFromDelta(List<Long> oldDatePartitionGroupIds) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();

            for (Long partitionGroupId : oldDatePartitionGroupIds) {
                Map<Integer, ParameterContext> params = new HashMap<>();

                int index = 1;
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, partitionGroupId);
                paramsBatch.add(params);
            }

            DdlMetaLogUtil.logSql(DELETE_OUTDATE_PART_GROUP_BY_ID, paramsBatch);

            return MetaDbUtil.update(
                DELETE_OUTDATE_PART_GROUP_BY_ID, paramsBatch,
                this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PARTITION_GROUP_DELTA, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public int[] deleteNewPartitionGroupFromDelta(Long tableGroupId, List<String> newPartitionNames) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();

            for (String newPartitionName : newPartitionNames) {
                Map<Integer, ParameterContext> params = new HashMap<>();

                int index = 1;
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, tableGroupId);
                MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, newPartitionName);
                paramsBatch.add(params);
            }

            DdlMetaLogUtil.logSql(DELETE_NEW_PART_GROUP_BY_TG_ID_AND_PART_NAME, paramsBatch);

            return MetaDbUtil.update(
                DELETE_NEW_PART_GROUP_BY_TG_ID_AND_PART_NAME, paramsBatch,
                this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.PARTITION_GROUP_DELTA, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<PartitionGroupRecord> getPartitionGroupsBySchema(String schemaName) {
        try {
            List<PartitionGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            records =
                MetaDbUtil
                    .query(GET_PARTITION_GROUP_BY_SCHEMA,
                        params,
                        PartitionGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.PARTITION_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

}
