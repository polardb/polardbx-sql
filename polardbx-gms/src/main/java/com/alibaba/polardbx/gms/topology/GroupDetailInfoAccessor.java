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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class GroupDetailInfoAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(GroupDetailInfoAccessor.class);

    private static final String GROUP_DETAIL_INFO_TABLE = GmsSystemTables.GROUP_DETAIL_INFO;

    private static final String SELECT_ALL_GROUP_DETAIL_INFO = "select * from `" + GROUP_DETAIL_INFO_TABLE + "`";
    private static final String SELECT_GROUP_DETAILS_BY_INST_ID_AND_DB_NAME =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where inst_id=? and db_name=?";
    private static final String SELECT_GROUP_DETAILS_BY_INST_ID =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where inst_id=?";

    private static final String SELECT_GROUP_DETAIL_BY_INST_DB_GROUP =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where inst_id=? and db_name=? and group_name=?";

    private static final String SELECT_GROUP_DETAIL_BY_DB_GROUP =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where db_name=? and group_name=?";

    protected static final String INSERT_IGNORE_NEW_GROUP_DETAIL_INFO =
        "insert ignore into group_detail_info (id, gmt_created, gmt_modified, inst_id, db_name, group_name, storage_inst_id) values (null, now(), now(), ?, ?, ?, ?)";

    private static final String SELECT_GROUP_DETAIL_LIST_BY_DB_NAME =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where db_name=?";

    private static final String DELETE_GROUP_DETAIL_INFO_BY_DB_NAME =
        "delete from `" + GROUP_DETAIL_INFO_TABLE + "` where db_name=?";

    private static final String DELETE_GROUP_DETAIL_INFO_BY_DB_AND_GROUP =
        "delete from `" + GROUP_DETAIL_INFO_TABLE + "` where  db_name=? and group_name=?";

    private static final String SELECT_GROUP_DETAILS_BY_STORAGE_INST_ID =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where storage_inst_id=?";

    private static final String SELECT_GROUP_DETAILS_BY_DB_NAME_AND_STORAGE_INST_ID =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where db_name =? and storage_inst_id=?";

    private static final String SELECT_ALL_PHY_DB_NAME_BY_DB_NAME_AND_STORAGE_INST_ID =
            "select t2.* from " + GROUP_DETAIL_INFO_TABLE + " as t1 " +
                    "join " + GmsSystemTables.DB_GROUP_INFO + " as t2 " +
                    "on t1.group_name = t2.group_name " +
                    "and t1.db_name=t2.db_name " +
                    "where t1.storage_inst_id = ? " +
                    "and t1.db_name=?";

    private static final String SELECT_GROUP_DETAILS_BY_DB_NAME_AND_GROUP =
        "select * from `" + GROUP_DETAIL_INFO_TABLE + "` where db_name =? and group_name=?";

    private static final String UPDATE_STORAGE_INST_ID_BY_INST_DB_GROUP =
        "update `" + GROUP_DETAIL_INFO_TABLE
            + "` set storage_inst_id=? where inst_id=? and db_name=? and group_name=?";

    private static final String SELECT_DISTINCT_INST_ID_LIST_BY_DB_GROUP =
        "select distinct g.inst_id as inst_id from group_detail_info g where g.db_name=? and g.group_name=?";

    private static final String SELECT_STORAGE_INST_ID_LIST_BY_STORAGE_DB_GROUP =
        "select g.storage_inst_id as storage_inst_id from group_detail_info g where g.storage_inst_id in (%s) and g.db_name=? and g.group_name=?";

    private static final String SELECT_GROUP_DETAIL_PHY_DB_INFO_BY_INST_ID =
        "select group_detail.storage_inst_id as storage_inst_id, group_detail.db_name as db_name, group_detail.group_name as group_name, db_group.phy_db_name as phy_db_name from db_group_info db_group inner join group_detail_info group_detail on db_group.db_name = group_detail.db_name and db_group.group_name = group_detail.group_name where  group_detail.inst_id=? and group_detail.db_name!=? order by   storage_inst_id,   db_name,  group_name,  phy_db_name;\n";

    private static final String SELECT_GROUP_DETAIL_FOR_PARTITION_PHY_DB_INFO_BY_INST_ID_STORAGE_INST_ID_DB_NAME =
        "select group_detail.storage_inst_id as storage_inst_id, group_detail.db_name as db_name, group_detail.group_name as group_name, db_group.phy_db_name as phy_db_name from db_group_info db_group inner join group_detail_info group_detail on db_group.db_name = group_detail.db_name and db_group.group_name = group_detail.group_name where  group_detail.inst_id=? and group_detail.db_name=? and group_detail.storage_inst_id=? and db_group.group_type=5 order by   storage_inst_id,   db_name,  group_name,  phy_db_name;\n";

    private static final String SELECT_DB_CNT_AND_GROUP_CNT_BY_STORAGE_INST_ID =
        "select count(distinct db_name) as db_count, count(distinct group_name) as group_cnt from `"
            + GROUP_DETAIL_INFO_TABLE + "` where storage_inst_id=?;";

    private static final String ADD_SLAVE_GROUP_CONFIGS_FOR_RO_INST_TEMPLATE =
        "insert ignore into group_detail_info(id, gmt_created, gmt_modified, inst_id, db_name, group_name, storage_inst_id ) select null as id, now() as gmt_created, now() as gmt_modified, '%s' as inst_id, db_name, group_name, '%s' as storage_inst_id from group_detail_info where db_name!='polardbx' and db_name!='information_schema' and storage_inst_id='%s';";

    private static final String ADD_SLAVE_META_DB_GROUP_CONFIGS_FOR_RO_INST_TEMPLATE =
        "insert ignore into group_detail_info(id, gmt_created, gmt_modified, inst_id, db_name, group_name, storage_inst_id ) select null as id, now() as gmt_created, now() as gmt_modified, '%s' as inst_id, db_name, group_name, '%s' as storage_inst_id from group_detail_info where db_name in ('polardbx','information_schema')";

    private static final String SELECT_GROUP_DETAIL_INFO_CNT_BY_STORAGE_INST_ID_AND_DB_NAME_AND_GROUP_NAMES =
        "select count(1) from `"
            + GROUP_DETAIL_INFO_TABLE + "` where storage_inst_id=? and db_name=? and group_name in (%s);";

    private static final String SELECT_GROUP_DETAIL_PHY_DB_INFO_BY_INST_ID_FOR_PARTITION_TABLE =
        "select group_detail.storage_inst_id as storage_inst_id, group_detail.db_name as db_name, group_detail.group_name as group_name, db_group.phy_db_name as phy_db_name from db_group_info db_group inner join group_detail_info group_detail on db_group.db_name = group_detail.db_name and db_group.group_name = group_detail.group_name where  group_detail.inst_id=? and db_group.group_type=? order by   storage_inst_id,   db_name,  group_name,  phy_db_name;\n";

    private static final String DELETE_GROUP_DETAIL_INFO_BY_INST_ID =
        "delete from `" + GROUP_DETAIL_INFO_TABLE + "` where inst_id=?";

    public void addNewGroupDetailInfo(String instId, String dbName, String groupName, String storageInstId)
        throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, groupName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, storageInstId);
        MetaDbUtil.insert(INSERT_IGNORE_NEW_GROUP_DETAIL_INFO, params, this.connection);
    }

    public void addNewGroupDetailInfosForRoInst(String readOnlyInstId, String storageMasterInstId,
                                                String storageSlaveInstId, boolean addGroupsForMetaDbInst)
        throws SQLException {
        String sql = "";
        if (addGroupsForMetaDbInst) {
            sql = String
                .format(ADD_SLAVE_META_DB_GROUP_CONFIGS_FOR_RO_INST_TEMPLATE, readOnlyInstId, storageMasterInstId);
        } else {
            sql = String.format(ADD_SLAVE_GROUP_CONFIGS_FOR_RO_INST_TEMPLATE, readOnlyInstId, storageSlaveInstId,
                storageMasterInstId);
        }

        Map<Integer, ParameterContext> params = new HashMap<>();

        MetaDbUtil.execute(sql, params, this.connection);
    }

    public void deleteGroupDetailInfoByDbName(String dbName) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
        MetaDbUtil.delete(DELETE_GROUP_DETAIL_INFO_BY_DB_NAME, params, this.connection);
    }

    public void deleteGroupDetailInfoByDbAndGroup(String dbName, String groupName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
            MetaDbUtil.delete(DELETE_GROUP_DETAIL_INFO_BY_DB_AND_GROUP, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }


    }

    public void deleteGroupDetailInfoByInstId(String instId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.delete(DELETE_GROUP_DETAIL_INFO_BY_INST_ID, params, this.connection);
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByInstIdAndDbName(String instId, String dbName) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAILS_BY_INST_ID_AND_DB_NAME, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByInstId(String instId) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAILS_BY_INST_ID, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByStorageInstId(String storageInstId) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageInstId);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAILS_BY_STORAGE_INST_ID, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByDbNameAndStorageInstId(String dbName, String storageInstId) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, storageInstId);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAILS_BY_DB_NAME_AND_STORAGE_INST_ID, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbGroupInfoRecord> getAllPhyDbNameByInstId(String dbName, String storageInstId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            return MetaDbUtil.query(SELECT_ALL_PHY_DB_NAME_BY_DB_NAME_AND_STORAGE_INST_ID, params, DbGroupInfoRecord.class,
                            connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                    GROUP_DETAIL_INFO_TABLE,
                    e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByDbNameAndGroup(String dbName, String groupName) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAILS_BY_DB_NAME_AND_GROUP, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<String> getDistinctInstIdListByDbNameAndGroupName(String dbName, String groupName) {
        try {
            List<String> instIdList = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
            try (PreparedStatement ps = connection.prepareStatement(SELECT_DISTINCT_INST_ID_LIST_BY_DB_GROUP)) {
                if (params != null && params.size() > 0) {
                    for (ParameterContext param : params.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        instIdList.add(rs.getString(1));
                    }
                }
            }
            return instIdList;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public Set<String> getStorageInstIdListByStorageIdAndDbNameAndGroupName(
        Set<String> storageIds, String dbName, String groupName) {
        try {
            Set<String> instIdList = new HashSet<>();
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);

            try (PreparedStatement ps = connection.prepareStatement(
                String.format(SELECT_STORAGE_INST_ID_LIST_BY_STORAGE_DB_GROUP, concat(storageIds)))) {
                if (params != null && params.size() > 0) {
                    for (ParameterContext param : params.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        instIdList.add(rs.getString(1));
                    }
                }
            }
            return instIdList;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public GroupDetailInfoRecord getGroupDetailInfoByInstIdAndGroupName(String instId, String dbName,
                                                                        String groupName) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, groupName);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAIL_BY_INST_DB_GROUP, params, GroupDetailInfoRecord.class,
                    connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoByInstIdAndGroupName(Set<String> instIds, String dbName,
                                                                              String groupName) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);

            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(SELECT_GROUP_DETAIL_BY_DB_GROUP);
            sqlBuilder.append(" and (");
            int index = 0;
            for (String instId : instIds) {
                if (index != 0) {
                    sqlBuilder.append(" or ");
                }
                sqlBuilder.append(" inst_id= ").append("'" + instId + "'");
                index++;
            }
            sqlBuilder.append(" )");
            records = MetaDbUtil
                .query(sqlBuilder.toString(), params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public int updateStorageInstId(String instId, String dbName,
                                   String groupName, String newStorageInstId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newStorageInstId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, groupName);
            int affectiveRow = MetaDbUtil.update(UPDATE_STORAGE_INST_ID_BY_INST_DB_GROUP, params, connection);
            return affectiveRow;
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoRecord> getGroupDetailInfoListByDbName(String dbName) {
        try {
            List<GroupDetailInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            String selectSql = String.format(SELECT_GROUP_DETAIL_LIST_BY_DB_NAME, dbName);
            records = MetaDbUtil
                .query(selectSql, params, GroupDetailInfoRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoExRecord> getCompletedGroupInfosByInstIdAndDbName(String instId, String storage_instId,
                                                                                 String dbName) {
        try {
            List<GroupDetailInfoExRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, storage_instId);

            records = MetaDbUtil
                .query(SELECT_GROUP_DETAIL_FOR_PARTITION_PHY_DB_INFO_BY_INST_ID_STORAGE_INST_ID_DB_NAME, params,
                    GroupDetailInfoExRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<GroupDetailInfoExRecord> getCompletedGroupInfosByInstId(String instId) {
        try {
            List<GroupDetailInfoExRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, SystemDbHelper.DEFAULT_DB_NAME);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAIL_PHY_DB_INFO_BY_INST_ID, params, GroupDetailInfoExRecord.class,
                    connection);
            records =
                records.stream().filter(r -> !SystemDbHelper.CDC_DB_NAME.equals(r.dbName)).collect(Collectors.toList());
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public Pair<Integer, Integer> getDbCountAndGroupCountByStorageInstId(String storageInstId) {
        try {
            Pair<Integer, Integer> dbCntAndGrpCntPair = null;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageInstId);
            try (PreparedStatement ps = connection.prepareStatement(SELECT_DB_CNT_AND_GROUP_CNT_BY_STORAGE_INST_ID)) {
                if (params != null && params.size() > 0) {
                    for (ParameterContext param : params.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    int dbCnt = rs.getInt(1);
                    int grpCnt = rs.getInt(2);
                    dbCntAndGrpCntPair = new Pair<>(dbCnt, grpCnt);
                }
            }
            return dbCntAndGrpCntPair;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

    public boolean checkGroupDetailInfoExistenceByStorageInstIdAndDbNameAndGroupNames(String storageInstId,
                                                                                      String dbName,
                                                                                      List<String> groupNames) {

        boolean res = false;

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);

            try (PreparedStatement ps = connection
                .prepareStatement(String
                    .format(SELECT_GROUP_DETAIL_INFO_CNT_BY_STORAGE_INST_ID_AND_DB_NAME_AND_GROUP_NAMES,
                        concat(groupNames)))) {
                if (params != null && params.size() > 0) {
                    for (ParameterContext param : params.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    int rowCount = rs.getInt(1);
                    res = rowCount > 0;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
        return res;
    }

    public List<GroupDetailInfoExRecord> getCompletedGroupInfosByInstIdForPartitionTables(String instId,
                                                                                          int groupType) {
        try {
            List<GroupDetailInfoExRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, groupType);
            records = MetaDbUtil
                .query(SELECT_GROUP_DETAIL_PHY_DB_INFO_BY_INST_ID_FOR_PARTITION_TABLE, params,
                    GroupDetailInfoExRecord.class,
                    connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + GROUP_DETAIL_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                GROUP_DETAIL_INFO_TABLE,
                e.getMessage());
        }
    }

}
