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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class DbGroupInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(DbGroupInfoAccessor.class);
    private static final String DB_GROUP_INFO_TABLE = GmsSystemTables.DB_GROUP_INFO;

    protected String instId = "";
    protected String dbName = "";

    protected static final String SELECT_GROUPS_BY_DB_NAME =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where db_name=?";

    protected static final String SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_NAME =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where db_name=? and group_name=?";

    protected static final String SELECT_DB_GROUP_INFO_BY_GROUP_NAME =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where group_name=?";

    protected static final String SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_NAME_FOR_UPDATE =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where db_name=? and group_name=? for update";

    protected static final String SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_TYPE =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where db_name=? and group_type=?";

    protected static final String SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_TYPE_FOR_UPDATE =
        "select * from `" + DB_GROUP_INFO_TABLE + "` where db_name=? and group_type=? for update";

    protected static final String INSERT_IGNORE_NEW_DB_GROUP =
        "insert ignore into db_group_info (id, gmt_created, gmt_modified, db_name, group_name, phy_db_name, group_type) values (null, now(), now(), ?, ?, ?, ?)";

    protected static final String DELETE_DB_GROUP_INFO_BY_DB_NAME =
        "delete from `" + DB_GROUP_INFO_TABLE + "` where db_name=?";

    protected static final String DELETE_DB_GROUP_INFO_BY_DB_AND_GROUP =
        "delete from `" + DB_GROUP_INFO_TABLE + "` where db_name=? and group_name=?";

    protected static final String UPDATE_GROUP_TYPE_BY_DB_AND_GROUP =
        "update `" + DB_GROUP_INFO_TABLE + "` set group_type=? where db_name=? and group_name=?";

    protected static final String UPDATE_PHY_DB_BY_DB_AND_GROUP =
        "update `" + DB_GROUP_INFO_TABLE + "` set phy_db_name=? where db_name=? and group_name=?";

    public int deleteDbGroupInfoByDbName(String dbName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            int affectedRows = MetaDbUtil.delete(DELETE_DB_GROUP_INFO_BY_DB_NAME, params, connection);
            return affectedRows;
        } catch (Exception e) {
            logger.error("Failed to delete from the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public int deleteDbGroupInfoByDbAndGroup(String dbName, String groupName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
            int affectedRows = MetaDbUtil.delete(DELETE_DB_GROUP_INFO_BY_DB_AND_GROUP, params, connection);
            return affectedRows;
        } catch (Exception e) {
            logger.error("Failed to delete from the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public DbGroupInfoRecord getDbGroupInfoByDbNameAndGroupName(String dbName, String groupName) {
        return getDbGroupInfoByDbNameAndGroupName(dbName, groupName, false);
    }

    public DbGroupInfoRecord getDbGroupInfoByDbNameAndGroupName(String dbName, String groupName, boolean isForUpdate) {
        try {
            List<DbGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
            String updateSql = isForUpdate ? SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_NAME_FOR_UPDATE :
                SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_NAME;
            records = MetaDbUtil.query(updateSql, params, DbGroupInfoRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public DbGroupInfoRecord getDbGroupInfoByGroupName(String groupName) {
        try {
            List<DbGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, groupName);
            String querysql = SELECT_DB_GROUP_INFO_BY_GROUP_NAME;
            records = MetaDbUtil.query(querysql, params, DbGroupInfoRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public void addNewDbAndGroup(String dbName, String groupName, String phyDbName, int groupType) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, groupName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, phyDbName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, groupType);

        try {
            MetaDbUtil.insert(INSERT_IGNORE_NEW_DB_GROUP, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateGroupTypeByDbAndGroup(String dbName, String groupName, int groupType) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, groupType);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, groupName);

        try {
            MetaDbUtil.update(UPDATE_GROUP_TYPE_BY_DB_AND_GROUP, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updatePhyDbNameByDbAndGroup(String dbName, String groupName, String newPhyDbName) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newPhyDbName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, groupName);

        try {
            MetaDbUtil.update(UPDATE_PHY_DB_BY_DB_AND_GROUP, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbGroupInfoRecord> queryDbGroupByDbName(String targetDbName) {
        try {
            List<DbGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, targetDbName);
            records = MetaDbUtil.query(SELECT_GROUPS_BY_DB_NAME, params, DbGroupInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbGroupInfoRecord> getDbGroupInfoByDbNameAndGroupType(String dbName, int groupType,
                                                                      boolean isForUpdate) {
        try {
            List<DbGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, groupType);
            String updateSql = isForUpdate ? SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_TYPE_FOR_UPDATE :
                SELECT_DB_GROUP_INFO_BY_DB_NAME_AND_GROUP_TYPE;
            records = MetaDbUtil.query(updateSql, params, DbGroupInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_GROUP_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_GROUP_INFO_TABLE,
                e.getMessage());
        }
    }

}
