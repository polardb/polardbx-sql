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

import com.alibaba.fastjson.JSONObject;
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
public class DbInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(DbInfoAccessor.class);
    private static final String DB_INFO_TABLE = GmsSystemTables.DB_INFO;

    private static final String EXTRA_COLUMN = "extra";

    protected static final String SELECT_DB_INFO_BY_STATUS = "select * from `" + DB_INFO_TABLE + "` where db_status=?";

    protected static final String SELECT_DB_INFO_BY_TYPE = "select * from `" + DB_INFO_TABLE + "` where db_type=?";

    protected static final String SELECT_DB_INFO_BY_DB_NAME = "select * from `" + DB_INFO_TABLE + "` where db_name=?";

    protected static final String SELECT_DB_INFO_BY_DB_TYPE =
        "select * from `" + DB_INFO_TABLE + "` where db_name=? and db_type=?";

    protected static final String SELECT_ALL_DB_INFO_LIST = "select * from `" + DB_INFO_TABLE + "`";

    protected static final String SELECT_DB_INFO_BY_DB_NAME_FOR_UPDATE =
        "select * from `" + DB_INFO_TABLE + "` where db_name=? for update";

    protected static final String INSERT_IGNORE_NEW_DB =
        "insert ignore into db_info (id, gmt_created, gmt_modified, db_name, app_name, db_type, db_status, charset, collation) values (null, now(), now(), ?, ?, ?, ?, ?, ?)";

    protected static final String INSERT_IGNORE_NEW_DB_WITH_EXTRA =
        "insert ignore into db_info (id, gmt_created, gmt_modified, db_name, app_name, db_type, db_status, charset, collation, extra) values (null, now(), now(), ?, ?, ?, ?, ?, ?, ?)";

    protected static final String DELETE_DB_INFO_BY_DB_NAME = "delete from `" + DB_INFO_TABLE + "` where db_name=?";

    protected static final String UPDATE_DB_STATUS_BY_DB_NAME =
        "update `" + DB_INFO_TABLE + "` set db_status=? where db_name=?";

    protected static final String UPDATE_DB_READ_WRITE_STATUS_BY_DB_NAME =
        "update `" + DB_INFO_TABLE + "` set read_write_status=? where db_name=?";

    public int deleteDbInfoByDbName(String dbName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            int affectedRow = MetaDbUtil.delete(DELETE_DB_INFO_BY_DB_NAME, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete from  the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public int updateDbStatusByDbName(String dbName, int dbStatus) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, dbStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);
            int affectedRow = MetaDbUtil.delete(UPDATE_DB_STATUS_BY_DB_NAME, params, connection);
            return affectedRow;
        } catch (Exception e) {
            logger.error("Failed to delete from  the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public DbInfoRecord getDbInfoByDbNameForUpdate(String dbName) {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            records = MetaDbUtil.query(SELECT_DB_INFO_BY_DB_NAME_FOR_UPDATE, params, DbInfoRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbInfoRecord> getAllDbInfos() {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            records = MetaDbUtil.query(SELECT_ALL_DB_INFO_LIST, params, DbInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public DbInfoRecord getDbInfoByDbName(String dbName) {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
            records = MetaDbUtil.query(SELECT_DB_INFO_BY_DB_NAME, params, DbInfoRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbInfoRecord> getDbInfoListByStatus(int status) {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
            records = MetaDbUtil.query(SELECT_DB_INFO_BY_STATUS, params, DbInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbInfoRecord> getDbInfoByType(int dbType) {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, dbType);
            records = MetaDbUtil.query(SELECT_DB_INFO_BY_TYPE, params, DbInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<DbInfoRecord> getDbInfoBySchAndType(String schemaName, int dbType) {
        try {
            List<DbInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, dbType);
            records = MetaDbUtil.query(SELECT_DB_INFO_BY_DB_TYPE, params, DbInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public void addNewDb(String dbName,
                         String appName,
                         int dbType,
                         int dbStatus,
                         String charset,
                         String collation,
                         Boolean encryption,
                         Boolean defaultSingle) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dbName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, appName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setInt, dbType);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, dbStatus);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, charset);
        MetaDbUtil.setParameter(6, params, ParameterMethod.setString, collation);
        JSONObject extra = new JSONObject();
        extra.put(DbInfoRecord.EXTRA_KEY_ENCRYPTION, encryption);
        extra.put(DbInfoRecord.EXTRA_KEY_DEFAULT_SINGLE, defaultSingle);
        MetaDbUtil.setParameter(7, params, ParameterMethod.setString, extra.toJSONString());
        try {
            MetaDbUtil.insert(INSERT_IGNORE_NEW_DB_WITH_EXTRA, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateDbReadWriteStatusByName(String dbName, int readWriteStatus) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, readWriteStatus);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dbName);

        try {
            MetaDbUtil.update(UPDATE_DB_READ_WRITE_STATUS_BY_DB_NAME, params, this.connection);
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + DB_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                DB_INFO_TABLE,
                e.getMessage());
        }
    }

}
