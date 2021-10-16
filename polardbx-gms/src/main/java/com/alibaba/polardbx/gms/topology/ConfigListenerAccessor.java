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

import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class ConfigListenerAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(ConfigListenerAccessor.class);

    private static final String CONFIG_LISTENER_TABLE = GmsSystemTables.CONFIG_LISTENER;

    public static final long DEFAULT_OP_VERSION = 1;

    private static final String SELECT_ALL_DATA_ID = "select * from `" + CONFIG_LISTENER_TABLE + "`";

    private static final String SELECT_DATA_ID_BY_PREFIX = SELECT_ALL_DATA_ID + " where data_id like ?";

    private static final String SELECT_DATA_ID_SET_BY_GMT_MODIFIED =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where gmt_modified >= DATE_ADD(now(),INTERVAL -1 * ? MINUTE)";

    private static final String SELECT_DATA_ID =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where data_id = ?";

    private static final String SELECT_DATA_ID_FOR_UPDATE =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where data_id = ? for update";

    private static final String SELECT_DATA_ID_BY_ID_FOR_UPDATE =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where id = ? for update";

    private static final String SELECT_DATA_ID_SET_BY_STATUS =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where status = ?";

    private static final String SELECT_DATA_ID_SET_BY_STATUS_FOR_UPDATE =
        "select * from `" + CONFIG_LISTENER_TABLE + "` where data_id = ? and status = ? for update";

    private static final String REPLACE_INTO_ONE_DATA_ID = "replace into `" + CONFIG_LISTENER_TABLE
        + "` (id, gmt_created, gmt_modified, data_id, status, op_version, extras) values (null, now(), now(), ?, ?, ?, NULL)";

    private static final String INSERT_IGNORE_INTO_ONE_DATA_ID = "insert ignore into `" + CONFIG_LISTENER_TABLE
        + "` (id, gmt_created, gmt_modified, data_id, status, op_version, extras) values (null, now(), now(), ?, ?, ?, NULL)";

    private static final String UPDATE_DATA_ID_OPVERSION = "update `" + CONFIG_LISTENER_TABLE
        + "` set op_version=LAST_INSERT_ID(op_version + 1) where data_id=?;";

    private static final String UPDATE_DATA_IDS_OPVERSION = "update `" + CONFIG_LISTENER_TABLE
        + "` set op_version=op_version+1 where data_id in (%s);";

    private static final String UPDATE_DATA_ID_STATUS = "update `" + CONFIG_LISTENER_TABLE
        + "` set status=? where data_id=?;";

    private static final String UPDATE_DATA_ID_STATUS_AND_VALUE = "update `" + CONFIG_LISTENER_TABLE
        + "` set status=?, op_version=? where data_id=?;";

    private static final String UPDATE_DATA_ID_STATUS_AND_VALUE_BY_ID = "update `" + CONFIG_LISTENER_TABLE
        + "` set status=?, op_version=? where id=?;";

    private static final String DELETE_DATA_ID = "delete from `" + CONFIG_LISTENER_TABLE + "` where data_id=?";

    public static final String DELETE_ALL_DATA_ID_BY_INST_ID =
        "delete from config_listener where data_id like '%%%s%%'";

    public List<ConfigListenerRecord> getAllDataIds() {
        try {
            return MetaDbUtil.query(SELECT_ALL_DATA_ID, ConfigListenerRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public List<ConfigListenerRecord> getDataIdsByPrefix(String dataIdPrefix) {
        try {
            String likeFilter = dataIdPrefix + "%";
            Map<Integer, ParameterContext> selectParams = MetaDbUtil.buildStringParameters(new String[] {likeFilter});
            return MetaDbUtil.query(SELECT_DATA_ID_BY_PREFIX, selectParams, ConfigListenerRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query active records the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query active records",
                CONFIG_LISTENER_TABLE, e.getMessage());
        }
    }

    public List<ConfigListenerRecord> getDataIds(int lastMinuteCount) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setInt, lastMinuteCount);
            return MetaDbUtil
                .query(SELECT_DATA_ID_SET_BY_GMT_MODIFIED, selectParams, ConfigListenerRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public List<ConfigListenerRecord> getDataIdsByStatus(int status) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setInt, status);
            return MetaDbUtil.query(SELECT_DATA_ID_SET_BY_STATUS, selectParams, ConfigListenerRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public ConfigListenerRecord getDataIdByStatusForUpdate(String dataId, int status) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, dataId);
            MetaDbUtil.setParameter(2, selectParams, ParameterMethod.setInt, status);
            List<ConfigListenerRecord> records = MetaDbUtil
                .query(SELECT_DATA_ID_SET_BY_STATUS_FOR_UPDATE, selectParams, ConfigListenerRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public ConfigListenerRecord getDataId(String dataId, boolean isForUpdate) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, dataId);
            List<ConfigListenerRecord> records = null;

            if (!isForUpdate) {
                records = MetaDbUtil.query(SELECT_DATA_ID, selectParams, ConfigListenerRecord.class, connection);
            } else {
                records =
                    MetaDbUtil.query(SELECT_DATA_ID_FOR_UPDATE, selectParams, ConfigListenerRecord.class, connection);
            }

            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public void addDataId(String dataId, int listenerStatus, long initOpVersion) {
        try {

            boolean needSetAutoCommitFalse = false;
            if (connection.getAutoCommit()) {
                needSetAutoCommitFalse = true;
                connection.setAutoCommit(false);
            }

            Map<Integer, ParameterContext> queryParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, queryParams, ParameterMethod.setString, dataId);
            List<ConfigListenerRecord> records =
                MetaDbUtil.query(SELECT_DATA_ID, queryParams, ConfigListenerRecord.class, connection);
            if (records.size() == 0) {
                Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
                MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, dataId);
                MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setInt, listenerStatus);
                MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setLong, initOpVersion);
                MetaDbUtil.insert(INSERT_IGNORE_INTO_ONE_DATA_ID, insertParams, connection);
                DdlMetaLogUtil.logSql(INSERT_IGNORE_INTO_ONE_DATA_ID, insertParams);
            } else {
                long idVal = records.get(0).id;
                Map<Integer, ParameterContext> forUpdateParams = Maps.newHashMap();
                MetaDbUtil.setParameter(1, forUpdateParams, ParameterMethod.setLong, idVal);
                DdlMetaLogUtil.logSql(SELECT_DATA_ID_BY_ID_FOR_UPDATE, forUpdateParams);
                List<ConfigListenerRecord> forUpdateRs =
                    MetaDbUtil.query(SELECT_DATA_ID_BY_ID_FOR_UPDATE, forUpdateParams, ConfigListenerRecord.class,
                        connection);
                if (forUpdateRs.size() > 0) {
                    ConfigListenerRecord recordForUpdate = forUpdateRs.get(0);
                    if (recordForUpdate.status == ConfigListenerRecord.DATA_ID_STATUS_REMOVED) {
                        // reset record.status to DATA_ID_STATUS_NORMAL
                        Map<Integer, ParameterContext> updateParams = Maps.newHashMap();
                        MetaDbUtil.setParameter(1, updateParams, ParameterMethod.setInt, listenerStatus);
                        MetaDbUtil.setParameter(2, updateParams, ParameterMethod.setLong, initOpVersion);
                        MetaDbUtil.setParameter(3, updateParams, ParameterMethod.setLong, idVal);
                        MetaDbUtil.update(UPDATE_DATA_ID_STATUS_AND_VALUE_BY_ID, updateParams, connection);
                        DdlMetaLogUtil.logSql(UPDATE_DATA_ID_STATUS_AND_VALUE_BY_ID, updateParams);
                    }
                }
            }

            if (needSetAutoCommitFalse) {
                connection.commit();
                connection.setAutoCommit(true);
            }

            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public void deleteDataId(String dataId) {
        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, dataId);
            DdlMetaLogUtil.logSql(DELETE_DATA_ID, insertParams);
            MetaDbUtil.delete(DELETE_DATA_ID, insertParams, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public void deleteAllDataIdsByInstId(String instId) {
        try {
            DdlMetaLogUtil.logSql(DELETE_ALL_DATA_ID_BY_INST_ID);
            String deleteSql = String.format(DELETE_ALL_DATA_ID_BY_INST_ID, instId);
            MetaDbUtil.delete(deleteSql, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public long updateOpVersion(String dataId) {
        try {
            Map<Integer, ParameterContext> updateParams = MetaDbUtil.buildStringParameters(new String[] {dataId});
            long opVersion = -1;
            DdlMetaLogUtil.logSql(UPDATE_DATA_ID_OPVERSION, updateParams);
            try (PreparedStatement ps = connection.prepareStatement(UPDATE_DATA_ID_OPVERSION,
                Statement.RETURN_GENERATED_KEYS)) {
                if (updateParams != null && updateParams.size() > 0) {
                    for (ParameterContext param : updateParams.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }

                int updateCount = ps.executeUpdate();
                if (updateCount == 1) {
                    try (ResultSet rs = ps.getGeneratedKeys()) {
                        if (rs.next()) {
                            opVersion = rs.getLong(1);
                        } else {
                            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                                "Fail to get op version from generatedKeys");
                        }
                    }
                } else {
                    // maybe dataId is not exists
                    opVersion = -1;
                }
            }
            return opVersion;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

    public void updateMultipleOpVersion(List<String> dataIds) {
        try {
            try (PreparedStatement ps = connection
                .prepareStatement(String.format(UPDATE_DATA_IDS_OPVERSION, concat(dataIds)))) {
                DdlMetaLogUtil.logSql(String.format(UPDATE_DATA_IDS_OPVERSION, concat(dataIds)));
                int updateCount = ps.executeUpdate();
                if (updateCount != dataIds.size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                        "Fail to update all dataIds");
                }
            }
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + CONFIG_LISTENER_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", CONFIG_LISTENER_TABLE,
                e.getMessage());
        }
    }

}
