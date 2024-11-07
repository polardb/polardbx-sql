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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.NextIdRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class ServerInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ServerInfoAccessor.class);
    private static final String SERVER_INFO_TABLE = GmsSystemTables.SERVER_INFO;

    private static final String SELECT_SERVER_INFO_BY_INST_ID =
        "select * from `" + SERVER_INFO_TABLE + "` where inst_id=? and status!=2 order by id asc";

    private static final String SELECT_ALL_SERVER_INFO =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 order by id asc";

    private static final String SELECT_1_SERVER_TYPE_BY_INST_ID =
        "select inst_type from `" + SERVER_INFO_TABLE + "` where inst_id=? and status!=2 order by id asc limit 1";

    private static final String SELECT_REMOVED_SERVER_INFO_BY_INST_ID =
        "select * from `" + SERVER_INFO_TABLE + "` where inst_id=? and status=2 order by id asc";

    private static final String GET_ALL_REMOVED_COLUMNAR_INST_ID_FOR_SERVER =
        "select t1.inst_id removed_inst_id from \n"
            + "(select inst_id, count(*) cn_cnt from server_info where inst_type=4 group by inst_id ) t1 \n"
            + " inner join\n"
            + "(select inst_id, count(*) rm_cn_cnt from server_info where inst_type=4 and status=2 group by inst_id ) t2 \n"
            + "on t1.inst_id=t2.inst_id and t1.cn_cnt=t2.rm_cn_cnt and t1.cn_cnt>0";

    private static final String SELECT_SERVER_INFO_FOR_MASTER =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 and inst_type = "
            + ServerInfoRecord.INST_TYPE_MASTER;

    private static final String SELECT_SERVER_INFO_FOR_STANDBY =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 and inst_type = "
            + ServerInfoRecord.INST_TYPE_STANDBY;

    private static final String SELECT_SERVER_INFO_FOR_READ_ONLY =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 and inst_type != "
            + ServerInfoRecord.INST_TYPE_MASTER;

    private static final String SELECT_SERVER_INFO_BY_ADDR =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 and ip=? and port=?";

    private static final String SELECT_SERVER_INFO_BY_ADDR_AND_INST_ID =
        "select * from `" + SERVER_INFO_TABLE + "` where status!=2 and ip=? and port=? and inst_id=?";

    private static final String SELECT_NEXT_ID =
        "select `auto_increment` from information_schema.tables where table_schema = database() and table_name = ?";

    private static final String UPDATE_CURRENT_ID = "update `" + SERVER_INFO_TABLE + "` set `id` = ? where `id` = ?";

    private static final String ALTER_AUTO_INCREMENT = "alter table `" + SERVER_INFO_TABLE + "` auto_increment = %s";

    private static final String SELECT_MASTER_INST_ID =
        "select distinct inst_id from " + SERVER_INFO_TABLE + " where status!=2 and inst_type=0;";

    private static final String SELECT_HTAP_LEADRNER_INSTS =
        "select distinct inst_id from " + SERVER_INFO_TABLE + " where status!=2 and inst_type="
            + ServerInfoRecord.INST_TYPE_HTAP_SLAVE;

    private static final String SELECT_COLUMNAR_INSTS =
        "select distinct inst_id from " + SERVER_INFO_TABLE + " where status!=2 and inst_type="
            + ServerInfoRecord.INST_TYPE_COLUMNAR_SLAVE;

    private static final String DELETE_REMOVED_RO_SERVER_INFOS =
        "delete from `" + SERVER_INFO_TABLE + "` where status=2 and inst_type!=0 and inst_id = ?";

    private static final String SQL_UPDATE_SERVER_STATUS =
        "update " + SERVER_INFO_TABLE + " set status = ? where ip=? and port=?";

    public String getMasterInstIdFromMetaDb() {
        String masterInstId = null;
        try (PreparedStatement ps = connection.prepareStatement(SELECT_MASTER_INST_ID)) {
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasNext = rs.next();
                if (hasNext) {
                    masterInstId = rs.getString(1);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return masterInstId;
    }

    public List<ServerInfoRecord> getServerInfoByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil.query(SELECT_SERVER_INFO_BY_INST_ID, params, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<ServerInfoRecord> getAllServerInfo() {
        try {
            return MetaDbUtil.query(SELECT_ALL_SERVER_INFO, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public int getServerTypeByInstId(String instId) {
        int instType = ServerInfoRecord.INST_TYPE_MASTER;
        try (PreparedStatement ps = connection.prepareStatement(SELECT_1_SERVER_TYPE_BY_INST_ID)) {
            ps.setString(1, instId);
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasNext = rs.next();
                if (hasNext) {
                    instType = rs.getInt(1);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return instType;
    }

    public List<ServerInfoRecord> getRemovedServerInfoByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil.query(SELECT_REMOVED_SERVER_INFO_BY_INST_ID, params, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public Set<String> getAllHTAPReadOnlyInstIdList() {
        String instId = null;
        Set<String> allHtapInstIdList = new HashSet<>();
        try (PreparedStatement ps = connection.prepareStatement(SELECT_HTAP_LEADRNER_INSTS)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    instId = rs.getString(1);
                    allHtapInstIdList.add(instId);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return allHtapInstIdList;
    }

    public synchronized Set<String> loadColumnarInstIdAndUpdate(Connection connection) {
        Set<String> allColumnarInstIds = new HashSet<>();
        try {
            Set<String> allColumnarInstIdList = new HashSet<>();
            try (PreparedStatement ps = connection.prepareStatement(SELECT_COLUMNAR_INSTS)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String instId = rs.getString(1);
                        allColumnarInstIdList.add(instId);
                    }
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
            return allColumnarInstIdList;
        } finally {
            if (allColumnarInstIds.size() == 0) {
                DynamicConfig.getInstance().existColumnarNodes(false);
            }
        }
    }

    public List<String> getAllRemovedColumnarReadOnlyInstIdList() {
        String instId = null;
        List<String> allRemovedRoInstIdList = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(GET_ALL_REMOVED_COLUMNAR_INST_ID_FOR_SERVER)) {
            try (ResultSet rs = ps.executeQuery()) {
                boolean hasNext = rs.next();
                if (hasNext) {
                    instId = rs.getString(1);
                    allRemovedRoInstIdList.add(instId);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return allRemovedRoInstIdList;
    }

    public List<ServerInfoRecord> getServerInfoForMaster() {
        try {
            return MetaDbUtil.query(SELECT_SERVER_INFO_FOR_MASTER, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "' for master nodes", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query for master nodes",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<ServerInfoRecord> getServerInfoForStandby() {
        try {
            return MetaDbUtil.query(SELECT_SERVER_INFO_FOR_STANDBY, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "' for master nodes", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query for standby nodes",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<ServerInfoRecord> getServerInfoForReadOnly() {
        try {
            return MetaDbUtil.query(SELECT_SERVER_INFO_FOR_READ_ONLY, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "' for read-only nodes", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query for read-only nodes",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<ServerInfoRecord> getServerInfoByAddr(String ip, Integer port) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, ip);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, port);
            return MetaDbUtil.query(SELECT_SERVER_INFO_BY_ADDR, params, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<ServerInfoRecord> getServerInfoByAddrAndInstId(String ip, Integer port, String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, ip);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, port);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);
            return MetaDbUtil.query(SELECT_SERVER_INFO_BY_ADDR_AND_INST_ID, params, ServerInfoRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<NextIdRecord> getNextId() {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(new String[] {SERVER_INFO_TABLE});
            return MetaDbUtil.query(SELECT_NEXT_ID, params, NextIdRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query next auto_increment id for the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "query next auto_increment id for",
                SERVER_INFO_TABLE, e.getMessage());
        }
    }

    public long updateCurrentId(long origId, long newId, String ip, Integer port) {
        long newIdFromAnotherNode = 0L;
        try {
            connection.setAutoCommit(false);

            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, newId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, origId);

            int updateCount = MetaDbUtil.update(UPDATE_CURRENT_ID, params, connection);

            if (updateCount <= 0) {
                // Another node updates it prior to us, so let's get current id instead.
                List<ServerInfoRecord> records = getServerInfoByAddr(ip, port);
                if (records != null && records.size() > 0) {
                    newIdFromAnotherNode = records.get(0).id;
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch", "Not found the server " + ip);
                }
            } else {
                // Update id successfully, and we must alter table with new auto_increment as well.
                alterAutoIncrement(newId);
            }

            connection.commit();

            return newIdFromAnotherNode;
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                logger.error("Failed to rollback for current id update");
            }
            logger.error("Failed to query next id from the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query next id from",
                SERVER_INFO_TABLE, e.getMessage());
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.error("Failed to set AutoCommit back to ON");
            }
        }
    }

    private void alterAutoIncrement(long newId) {
        try {
            MetaDbUtil.executeDDL(String.format(ALTER_AUTO_INCREMENT, newId), connection);
        } catch (Exception e) {
            logger.error(
                "Failed to alter auto_increment to " + newId + " for the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "alter auto_increment to " + newId + " for",
                SERVER_INFO_TABLE, e.getMessage());
        }
    }

    public void clearRemovedReadOnlyServerInfosByInstId(String instId) {

        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, instId);
            MetaDbUtil.delete(DELETE_REMOVED_RO_SERVER_INFOS, insertParams, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", SERVER_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateServerStatusByIpPort(String ip, int port, int status) {
        try {
            Map<Integer, ParameterContext> updateParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, updateParams, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, updateParams, ParameterMethod.setString, ip);
            MetaDbUtil.setParameter(3, updateParams, ParameterMethod.setInt, port);
            MetaDbUtil.update(SQL_UPDATE_SERVER_STATUS, updateParams, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to update the system table '" + SERVER_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", SERVER_INFO_TABLE,
                e.getMessage());
        }
    }
}
