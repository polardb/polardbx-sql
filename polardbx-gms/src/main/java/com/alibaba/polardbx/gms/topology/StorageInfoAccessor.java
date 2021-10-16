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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class StorageInfoAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(StorageInfoAccessor.class);

    private static final String STORAGE_INFO_TABLE = GmsSystemTables.STORAGE_INFO;

    private static final String ALL_STORAGE_INFO_COLUMNS =
        "id, gmt_created, gmt_modified, inst_id, storage_inst_id, storage_master_inst_id, ip, port, xport, user, passwd_enc, storage_type, inst_kind, status, region_id, azone_id, idc_id, max_conn, cpu_core, mem_size, is_vip, extras";

    private static final String SELECT_STORAGE_INFOS_BY_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE + "` where inst_id=? order by id";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_TYPE =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE + "` where inst_kind=? order by id asc";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where storage_inst_id=? and status!=2";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_INST_ID_LIST =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where storage_inst_id in (%s) and status!=2 order by storage_inst_id";

    private static final String SELECT_SLAVE_STORAGE_INFOS_BY_MASTER_STORAGE_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where inst_kind=1 and status!=2 and storage_master_inst_id=?";

    private static final String SELECT_STORAGE_INFO_BY_INST_ID_AND_TYPE =
        "select " + ALL_STORAGE_INFO_COLUMNS
            + " from " + TStringUtil.backQuote(STORAGE_INFO_TABLE)
            + " where inst_id=? and inst_kind=? and status!=2";

    private static final String SELECT_STORAGE_ID_LIST_BY_INST_ID_AND_INST_TYPE =
        "select distinct storage_inst_id from `" + STORAGE_INFO_TABLE
            + "` where status=0 and inst_id=? and `inst_kind`=?";

    private static final String SELECT_RO_STORAGE_INFO_LIST_BY_INST_ID_AND_INST_TYPE =
        "select distinct storage_inst_id, storage_master_inst_id from `" + STORAGE_INFO_TABLE
            + "` where status=0 and `inst_kind`=1 and inst_id=?";

    private static final String INSERT_IGNORE_NEW_STORAGE_INFO =
        "insert ignore into storage_info (" + ALL_STORAGE_INFO_COLUMNS
            + ") VALUES (null, now(), now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String DELETE_STORAGE_INFO_BY_INST_ID_IP_PORT =
        "delete from storage_info where storage_inst_id=? and ip=? and port=?;";

    private static final String GET_MASTER_INST_ID_FOR_SERVER =
        "select distinct inst_id, inst_kind from storage_info where status!=2 and inst_kind=0 limit 1";

    private static final String GET_SLAVE_ALL_INST_ID_FOR_SERVER =
        "select distinct inst_id, inst_kind from storage_info where status!=2 and inst_kind=1";

    private static final String GET_ALL_REMOVED_RO_INST_ID_FOR_SERVER =
        "select distinct inst_id, inst_kind from storage_info where status=2 and inst_kind=1";

    private static final String DELETE_REMOVED_RO_STORAGE_INFO_BY_SERVER_INST_ID =
        "delete from storage_info where status=2 and inst_kind=1 and inst_id=?";

    private static final String UPDATE_STORAGE_STATUS =
        "update storage_info set status=? where storage_master_inst_id=?";

    public void removeStorageInfo(String storageInstId, String ip, Integer port) {

        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, storageInstId);
            MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setString, ip);
            MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setInt, port);
            MetaDbUtil.delete(DELETE_STORAGE_INFO_BY_INST_ID_IP_PORT, insertParams, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public void clearRemovedReadOnlyStorageInfosByInstId(String instId) {

        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, instId);
            MetaDbUtil.delete(DELETE_REMOVED_RO_STORAGE_INFO_BY_SERVER_INST_ID, insertParams, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    /**
     * Update status of a storage node
     */
    public void updateStorageStatus(String instId, int status) {
        try {
            Map<Integer, ParameterContext> updateParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, updateParams, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, updateParams, ParameterMethod.setString, instId);
            int affected = MetaDbUtil.update(UPDATE_STORAGE_STATUS, updateParams, connection);
            if (affected == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                    String.format("Failed to update system table %s: no instId=%s exists",
                        STORAGE_INFO_TABLE, instId));
            }
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to update the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public void addStorageInfo(StorageInfoRecord storageInfoRecord) {
        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            // instId
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, storageInfoRecord.instId);
            // storage_inst_id
            MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setString, storageInfoRecord.storageInstId);
            // storage_master_inst_id
            MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setString, storageInfoRecord.storageMasterInstId);
            // ip
            MetaDbUtil.setParameter(4, insertParams, ParameterMethod.setString, storageInfoRecord.ip);
            // port
            MetaDbUtil.setParameter(5, insertParams, ParameterMethod.setInt, storageInfoRecord.port);
            // xport
            MetaDbUtil.setParameter(6, insertParams, ParameterMethod.setInt, storageInfoRecord.xport);
            // user
            MetaDbUtil.setParameter(7, insertParams, ParameterMethod.setString, storageInfoRecord.user);
            // passwd_enc
            MetaDbUtil.setParameter(8, insertParams, ParameterMethod.setString, storageInfoRecord.passwdEnc);
            // storage_type
            MetaDbUtil.setParameter(9, insertParams, ParameterMethod.setInt, storageInfoRecord.storageType);
            // inst_kind
            MetaDbUtil.setParameter(10, insertParams, ParameterMethod.setInt, storageInfoRecord.instKind);
            // status
            MetaDbUtil.setParameter(11, insertParams, ParameterMethod.setInt, storageInfoRecord.status);
            // region_id
            MetaDbUtil.setParameter(12, insertParams, ParameterMethod.setString, storageInfoRecord.regionId);
            // azone_id
            MetaDbUtil.setParameter(13, insertParams, ParameterMethod.setString, storageInfoRecord.azoneId);
            // idc_id
            MetaDbUtil.setParameter(14, insertParams, ParameterMethod.setString, storageInfoRecord.idcId);
            // max_conn
            MetaDbUtil.setParameter(15, insertParams, ParameterMethod.setInt, storageInfoRecord.maxConn);
            // cpu_core
            MetaDbUtil.setParameter(16, insertParams, ParameterMethod.setInt, storageInfoRecord.cpuCore);
            // mem_size
            MetaDbUtil.setParameter(17, insertParams, ParameterMethod.setInt, storageInfoRecord.memSize);
            // is_vip
            MetaDbUtil.setParameter(18, insertParams, ParameterMethod.setInt, storageInfoRecord.isVip);
            // extras
            MetaDbUtil.setParameter(19, insertParams, ParameterMethod.setString, storageInfoRecord.extras);
            MetaDbUtil.insert(INSERT_IGNORE_NEW_STORAGE_INFO, insertParams, connection);
            return;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getStorageInfosByInstKind(int instKind) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, instKind);
            return MetaDbUtil
                .query(SELECT_STORAGE_INFO_BY_STORAGE_TYPE, params, StorageInfoRecord.class, this.connection);
        } catch (Exception e) {
            if (!e.getMessage().contains("doesn't exist")) {
                logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getStorageInfosByStorageInstId(String storageInstId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, storageInstId);
            return MetaDbUtil
                .query(SELECT_STORAGE_INFO_BY_STORAGE_INST_ID, params, StorageInfoRecord.class, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    /**
     * key : storage_inst_id
     * val : list of storage node info for one storage_inst_id
     */
    public Map<String, List<StorageInfoRecord>> getStorageInfosByStorageInstIdList(List<String> storageInstIdList) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            String idListStr = "";
            for (int i = 0; i < storageInstIdList.size(); i++) {
                String storageInstId = storageInstIdList.get(i);
                if (i > 0) {
                    idListStr += ",";
                }
                idListStr += String.format("'%s'", storageInstId);
            }
            String selectSql = String.format(SELECT_STORAGE_INFO_BY_STORAGE_INST_ID_LIST, idListStr);

            List<StorageInfoRecord> storageInfoRecords = MetaDbUtil
                .query(selectSql, params, StorageInfoRecord.class, this.connection);

            Map<String, List<StorageInfoRecord>> storageInfoRecMap = new HashMap<>();
            for (int i = 0; i < storageInfoRecords.size(); i++) {
                StorageInfoRecord rec = storageInfoRecords.get(i);
                storageInfoRecMap.computeIfAbsent(rec.storageInstId, k -> new ArrayList<>()).add(rec);
            }

            return storageInfoRecMap;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getSlaveStorageInfosByMasterStorageInstId(String masterStorageInstId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, masterStorageInstId);
            return MetaDbUtil
                .query(SELECT_SLAVE_STORAGE_INFOS_BY_MASTER_STORAGE_INST_ID, params, StorageInfoRecord.class,
                    this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getStorageInfosByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil
                .query(SELECT_STORAGE_INFOS_BY_INST_ID, params, StorageInfoRecord.class,
                    this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getStorageInfosByInstIdAndKind(String instId, int instKind) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, instKind);

            return MetaDbUtil.query(SELECT_STORAGE_INFO_BY_INST_ID_AND_TYPE,
                params, StorageInfoRecord.class, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public String getServerMasterInstIdFromStorageInfo() {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(GET_MASTER_INST_ID_FOR_SERVER);
                rs = stmt.executeQuery();
                List<String> storageInstIdList = Lists.newArrayList();
                while (rs.next()) {
                    storageInstIdList.add(rs.getString(1));
                }
                if (storageInstIdList.size() == 0) {
                    return null;
                }
                return storageInstIdList.get(0);
            } catch (Throwable ex) {
                throw ex;
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<String> getServerReadOnlyInstIdSetFromStorageInfo() {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(GET_SLAVE_ALL_INST_ID_FOR_SERVER);
                rs = stmt.executeQuery();
                List<String> storageInstIdList = Lists.newArrayList();
                while (rs.next()) {
                    storageInstIdList.add(rs.getString(1));
                }
                return storageInstIdList;
            } catch (Throwable ex) {
                throw ex;
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<String> getStorageIdListByInstIdAndInstKind(String instId, int instKind) {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(SELECT_STORAGE_ID_LIST_BY_INST_ID_AND_INST_TYPE);
                stmt.setString(1, instId);
                stmt.setInt(2, instKind);
                rs = stmt.executeQuery();
                List<String> storageInstIdList = Lists.newArrayList();
                while (rs.next()) {
                    storageInstIdList.add(rs.getString(1));
                }
                return storageInstIdList;
            } catch (Throwable ex) {
                throw ex;
            } finally {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<String> getAllRemovedReadOnlyInstIdList() {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(GET_ALL_REMOVED_RO_INST_ID_FOR_SERVER);
                rs = stmt.executeQuery();
                List<String> storageInstIdList = Lists.newArrayList();
                while (rs.next()) {
                    storageInstIdList.add(rs.getString(1));
                }
                return storageInstIdList;
            } catch (Throwable ex) {
                throw ex;
            } finally {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<Pair<String, String>> getReadOnlyStorageInfoListByInstIdAndInstKind(String instId) {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(SELECT_RO_STORAGE_INFO_LIST_BY_INST_ID_AND_INST_TYPE);
                stmt.setString(1, instId);
                rs = stmt.executeQuery();
                List<Pair<String, String>> roAndRwPairList = Lists.newArrayList();
                while (rs.next()) {
                    String roInstId = rs.getString(1);
                    String rwInstId = rs.getString(2);
                    Pair<String, String> roInstIdAndRwIdPair = new Pair<String, String>(roInstId, rwInstId);
                    roAndRwPairList.add(roInstIdAndRwIdPair);
                }
                return roAndRwPairList;
            } catch (Throwable ex) {
                throw ex;
            } finally {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }
            }

        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }
}
