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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static final String SELECT_ALIVE_STORAGE_INFOS_BY_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where inst_id=? and status!=2 order by id";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_TYPE =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE + "` where inst_kind=? order by id asc";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where storage_inst_id=? and status!=2";

    private static final String SELECT_STORAGE_INFO =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE + "` where status!=2";

    private static final String SELECT_STORAGE_INFO_BY_STORAGE_INST_ID_LIST =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where storage_inst_id in (%s) and status!=2 order by storage_inst_id";

    private static final String SELECT_SLAVE_STORAGE_INFOS_BY_MASTER_STORAGE_INST_ID =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from `" + STORAGE_INFO_TABLE
            + "` where inst_kind=1 and status!=2 and storage_master_inst_id=?";

    private static final String SELECT_STORAGE_INFO_BY_INST_ID_AND_TYPE =
        "select " + ALL_STORAGE_INFO_COLUMNS + " from " + TStringUtil.backQuote(STORAGE_INFO_TABLE)
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
        "delete from storage_info where storage_inst_id=? and ip=? and port=? and is_vip=0;";

    private static final String GET_MASTER_INST_ID_FOR_SERVER =
        "select distinct inst_id, inst_kind from storage_info where status!=2 and inst_kind=0 limit 1";

    private static final String GET_ALL_INST_ID_FOR_SERVER =
        "select distinct inst_id, storage_inst_id from storage_info where status!=2";

    private static final String GET_ALL_REMOVED_RO_INST_ID_FOR_SERVER =
        "select t1.inst_id removed_inst_id, dn_cnt all_dn_cnt, rm_dn_cnt removed_dn_cnt  from \n"
            + "(select inst_id, count(distinct storage_inst_id) dn_cnt from storage_info where inst_kind=1 group by inst_id ) t1 \n"
            + " inner join\n"
            + "(select inst_id, count(distinct storage_inst_id) rm_dn_cnt from storage_info where inst_kind=1 and status=2 group by inst_id ) t2 \n"
            + "on t1.inst_id=t2.inst_id and t1.dn_cnt=t2.rm_dn_cnt and t1.dn_cnt>0";

    private static final String DELETE_REMOVED_RO_STORAGE_INFO_BY_SERVER_INST_ID =
        "delete from storage_info where status=2 and inst_kind=1 and inst_id=?";

    private static final String UPDATE_STORAGE_STATUS =
        "update storage_info set status=? where storage_master_inst_id=?";

    private static final String UPDATE_STORAGE_POOL_NAME =
        "update storage_info set extras=? where storage_master_inst_id=?";

    private static final String UPDATE_STORAGE_INFO_DELETABLE =
        "update storage_info set deletable=? where storage_master_inst_id=?";
    private static final String COUNT_ALL_RW_STORAGES =
        "select count(distinct storage_inst_id) as rw_dn_cnt from storage_info where status=0 and inst_kind=0";

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
                    String.format("Failed to update system table %s: no instId=%s exists", STORAGE_INFO_TABLE, instId));
            }
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to update the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateStoragePoolName(String storageInst, StorageInfoExtraFieldJSON extras) {
        try {
            Map<Integer, ParameterContext> updateParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, updateParams, ParameterMethod.setString,
                StorageInfoExtraFieldJSON.toJson(extras));
            MetaDbUtil.setParameter(2, updateParams, ParameterMethod.setString, storageInst);
            int affected = MetaDbUtil.update(UPDATE_STORAGE_POOL_NAME, updateParams, connection);
            if (affected == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                    String.format("Failed to update system table %s: no instId=%s exists",
                        STORAGE_INFO_TABLE, storageInst));
            }
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to update the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateStorageInfoDeletable(String storageInst, boolean deletable) {
        try {
            Map<Integer, ParameterContext> updateParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, updateParams, ParameterMethod.setBoolean, deletable);
            MetaDbUtil.setParameter(2, updateParams, ParameterMethod.setString, storageInst);
            int affected = MetaDbUtil.update(UPDATE_STORAGE_INFO_DELETABLE, updateParams, connection);
            if (affected == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                    String.format("Failed to update system table %s: no instId=%s exists",
                        STORAGE_INFO_TABLE, storageInst));
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
            MetaDbUtil.setParameter(19, insertParams, ParameterMethod.setString,
                StorageInfoExtraFieldJSON.toJson(storageInfoRecord.extras));
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
            return MetaDbUtil.query(SELECT_STORAGE_INFO_BY_STORAGE_TYPE, params, StorageInfoRecord.class,
                this.connection);
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
            return MetaDbUtil.query(SELECT_STORAGE_INFO_BY_STORAGE_INST_ID, params, StorageInfoRecord.class,
                this.connection);
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

            List<StorageInfoRecord> storageInfoRecords =
                MetaDbUtil.query(selectSql, params, StorageInfoRecord.class, this.connection);

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
            return MetaDbUtil.query(SELECT_SLAVE_STORAGE_INFOS_BY_MASTER_STORAGE_INST_ID, params,
                StorageInfoRecord.class, this.connection);
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
            return MetaDbUtil.query(SELECT_STORAGE_INFOS_BY_INST_ID, params, StorageInfoRecord.class, this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getAliveStorageInfosByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            return MetaDbUtil.query(SELECT_ALIVE_STORAGE_INFOS_BY_INST_ID, params, StorageInfoRecord.class,
                this.connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + STORAGE_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", STORAGE_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<StorageInfoRecord> getAliveStorageInfos() {
        try {
            return MetaDbUtil.query(SELECT_STORAGE_INFO, null, StorageInfoRecord.class, this.connection);
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

            return MetaDbUtil.query(SELECT_STORAGE_INFO_BY_INST_ID_AND_TYPE, params, StorageInfoRecord.class,
                this.connection);
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

    public Map<String, Set<String>> getServerStorageInstIdMapFromStorageInfo() {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(GET_ALL_INST_ID_FOR_SERVER);
                rs = stmt.executeQuery();
                Map<String, Set<String>> instId2StorageIds = new HashMap<>();
                while (rs.next()) {
                    String instId = rs.getString(1);
                    String storageId = rs.getString(2);
                    if (!instId2StorageIds.containsKey(instId)) {
                        instId2StorageIds.put(instId, new HashSet<String>());
                    }
                    instId2StorageIds.get(instId).add(storageId);
                }
                return instId2StorageIds;
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

    public List<StorageInfoRecord> getStorageInfoForReadOnly(Set<String> instIds) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(SELECT_STORAGE_INFO);
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
        try {
            return MetaDbUtil.query(sqlBuilder.toString(), StorageInfoRecord.class, connection);
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
                List<String> removedRoInstIdList = Lists.newArrayList();
                while (rs.next()) {
                    removedRoInstIdList.add(rs.getString("removed_inst_id"));
                }
                return removedRoInstIdList;
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

    public long countAllRwStorageInsts() {
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = this.connection.prepareStatement(COUNT_ALL_RW_STORAGES);
                rs = stmt.executeQuery();
                long rwDnCnt = 0;
                while (rs.next()) {
                    rwDnCnt = rs.getLong("rw_dn_cnt");
                }
                return rwDnCnt;
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
