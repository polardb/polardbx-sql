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

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.rpc.XConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenghui.lch
 */
public class StorageHaChecker {

    // =======HA_SQL_FOR_X-DB=======
    // =======HA_SQL_FOR_RDS80=======

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL =
        "select role, current_leader, instance_type from information_schema.alisql_cluster_local limit 1";

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL_FOR_LOGGER =
        "select INSTANCE_TYPE from information_schema.alisql_cluster_local limit 1";

    protected static final String SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_GLOBAL =
        "select role, ip_port from information_schema.alisql_cluster_global";

    // =======HA_SQL_FOR_OTHERS=======

    protected static final String SELECT_GALAXY_XPORT = "select @@galaxyx_port";
    protected static final String SELECT_POLARX_XPORT = "select @@polarx_port";

    protected static final String SQL_UPGRADE_LEARNER =
        "CALL dbms_consensus.upgrade_learner(?)";
    protected static final String SQL_DOWNGRADE_FOLLOWER =
        "CALL dbms_consensus.downgrade_follower(?)";
    protected static final String SQL_CHANGE_TO_LEADER =
        "CALL dbms_consensus.change_leader(?)";
    protected static final String SQL_CHANGE_ELECTION_WEIGHT =
        "CALL dbms_consensus.configure_follower(?, ?)";

    protected static Map<String, String> HA_CHECKER_JDBC_CONN_PROPS_MAP =
        GmsJdbcUtil.getDefaultConnPropertiesForHaChecker();

    protected static final Map<String, String> HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST =
        GmsJdbcUtil.getDefaultConnPropertiesForHaCheckerFast();

    protected static boolean enablePrintHaCheckTaskLog = true;

    public static void adjustStorageHaTaskConnProps(String key, String val) {
        if (key != null && val != null) {
            HA_CHECKER_JDBC_CONN_PROPS_MAP.put(key, val);
            if (key.equalsIgnoreCase(GmsJdbcUtil.JDBC_CONNECT_TIMEOUT) ||
                key.equalsIgnoreCase(GmsJdbcUtil.JDBC_SOCKET_TIMEOUT)) {
                try {
                    long timeout = Long.parseLong(val);
                    if (timeout > 3000) {
                        timeout = 3000;
                    }
                    HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, Long.toString(timeout));
                } catch (Throwable ignore) {
                    HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, val);
                }
            } else {
                HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST.put(key, val);
            }
        }
    }

    public static void setPrintHaCheckTaskLog(boolean val) {
        enablePrintHaCheckTaskLog = val;
    }

    public static String getHaCheckerJdbcConnPropsUrlStr() {
        return GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(HA_CHECKER_JDBC_CONN_PROPS_MAP);
    }

    public static String getHaCheckerJdbcConnPropsUrlStrFast() {
        return GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(HA_CHECKER_JDBC_CONN_PROPS_MAP_FAST);
    }

    /**
     * Change replica to leader
     */
    public static void changeLeaderOfNode(String storageInstId, String oldLeaderAddr,
                                          String user, String passwd, String newLeaderAddr) throws SQLException {
        changePaxosRoleOfNode(oldLeaderAddr, user, passwd, newLeaderAddr, StorageRole.FOLLOWER, StorageRole.LEADER);
    }

    private static String getRoleChangeSql(StorageRole oldRole, StorageRole newRole) {
        if (oldRole == StorageRole.FOLLOWER && newRole == StorageRole.LEARNER) {
            return SQL_DOWNGRADE_FOLLOWER;
        }
        if (oldRole == StorageRole.LEARNER && newRole == StorageRole.FOLLOWER) {
            return SQL_UPGRADE_LEARNER;
        }
        if (oldRole == StorageRole.FOLLOWER && newRole == StorageRole.LEADER) {
            return SQL_CHANGE_TO_LEADER;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
            String.format("change role from %s to %s", oldRole, newRole));
    }

    /**
     * Change specified replica to role(leader/learner/follower)
     */
    public static void changePaxosRoleOfNode(String currentLeader, String user, String passwd,
                                             String replicaAddress,
                                             StorageRole oldRole, StorageRole newRole) throws SQLException {
        Pair<String, Integer> leaderCurrentAddress = resolveHostPort(currentLeader);
        String sql = getRoleChangeSql(oldRole, newRole);
        String paxosReplicaAddress = AddressUtils.getPaxosAddressByStorageAddress(replicaAddress);

        try (Connection conn = GmsJdbcUtil.createConnection(
            leaderCurrentAddress.getKey(), leaderCurrentAddress.getValue(),
            GmsJdbcUtil.DEFAULT_PHY_DB, StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), user, passwd);
            PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, paxosReplicaAddress);
            stmt.execute();
        }
    }

    /**
     * Change election weight of xpaxos group
     */
    public static void changeElectionWeight(String leaderAddress, String user, String passwd,
                                            String replicaAddress,
                                            int electionWeight) throws SQLException {
        Pair<String, Integer> hostPort = resolveHostPort(leaderAddress);
        String paxosReplicaAddress = AddressUtils.getPaxosAddressByStorageAddress(replicaAddress);

        try (Connection conn = GmsJdbcUtil.createConnection(
            hostPort.getKey(), hostPort.getValue(), GmsJdbcUtil.DEFAULT_PHY_DB,
            StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), user,
            passwd);
            PreparedStatement stmt = conn.prepareStatement(SQL_CHANGE_ELECTION_WEIGHT)) {
            stmt.setString(1, paxosReplicaAddress);
            stmt.setInt(2, electionWeight);
            stmt.execute();
        }
    }

    public static int fetchXPortByAddr(String addr, String usr, String passwd) throws SQLException {
        Pair<String, Integer> hostPort = resolveHostPort(addr);
        String host = hostPort.getKey();
        Integer port = hostPort.getValue();
        try (Connection conn = GmsJdbcUtil
            .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, getHaCheckerJdbcConnPropsUrlStrFast(),
                usr, passwd)) {
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
                XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT : SELECT_POLARX_XPORT)) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            if (msg.contains("Access denied") || msg.contains("Communications link failure") || msg
                .contains("Connection refused")) {
                // maybe is logger, ignore log in meta-db.log
            } else {
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format("Fail to fetch xport from node[%s:%s], err is %s",
                        host, port, ex.getMessage()));
                throw ex;
            }
        }
        return -1;
    }

    /**
     * <pre>
     *     key: storageInstAddr
     *     val: newest of storage inst
     * </pre>
     *
     * @param addrList the addr list that not contain vip
     * @param allowFetchRoleOnlyFromLeader allow return the role from the new hosts (not exists in metaDB yet)
     */
    public static Map<String, StorageNodeHaInfo> checkAndFetchRole(
        List<String> addrList, String vipAddr, int xport, String usr, String passwd, int storageType,
        int storageInstKind, boolean allowFetchRoleOnlyFromLeader) {

        boolean isMasterStorage = true;
        if (storageInstKind == StorageInfoRecord.INST_KIND_SLAVE) {
            isMasterStorage = false;
        }

        // Try to get role by rolling each addr
        Map<String, StorageNodeHaInfo> addrHaInfoMap = new HashMap<>();

        if (storageType != StorageInfoRecord.STORAGE_TYPE_XCLUSTER &&
            storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
            // the storage node is mysql or polardb,
            // e.g in test env, storage node is just mysql or polardb

            // If has vip addr, then use vip addr
            String availableAddr = vipAddr;
            if (availableAddr == null && addrList.size() == 1) {
                // if has no vip addr, then use first storage node info as available addr
                availableAddr = addrList.get(0);
            }

            // For storage master inst, use leader role
            StorageRole role = StorageRole.LEADER;
            if (!isMasterStorage) {
                // For storage slave inst, use learner role
                role = StorageRole.LEARNER;
            }

            if (XConfig.VIP_WITH_X_PROTOCOL) {
                xport = resolveHostPort(availableAddr).getValue();
            } else if (!XConfig.GALAXY_X_PROTOCOL ||
                storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_SINGLE) {
                // Try get xport?
                try {
                    xport = fetchXPortByAddr(availableAddr, usr, passwd); // At most 3s.
                } catch (Throwable t) {
                    // Single node and not connected before, so ignore the error.
                    // Ignore it(log printed).
                    xport = -1;
                    MetaDbLogUtil.META_DB_LOG.warn(
                        String.format("Fail to fetch xport from node[%s] type %d, and set xport=-1",
                            availableAddr, storageType));
                }
            }

            MetaDbLogUtil.META_DB_LOG.info(
                "Non-cluster DN with type: " + storageType + " addr: " + availableAddr + (XConfig.GALAXY_X_PROTOCOL ?
                    " with galaxy" : "") + " xport: " + xport);

            StorageNodeHaInfo storageHaInfo = new StorageNodeHaInfo(availableAddr, role, true, xport);
            addrHaInfoMap.put(availableAddr, storageHaInfo);
            return addrHaInfoMap;
        }

        // Try to fetch storage leader info from all storage nodes (non-vip)
        boolean isFetchLeaderSucc = false;
        for (int i = 0; i < addrList.size(); i++) {
            String addr = addrList.get(i);
            AtomicBoolean isFetchLeader = new AtomicBoolean(false);
            fetchPaxosRoleInfosByAddr(addr, usr, passwd, storageType, isMasterStorage, allowFetchRoleOnlyFromLeader,
                addrHaInfoMap, isFetchLeader);

            if (isFetchLeader.get()) {
                isFetchLeaderSucc = true;
            }

            if (isMasterStorage && allowFetchRoleOnlyFromLeader && isFetchLeaderSucc) {
                break;
            }
        }

        // Try to fetch storage leader info from the vip info of storage inst
        // if it its failed to fetch leader before
        if (!isFetchLeaderSucc && isMasterStorage) {
            // Try to use vip to fetch role infos
            if (vipAddr != null) {
                AtomicBoolean isFetchLeader = new AtomicBoolean(false);
                fetchPaxosRoleInfosByAddr(vipAddr, usr, passwd, storageType, isMasterStorage,
                    allowFetchRoleOnlyFromLeader, addrHaInfoMap, isFetchLeader);
            }
        }

        if (ConfigDataMode.enableSlaveReadForPolarDbX() && storageInstKind == StorageInfoRecord.INST_KIND_MASTER) {
            modifyTheFollowRole(addrHaInfoMap, usr, passwd, storageType);
        }
        return addrHaInfoMap;
    }

    private static Pair<String, Integer> resolveHostPort(String addr) {
        String[] ipPortArr = addr.trim().split(":");
        assert ipPortArr.length == 2;
        String host = ipPortArr[0].trim();
        Integer port = Integer.valueOf(ipPortArr[1].trim());
        return new Pair<>(host, port);
    }

    /**
     * modify the role, because the follow node maybe contain the logger node.
     */
    protected static void modifyTheFollowRole(Map<String, StorageNodeHaInfo> addrHaInfoMap,
                                              String usr,
                                              String passwd,
                                              int storageType) {
        for (StorageNodeHaInfo nodeHaInfo : addrHaInfoMap.values()) {
            if (nodeHaInfo.role == StorageRole.FOLLOWER) {
                Connection conn = null;
                try {
                    String[] ipPortArr = nodeHaInfo.addr.trim().split(":");
                    assert ipPortArr.length == 2;
                    String host = ipPortArr[0].trim();
                    Integer port = Integer.valueOf(ipPortArr[1].trim());
                    conn = GmsJdbcUtil
                        .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB,
                            StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr,
                            passwd);
                    final String queryRoleFromLocalSql = buildQueryRoleSqlFromClusterLocal(storageType);
                    try (Statement loggerStmt = conn.createStatement(); ResultSet allNodeRs = loggerStmt
                        .executeQuery(queryRoleFromLocalSql)) {
                        while (allNodeRs.next()) {
                            String instanceType = allNodeRs.getString("INSTANCE_TYPE");
                            if ("log".equalsIgnoreCase(instanceType)) {
                                nodeHaInfo.setRole(StorageRole.LOGGER);
                            }
                        }
                    } catch (Throwable ex) {
                        MetaDbLogUtil.META_DB_LOG.info(ex);
                    }
                } catch (Throwable ex) {
                    // ignore error of building conn to logger,
                    // logger is NOT allowed to accepting any connections
                    nodeHaInfo.setRole(StorageRole.LOGGER);
                    MetaDbLogUtil.META_DB_LOG.warn(
                        String.format("Fail to get conn from storage node[%s] during check logger", nodeHaInfo.addr),
                        ex);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException ex) {
                            MetaDbLogUtil.META_DB_LOG.info(ex);
                        }
                    }
                }
            }
        }
    }

    /**
     * Fetch all paxos-group role infos by the addr,
     * and save them in output param addrHaInfoMap.
     * <p>
     * <p>
     * <p>
     * Note:
     * The output param addrHaInfoMap may NOT contains any leader info.
     *
     * @return true if the fetch process is successful;
     * false if the the fetch process failed and has any exceptions
     */
    protected static boolean fetchPaxosRoleInfosByAddr(String addr,
                                                       String usr,
                                                       String passwd,
                                                       int storageType,
                                                       boolean isMasterStorage,
                                                       boolean allowFetchRoleOnlyFromLeader,
                                                       Map<String, StorageNodeHaInfo> addrHaInfoMap,
                                                       AtomicBoolean isFetchLeader) {
        Pair<String, Integer> hostPort = resolveHostPort(addr);
        String host = hostPort.getKey();
        Integer port = hostPort.getValue();

        StorageRole roleVal = StorageRole.FOLLOWER;
        if (!isMasterStorage) {
            roleVal = StorageRole.LEARNER;
        }
        int xPort = -1;
        boolean isHealthy;
        final String currentLeaderAddr;
        boolean isSucc = false;

        try (Connection conn = GmsJdbcUtil
            .createConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB,
                StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr, passwd)) {

            // Get role and leader of current node.
            final String currentPaxosLeaderAddr;
            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(buildQueryRoleSqlFromClusterLocal(storageType))) {
                /**
                 * Get Leader by the node of paxos-group, the query sql is:
                 *  "select role, current_leader from information_schema.alisql_cluster_local limit 1"
                 */
                rs.next();
                // Get the itself role for the current node which address is addr
                String role = rs.getString("role");
                // Get the leader addr for the current node which address is addr
                currentPaxosLeaderAddr = rs.getString("current_leader");
                if (role != null) {
                    roleVal = StorageRole.getStorageRoleByString(role);
                }
                isHealthy = true;
            }

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(
                XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT : SELECT_POLARX_XPORT)) {
                if (rs.next()) {
                    xPort = rs.getInt(1);
                }
            } // This should never fail. Or throw an error.

            if (allowFetchRoleOnlyFromLeader) {
                currentLeaderAddr = AddressUtils.getStorageNodeAddrByPaxosNodeAddr(currentPaxosLeaderAddr);
                Pair<String, Integer> leaderIpPort = AddressUtils.getIpPortPairByAddrStr(currentLeaderAddr);
                int leaderXPort = -1;
                boolean canConnectToNewLeaderByPasswd = false;
                /**
                 * Try to connect to new leader and fetch all other paxos-group nodes
                 * by using the passwd of current xcluster (each xcluster has a corresponding random passwd ).
                 *
                 * Note:
                 * If the new leader addr is NOT the true leader of current xcluster,
                 * its must can NOT be connected successfully.
                 *
                 */
                try (Connection leaderConn = GmsJdbcUtil
                    .createConnection(leaderIpPort.getKey(), leaderIpPort.getValue(), GmsJdbcUtil.DEFAULT_PHY_DB,
                        StorageHaChecker.getHaCheckerJdbcConnPropsUrlStr(), usr, passwd)) {
                    /**
                     * Because the leader has been found, so
                     * try to get all nodes of paxos-group by the query sql:
                     *  "select role, ip_port from information_schema.alisql_cluster_global"
                     */
                    boolean peerSucc = false;
                    try (Statement leaderStmt = leaderConn.createStatement(); ResultSet allNodeRs = leaderStmt
                        .executeQuery(buildQueryRoleSqlFromClusterGlobal(storageType))) {
                        // Label the leader can be connected by passwd, the leader is true leader of current xcluster
                        canConnectToNewLeaderByPasswd = true;
                        while (allNodeRs.next()) {
                            String nodeRole = allNodeRs.getString("role");
                            String paxosIpPort = allNodeRs.getString("ip_port");
                            String nodeAddr = AddressUtils.getStorageNodeAddrByPaxosNodeAddr(paxosIpPort);
                            StorageRole nodeRoleVal = StorageRole.getStorageRoleByString(nodeRole);
                            if (StorageRole.LEADER == nodeRoleVal) {
                                // The haInfo of leader should be added at last outside the loop
                                if (currentLeaderAddr != null && nodeAddr.contains(currentLeaderAddr)) {
                                    continue;
                                }
                            }
                            int nodeXPort;
                            if (StorageRole.LOGGER == nodeRoleVal) {
                                nodeXPort = -1; // logger is NOT allowed to accepting any connections
                            } else {
                                try {
                                    nodeXPort = fetchXPortByAddr(nodeAddr, usr, passwd);
                                    // Another 3s consume if follower/learner down.
                                } catch (Throwable ignore) {
                                    // Ignore it.
                                    nodeXPort = -1;
                                    MetaDbLogUtil.META_DB_LOG.warn(
                                        String.format("Fail to fetch xport from node[%s] role %s, and set xport=-1",
                                            nodeAddr, nodeRoleVal.name()));
                                }
                            }
                            StorageNodeHaInfo newStorageHaInfo =
                                new StorageNodeHaInfo(nodeAddr, nodeRoleVal, true, nodeXPort);
                            addrHaInfoMap.putIfAbsent(nodeAddr, newStorageHaInfo);
                        }
                        peerSucc = true;
                    } catch (Throwable ex) {
                        MetaDbLogUtil.META_DB_LOG.info(ex);
                    }

                    // Get xPort of leader.
                    boolean portSucc = false;
                    try (Statement leaderStmt = leaderConn.createStatement();
                        ResultSet rs = leaderStmt.executeQuery(
                            XConfig.GALAXY_X_PROTOCOL ? SELECT_GALAXY_XPORT : SELECT_POLARX_XPORT)) {
                        if (rs.next()) {
                            leaderXPort = rs.getInt(1);
                        }
                        portSucc = true;
                    } catch (Throwable ex) {
                        // Bad server?
                        canConnectToNewLeaderByPasswd = false;
                        MetaDbLogUtil.META_DB_LOG.warn(
                            String.format("Fail to fetch xport from leader node[%s], and set xport=-1",
                                currentLeaderAddr),
                            ex);
                    }

                    isSucc = peerSucc && portSucc;
                } catch (Throwable ex) {
                    MetaDbLogUtil.META_DB_LOG.info(ex);
                }

                if (canConnectToNewLeaderByPasswd) {
                    // Add the true leader info for current xcluster
                    StorageNodeHaInfo leaderStorageHaInfo =
                        new StorageNodeHaInfo(currentLeaderAddr, StorageRole.LEADER, true, leaderXPort);
                    addrHaInfoMap.putIfAbsent(currentLeaderAddr, leaderStorageHaInfo);
                    isFetchLeader.set(true);
                }
                return isSucc;
            } else {
                isSucc = true;
                if (StorageRole.LEADER == roleVal) {
                    isFetchLeader.set(true);
                }
            }
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            if (msg != null && msg.toLowerCase().contains("access denied")) {
                // ignore error of building conn to logger,
                // logger is NOT allowed to accepting any connections
                roleVal = StorageRole.LOGGER;
                isHealthy = true;
            } else if (msg.contains("Communications link failure") || msg.contains("Connection refused")) {
                /**
                 * The addr cannot be connected or connected timeout, treate it as healthy
                 */
                isHealthy = false;
            } else {
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format("Fail to get conn from storage node[%s:%s] during check leader, err is %s", host,
                        port,
                        ex.getMessage()),
                    ex);
                isHealthy = false;
            }
        }
        if (!allowFetchRoleOnlyFromLeader) {
            StorageNodeHaInfo storageHaInfo = new StorageNodeHaInfo(addr, roleVal, isHealthy, xPort);
            addrHaInfoMap.putIfAbsent(addr, storageHaInfo);
        }
        return isSucc;
    }

    protected static String buildQueryRoleSqlFromClusterLocal(int storageType) {
        if (storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
            storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
            return SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_LOCAL;
        }
        // TODO: other cluster.
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Unknown cluster type.");
    }

    protected static String buildQueryRoleSqlFromClusterGlobal(int storageType) {
        if (storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
            storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
            return SELECT_STORAGE_NODE_ROLE_INFO_FROM_ALISQL_CLUSTER_GLOBAL;
        }
        // TODO: other cluster.
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Unknown cluster type.");
    }
}
