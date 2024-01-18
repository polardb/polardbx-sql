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

package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.record.NextIdRecord;
import com.alibaba.polardbx.gms.sync.GmsSyncDataSource;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GmsNodeManager extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerInit.TDDL_DYNAMIC_CONFIG;

    private static final int NODE_ID_SPACE = 1024;

    private static final String SEPARATOR_COLON = TddlNode.SEPARATOR_INNER;
    private static final String SEPARATOR_SEMICOLON = ";";
    private static final String SEPARATOR_COMMA = ", ";
    private static final String SEPARATOR_LINE = "\n";

    private static final GmsNodeManager INSTANCE = new GmsNodeManager();

    private static final Map<String, GmsSyncDataSource> dataSources = new ConcurrentHashMap<>();

    private final ServerInfoAccessor serverInfoAccessor;

    /**
     * Current node.
     */
    private volatile GmsNode localNode = null;

    /**
     * Remote nodes only in current instance.
     */
    private final List<GmsNode> remoteNodes = new ArrayList<>();

    /**
     * All master nodes
     */
    private final List<GmsNode> masterNodes = new ArrayList<>();

    /**
     * All standby nodes
     */
    private final List<GmsNode> standbyNodes = new ArrayList<>();

    /**
     * All read-only nodes by instance id.
     */
    private final Map<String, List<GmsNode>> readOnlyNodesByInstId = new ConcurrentHashMap<>();

    /**
     * All read-ony nodes.
     */
    private final List<GmsNode> readOnlyNodes = new ArrayList<>();

    /**
     * All the nodes including master and read-only, it is sorted by node index.
     */
    private final List<GmsNode> allNodes = new ArrayList<>();

    private int currentIndex = -1;
    /**
     * FIXME 暂时先认为只读实例的cpu cores都是一样的
     */
    private volatile int readOnlyNodeCpuCore = -1;

    private GmsNodeManager() {
        this.serverInfoAccessor = new ServerInfoAccessor();
    }

    public static GmsNodeManager getInstance() {
        INSTANCE.init();
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    public GmsNode getLocalNode() {
        return localNode;
    }

    public List<GmsNode> getRemoteNodes() {
        return remoteNodes;
    }

    public List<GmsNode> getStandbyNodes() {
        return standbyNodes;
    }

    public List<GmsNode> getMasterNodes() {
        return masterNodes;
    }

    public Map<String, List<GmsNode>> getReadOnlyNodesByInstId() {
        return readOnlyNodesByInstId;
    }

    public List<GmsNode> getReadOnlyNodes() {
        return readOnlyNodes;
    }

    public List<GmsNode> getAllNodes() {
        return allNodes;
    }

    public int getCurrentIndex() {
        if (ConfigDataMode.isFastMock()) {
            return 1;
        }
        return currentIndex;
    }

    public List<GmsNode> getAllTrustedNodes() {
        return allNodes;
    }

    public List<GmsNode> getNodesBySyncScope(SyncScope scope) {
        switch (scope) {
        case ALL:
            return getAllNodes();
        case MASTER_ONLY:
            return getMasterNodes();
        case SLAVE_ONLY:
            return getReadOnlyNodes();
        case CURRENT_ONLY:
        default:
            return remoteNodes;
        }
    }

    public boolean isCurrentNodeMaster() {
        return ServerInstIdManager.getInstance().isMasterInst();
    }

    public boolean isCurrentNodeReadOnly() {
        return !ServerInstIdManager.getInstance().isMasterInst();
    }

    public void reloadNodes(int localServerPort) {
        // Clean up sync data sources.
        clearDataSources();

        // Load various types of nodes.
        loadNodesForCurrentInstance(localServerPort);
        loadMasterNodes();
        loadReadOnlyNodes();
        loadStandbyNodes();
        loadRedundantNodes();

        // Refresh node info which relies on node change.
        refreshNodeIdList();
        refreshLocalGmsNode(localServerPort);

        // Print the latest node list.
        printGmsNode();
    }

    private void clearDataSources() {
        for (GmsSyncDataSource dataSource : dataSources.values()) {
            try {
                dataSource.destroy();
            } catch (Exception ignored) {
            }
        }
        dataSources.clear();
    }

    private void loadNodesForCurrentInstance(int localServerPort) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            serverInfoAccessor.setConnection(metaDbConn);

            // Fetch the latest server info for current instance.
            List<ServerInfoRecord> records = serverInfoAccessor.getServerInfoByInstId(InstIdUtil.getInstId());

            // Clear loaded nodes.
            localNode = null;
            remoteNodes.clear();

            // Reload new ones.
            Set<Integer> assignedUniqueIds = new HashSet<>();
            for (ServerInfoRecord record : records) {
                if (record.status == ServerInfoRecord.SERVER_STATUS_READY) {
                    int uniqueId = assignUniqueId(record, assignedUniqueIds);
                    if (TStringUtil.equalsIgnoreCase(record.ip, AddressUtils.getHostIp())
                        && record.port == localServerPort) {
                        localNode = buildNode(record, uniqueId);
                    } else {
                        remoteNodes.add(buildNode(record, uniqueId));
                    }
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            serverInfoAccessor.setConnection(null);
        }
    }

    private void loadMasterNodes() {
        if (isCurrentNodeReadOnly()) {
            // Only read-only node maintains master node list for authentication.
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                serverInfoAccessor.setConnection(metaDbConn);

                // Fetch the latest master node info.
                List<ServerInfoRecord> records = serverInfoAccessor.getServerInfoForMaster();

                // Clear loaded master nodes.
                masterNodes.clear();

                // Reload new ones.
                Set<Integer> assignedUniqueIds = new HashSet<>();
                for (ServerInfoRecord record : records) {
                    if (record.status == ServerInfoRecord.SERVER_STATUS_READY) {
                        int uniqueId = assignUniqueId(record, assignedUniqueIds);
                        masterNodes.add(buildNode(record, uniqueId));
                    }
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            } finally {
                serverInfoAccessor.setConnection(null);
            }
        } else {
            masterNodes.clear();
            if (localNode != null) {
                masterNodes.add(localNode);
            }
            masterNodes.addAll(remoteNodes);
        }
    }

    private void loadReadOnlyNodes() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            serverInfoAccessor.setConnection(metaDbConn);

            // Only master node maintains read-only node list for sync.
            List<ServerInfoRecord> records = serverInfoAccessor.getServerInfoForReadOnly();

            // Clear loaded read-only nodes.
            readOnlyNodesByInstId.clear();

            // Reload new ones.
            Set<Integer> assignedUniqueIds = new HashSet<>();
            for (ServerInfoRecord record : records) {
                if (record.status == ServerInfoRecord.SERVER_STATUS_READY) {
                    List<GmsNode> readOnlyNodes =
                        readOnlyNodesByInstId.computeIfAbsent(record.instId, k -> new ArrayList<>());
                    int uniqueId = assignUniqueId(record, assignedUniqueIds);
                    readOnlyNodes.add(buildNode(record, uniqueId));
                    this.readOnlyNodeCpuCore = record.cpuCore;
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            serverInfoAccessor.setConnection(null);
        }
    }

    private void loadStandbyNodes() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            serverInfoAccessor.setConnection(metaDbConn);

            // Fetch the latest standby node info.
            List<ServerInfoRecord> records = serverInfoAccessor.getServerInfoForStandby();

            // Clear loaded standby nodes.
            standbyNodes.clear();

            // Reload new ones.
            Set<Integer> assignedUniqueIds = new HashSet<>();
            for (ServerInfoRecord record : records) {
                if (record.status == ServerInfoRecord.SERVER_STATUS_READY) {
                    int uniqueId = assignUniqueId(record, assignedUniqueIds);
                    standbyNodes.add(buildNode(record, uniqueId));
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            serverInfoAccessor.setConnection(null);
        }
    }

    private void loadRedundantNodes() {
        // All read-only nodes.
        readOnlyNodes.clear();
        for (List<GmsNode> nodes : readOnlyNodesByInstId.values()) {
            readOnlyNodes.addAll(nodes);
        }

        // All the nodes.
        allNodes.clear();
        allNodes.addAll(masterNodes);
        allNodes.addAll(readOnlyNodes);
        allNodes.addAll(standbyNodes);
        if (allNodes.isEmpty()) {
            // Need one node at least for local test even if it's null.
            allNodes.add(localNode);
        }
        allNodes.sort((gmsNode1, gmsNode2) -> {
            if (gmsNode1.instId.equals(gmsNode2.instId)) {
                return Integer.compare(gmsNode1.uniqueId, gmsNode2.uniqueId);
            }
            return gmsNode1.instId.compareTo(gmsNode2.instId);
        });
        this.currentIndex = allNodes.indexOf(GmsNodeManager.getInstance().getLocalNode());
        if (currentIndex == -1) {
            LOGGER.error(String.format(
                "local node not found from allNodes, local node is %s, while all node is %s",
                GmsNodeManager.getInstance().getLocalNode(),
                allNodes.stream().map(node -> node.toString()).collect(Collectors.joining(",")))
            );
        }
    }

    private GmsNode buildNode(ServerInfoRecord record, int uniqueId) {
        GmsNode node = new GmsNode();
        node.origId = record.id;
        node.uniqueId = uniqueId;
        node.host = record.ip;
        node.serverPort = record.port;
        node.managerPort = record.mgrPort;
        node.rpcPort = record.mppPort;
        node.status = record.status;
        node.instId = record.instId;
        node.instType = record.instType;
        node.cpuCore = record.cpuCore;
        return node;
    }

    private int assignUniqueId(ServerInfoRecord record, Set<Integer> assignedUniqueIds) {
        long nextId = record.id;
        int uniqueId = generateUniqueId(nextId);

        boolean renewId = false;

        if (assignedUniqueIds.contains(uniqueId)) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                serverInfoAccessor.setConnection(metaDbConn);
                // The unique id conflicts with existing one.
                List<NextIdRecord> records = serverInfoAccessor.getNextId();
                if (records != null && records.size() > 0) {
                    nextId = records.get(0).nextId;
                    uniqueId = generateUniqueId(nextId);
                    renewId = true;
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch",
                        "Failed to fetch next auto_increment id for node " + record.ip + SEPARATOR_COLON + record.port);
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            } finally {
                serverInfoAccessor.setConnection(null);
            }
        }

        while (assignedUniqueIds.contains(uniqueId)) {
            // Increase until no conflict.
            nextId = ++uniqueId;
            uniqueId = generateUniqueId(nextId);
        }

        if (renewId) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                serverInfoAccessor.setConnection(metaDbConn);
                // Update new id for current record.
                long newId = serverInfoAccessor.updateCurrentId(record.id, nextId, record.ip, record.port);
                if (newId > 0) {
                    record.id = newId;
                    uniqueId = generateUniqueId(newId);
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            } finally {
                serverInfoAccessor.setConnection(null);
            }
        }

        // Finally get a valid unique id.
        assignedUniqueIds.add(uniqueId);

        return uniqueId;
    }

    private int generateUniqueId(long id) {
        return (int) id % NODE_ID_SPACE;
    }

    private void refreshNodeIdList() {
        List<Integer> uniqueIdList = new ArrayList<>();
        Map<Integer, String> uniqueIdKeyMapping = new ConcurrentHashMap<>();

        if (localNode != null) {
            uniqueIdList.add(localNode.uniqueId);
            uniqueIdKeyMapping.put(localNode.uniqueId, localNode.getServerKey());
        }

        for (GmsNode nodeInfo : remoteNodes) {
            if (nodeInfo != null) {
                GmsNode remoteNode = (GmsNode) nodeInfo;
                uniqueIdList.add(remoteNode.uniqueId);
                uniqueIdKeyMapping.put(remoteNode.uniqueId, remoteNode.getServerKey());
            }
        }

        TddlNode.setNodeIdKeyMapping(uniqueIdKeyMapping);

        String newUniqueIdList = "";
        if (!uniqueIdList.isEmpty()) {
            Collections.sort(uniqueIdList);
            StringBuilder sb = new StringBuilder();
            for (Integer uniqueId : uniqueIdList) {
                sb.append(SEPARATOR_SEMICOLON).append(uniqueId);
            }
            newUniqueIdList = sb.deleteCharAt(0).toString();
        }

        if (!TStringUtil.equalsIgnoreCase(newUniqueIdList, TddlNode.getNodeIdList())) {
            TddlNode.setNodeIdList(newUniqueIdList);
        }
    }

    private void refreshLocalGmsNode(int localServerPort) {
        if (localNode != null) {
            TddlNode.setInstId(InstIdUtil.getInstId());
            TddlNode.setHost(localNode.host);
            TddlNode.setPort(localNode.serverPort);
            TddlNode.setNodeId(localNode.uniqueId);
            TddlNode.setUniqueNodeId(localNode.uniqueId);
            TddlNode.setMaxNodeIndex(remoteNodes != null ? remoteNodes.size() + 1 : 1);
        } else {
            // FIXME: use for local test
            TddlNode.setPort(localServerPort);
        }
    }

    public static class GmsNode {

        public long origId;
        public int uniqueId;
        public String host;
        public int serverPort;
        public int managerPort;
        public int rpcPort;
        public int status;
        public String instId;
        public int instType;
        public int cpuCore;

        public String getHost() {
            return host;
        }

        public String getHostPort() {
            return AddressUtils.getAddrStrByIpPort(host, serverPort);
        }

        public String getServerKey() {
            return host + SEPARATOR_COLON + serverPort;
        }

        public String getManagerKey() {
            return host + SEPARATOR_COLON + managerPort;
        }

        public GmsSyncDataSource getManagerDataSource() {
            String managerKey = host + SEPARATOR_COLON + managerPort;
            GmsSyncDataSource syncDataSource = dataSources.get(managerKey);
            if (syncDataSource == null) {
                synchronized (this) {
                    syncDataSource = dataSources.get(managerKey);
                    if (syncDataSource == null) {
                        syncDataSource = buildDataSource(instId, host, managerPort);
                        dataSources.put(managerKey, syncDataSource);
                    }
                }
            }
            return syncDataSource;
        }

        public int getCpuCore() {
            return cpuCore;
        }

        @Override
        public String toString() {
            StringBuilder nodeInfo = new StringBuilder();
            print(nodeInfo, "id", origId);
            print(nodeInfo, "uniqueId", uniqueId);
            print(nodeInfo, "host", host);
            print(nodeInfo, "serverPort", serverPort);
            print(nodeInfo, "managerPort", managerPort);
            print(nodeInfo, "rpcPort", rpcPort);
            print(nodeInfo, "status", status);
            print(nodeInfo, "instId", instId);
            print(nodeInfo, "instType", instType);
            print(nodeInfo, "cpuCore", cpuCore);
            return nodeInfo.toString();
        }

        private void print(StringBuilder nodeInfo, String label, Object content) {
            nodeInfo.append(label).append(": ").append(content).append(SEPARATOR_COMMA);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GmsNode gmsNode = (GmsNode) o;
            return Objects.equals(instId, gmsNode.instId) && uniqueId == gmsNode.uniqueId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(instId, uniqueId);
        }
    }

    private static GmsSyncDataSource buildDataSource(String instId, String host, int port) {
        GmsSyncDataSource dataSource = new GmsSyncDataSource(instId, host, String.valueOf(port));
        dataSource.init();
        return dataSource;
    }

    private void printGmsNode() {
        StringBuilder nodeInfo = new StringBuilder();

        nodeInfo.append("Node List for ").append(InstIdUtil.getInstId()).append(SEPARATOR_LINE);

        if (localNode != null) {
            nodeInfo.append("Local Node:").append(SEPARATOR_LINE);
            nodeInfo.append(localNode.toString()).append(SEPARATOR_LINE);
        }

        if (remoteNodes != null) {
            nodeInfo.append("Remote Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : remoteNodes) {
                if (node != null) {
                    nodeInfo.append(node.toString()).append(SEPARATOR_LINE);
                }
            }
        }

        if (standbyNodes != null && standbyNodes.size() > 0) {
            nodeInfo.append("Standby Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : standbyNodes) {
                if (node != null) {
                    nodeInfo.append(node.toString()).append(SEPARATOR_LINE);
                }
            }
        }

        if (masterNodes != null && masterNodes.size() > 0) {
            nodeInfo.append("Master Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : masterNodes) {
                if (node != null) {
                    nodeInfo.append(node.toString()).append(SEPARATOR_LINE);
                }
            }
        }

        if (readOnlyNodesByInstId != null && readOnlyNodesByInstId.size() > 0) {
            for (String instId : readOnlyNodesByInstId.keySet()) {
                nodeInfo.append("Read-Only Nodes in ").append(instId).append(": ").append(SEPARATOR_LINE);
                for (GmsNode node : readOnlyNodesByInstId.get(instId)) {
                    if (node != null) {
                        nodeInfo.append(node.toString()).append(SEPARATOR_LINE);
                    }
                }
            }
        }

        LOGGER.info(nodeInfo.toString());
    }

    public int getReadOnlyNodeCpuCore() {
        return readOnlyNodeCpuCore;
    }
}
