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

    /**
     * Current node.
     */
    private volatile GmsNode localNode = null;

    /**
     * Remote nodes only in current instance.
     */
    private List<GmsNode> remoteNodes = new ArrayList<>();
    /**
     * All master nodes
     */
    private List<GmsNode> masterNodes = new ArrayList<>();

    /**
     * All standby nodes
     */
    private List<GmsNode> standbyNodes = new ArrayList<>();

    /**
     * All read-ony nodes.
     */
    private List<GmsNode> readOnlyNodes = new ArrayList<>();

    /**
     * All row read-ony nodes.
     */
    private List<GmsNode> rowReadOnlyNodes = new ArrayList<>();

    /**
     * All row read-ony nodes and master nodes.
     */
    private List<GmsNode> noColumnarReadNodes = new ArrayList<>();

    /**
     * All columnar row read-ony nodes.
     */
    private List<GmsNode> columnarReadOnlyNodes = new ArrayList<>();

    /**
     * All the nodes including master and read-only, it is sorted by node index.
     */
    private List<GmsNode> allNodes = new ArrayList<>();

    private int currentIndex = -1;
    private int readOnlyNodeCpuCore = -1;
    private int readOnlyColumnarCpuCore = -1;

    private GmsNodeManager() {
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

    public List<GmsNode> getReadOnlyNodes() {
        return readOnlyNodes;
    }

    public List<GmsNode> getRowReadOnlyNodes() {
        return rowReadOnlyNodes;
    }

    public List<GmsNode> getColumnarReadOnlyNodes() {
        return columnarReadOnlyNodes;
    }

    public List<GmsNode> getNoColumnarReadNodes() {
        return noColumnarReadNodes;
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
        case ROW_SLAVE_ONLY:
            return getRowReadOnlyNodes();
        case COLUMNAR_SLAVE_ONLY:
            return getColumnarReadOnlyNodes();
        case NOT_COLUMNAR_SLAVE:
            return getNoColumnarReadNodes();
        case CURRENT_ONLY:
        default:
            return remoteNodes;
        }
    }

    public void reloadNodes(int localServerPort) {
        // Clean up sync data sources.
        clearDataSources();

        loadAllNodes();
        reloadDifferentNodes(localServerPort);

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

    private void reloadDifferentNodes(int localServerPort) {

        GmsNode localNode = null;
        final List<GmsNode> remoteNodes = new ArrayList<>();
        final List<GmsNode> masterNodes = new ArrayList<>();
        final List<GmsNode> standbyNodes = new ArrayList<>();
        final List<GmsNode> readOnlyNodes = new ArrayList<>();
        final List<GmsNode> rowReadOnlyNodes = new ArrayList<>();
        final List<GmsNode> columnarReadOnlyNodes = new ArrayList<>();
        final List<GmsNode> noColumnarReadNodes = new ArrayList<>();

        allNodes.sort((gmsNode1, gmsNode2) -> {
            if (gmsNode1.instId.equals(gmsNode2.instId)) {
                return Integer.compare(gmsNode1.uniqueId, gmsNode2.uniqueId);
            }
            return gmsNode1.instId.compareTo(gmsNode2.instId);
        });

        for (GmsNode gmsNode : allNodes) {
            String instId = gmsNode.instId;
            int instType = gmsNode.instType;
            if (instType == ServerInfoRecord.INST_TYPE_MASTER) {
                masterNodes.add(gmsNode);
                noColumnarReadNodes.add(gmsNode);
            } else if (instType == ServerInfoRecord.INST_TYPE_STANDBY) {
                masterNodes.add(gmsNode);
                standbyNodes.add(gmsNode);
                noColumnarReadNodes.add(gmsNode);
            } else if (instType == ServerInfoRecord.INST_TYPE_ROW_SLAVE) {
                readOnlyNodes.add(gmsNode);
                rowReadOnlyNodes.add(gmsNode);
                noColumnarReadNodes.add(gmsNode);
            } else if (instType == ServerInfoRecord.INST_TYPE_HTAP_SLAVE) {
                readOnlyNodes.add(gmsNode);
                rowReadOnlyNodes.add(gmsNode);
                noColumnarReadNodes.add(gmsNode);
                this.readOnlyNodeCpuCore = gmsNode.cpuCore;
            } else if (instType == ServerInfoRecord.INST_TYPE_COLUMNAR_SLAVE) {
                readOnlyNodes.add(gmsNode);
                columnarReadOnlyNodes.add(gmsNode);
                this.readOnlyColumnarCpuCore = gmsNode.cpuCore;
            }
            if (instId.equalsIgnoreCase(InstIdUtil.getInstId())) {
                if (TStringUtil.equalsIgnoreCase(gmsNode.host, AddressUtils.getHostIp())
                    && gmsNode.serverPort == localServerPort) {
                    localNode = gmsNode;
                } else {
                    remoteNodes.add(gmsNode);
                }
            }
        }

        this.localNode = localNode;

        if (allNodes.isEmpty()) {
            // Need one node at least for local test even if it's null.
            allNodes.add(localNode);
        }

        this.remoteNodes = remoteNodes;
        this.masterNodes = masterNodes;
        this.standbyNodes = standbyNodes;
        this.readOnlyNodes = readOnlyNodes;
        this.rowReadOnlyNodes = rowReadOnlyNodes;
        this.columnarReadOnlyNodes = columnarReadOnlyNodes;
        this.noColumnarReadNodes = noColumnarReadNodes;
        this.currentIndex = allNodes.indexOf(this.localNode);
    }

    private void loadAllNodes() {
        List<GmsNode> allNodes = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {

            ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
            serverInfoAccessor.setConnection(metaDbConn);

            List<ServerInfoRecord> records = serverInfoAccessor.getAllServerInfo();

            // Reload new ones.
            Set<Integer> assignedUniqueIds = new HashSet<>();
            for (ServerInfoRecord record : records) {
                if (record.status == ServerInfoRecord.SERVER_STATUS_READY) {
                    int uniqueId = assignUniqueId(record, assignedUniqueIds);
                    allNodes.add(buildNode(record, uniqueId));
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
        this.allNodes = allNodes;
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
                ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
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
            }
        }

        while (assignedUniqueIds.contains(uniqueId)) {
            // Increase until no conflict.
            nextId = ++uniqueId;
            uniqueId = generateUniqueId(nextId);
        }

        if (renewId) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
                serverInfoAccessor.setConnection(metaDbConn);
                // Update new id for current record.
                long newId = serverInfoAccessor.updateCurrentId(record.id, nextId, record.ip, record.port);
                if (newId > 0) {
                    record.id = newId;
                    uniqueId = generateUniqueId(newId);
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
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
                    nodeInfo.append(node).append(SEPARATOR_LINE);
                }
            }
        }

        if (masterNodes != null && masterNodes.size() > 0) {
            nodeInfo.append("Master Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : masterNodes) {
                if (node != null) {
                    nodeInfo.append(node).append(SEPARATOR_LINE);
                }
            }
        }

        if (standbyNodes != null && standbyNodes.size() > 0) {
            nodeInfo.append("Standby Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : standbyNodes) {
                if (node != null) {
                    nodeInfo.append(node).append(SEPARATOR_LINE);
                }
            }
        }

        if (readOnlyNodes != null && readOnlyNodes.size() > 0) {
            nodeInfo.append("readOnly Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : readOnlyNodes) {
                if (node != null) {
                    nodeInfo.append(node).append(SEPARATOR_LINE);
                }
            }
        }

        if (columnarReadOnlyNodes != null && columnarReadOnlyNodes.size() > 0) {
            nodeInfo.append("columnar readOnly Nodes:").append(SEPARATOR_LINE);
            for (GmsNode node : columnarReadOnlyNodes) {
                if (node != null) {
                    nodeInfo.append(node).append(SEPARATOR_LINE);
                }
            }
        }

        LOGGER.info(nodeInfo.toString());
    }

    public int getReadOnlyNodeCpuCore() {
        return readOnlyNodeCpuCore;
    }

    public int getReadOnlyColumnarCpuCore() {
        return readOnlyColumnarCpuCore;
    }

}
