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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

public abstract class NodeStatusManager {

    protected static final Logger logger = LoggerFactory.getLogger(NodeStatusManager.class);

    public final static int STATUS_INACTIVE = 0;
    public final static int STATUS_ACTIVE = 1;
    public final static int STATUS_SHUTDOWN = 2;
    //临时非激活状态，1分钟内不允许激活
    public final static int STATUS_TEMP_INACTIVE = 3;
    public final static long ACTIVE_LEASE = 30L;
    public final static long KEEPALIVE_INTERVAR = 15L;

    //是否是worker
    public final static int ROLE_WORKER = 1;
    //是否是CoorDinator
    public final static int ROLE_COORDINATOR = 2;
    //是否是主实例节点
    public final static int ROLE_MASTER = 4;
    //是否Leader
    public final static int ROLE_LEADER = 8;
    //加到黑名单中的节点不提供mpp服务
    public final static int ROLE_BLACKLIST = 16;
    //ROLE_SLAVE: 1 - 只读实例（HTAP使用），主实例     0 - 只读实例节点未加入HTAP链路
    public final static int ROLE_HTAP = 32;

    protected InternalNode localNode;
    protected String selectSql;
    protected String deleteLeaderSql;
    protected String checkLeaderSql;
    protected String deleteOldNodeSql;

    protected InternalNodeManager nodeManager;

    protected ScheduledFuture injectFuture;
    protected ScheduledFuture checkFuture;

    protected String tableName;

    public NodeStatusManager(InternalNodeManager nodeManager, String tableName, InternalNode localNode) {
        this.nodeManager = nodeManager;
        this.localNode = localNode;
        this.tableName = tableName;
        this.selectSql =
            "select `NODEID`,`VERSION`,`CLUSTER`,`INST_ID`,`IP`,`PORT`,`RPC_PORT`,`ROLE`,`STATUS`,TIMESTAMPDIFF(SECOND, "
                + "`GMT_MODIFIED`,CURRENT_TIMESTAMP) as `TIMEALIVE` from "
                + tableName + " where cluster='" + localNode.getCluster() + "'";

        this.deleteLeaderSql = "delete from " + tableName + " where cluster='" + localNode
            .getCluster()
            + "' and `ROLE`&" + ROLE_LEADER + "=" + ROLE_LEADER;

        this.checkLeaderSql = "select `NODEID` from " + tableName + " where cluster='" + localNode
            .getCluster() + "' and `ROLE`&" + ROLE_LEADER + "=" + ROLE_LEADER
            + " and TIMESTAMPDIFF(SECOND, `GMT_MODIFIED`,CURRENT_TIMESTAMP) < " + ACTIVE_LEASE;

        this.deleteOldNodeSql = "delete from " + tableName +
            " where TIMESTAMPDIFF(SECOND, `GMT_MODIFIED`, CURRENT_TIMESTAMP) > 86400";
    }

    public synchronized void init() {
        doInit();
    }

    protected abstract void doInit();

    protected abstract Connection getConnection() throws SQLException;

    public synchronized void destroy(boolean stop) {
        doDestroy(stop);
    }

    protected abstract void doDestroy(boolean stop);

    protected abstract void updateActiveNodes(
        String instId, InternalNode node, Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes, int role);

    protected abstract String updateTableMetaSql(int status);

    protected abstract void checkLeader(Connection conn, String leaderId);

    public abstract boolean forceToBeLeader();

    public boolean isInactiveNode(InternalNode node) {
        try (Connection conn = getConnection(); Statement statement = conn.createStatement()) {
            String sql = String.format(
                "select `STATUS` from %s where `STATUS`=%d and `CLUSTER`='%s' and `NODEID`='%s'",
                tableName, STATUS_TEMP_INACTIVE, node.getCluster(), node.getNodeIdentifier());

            ResultSet rs = statement.executeQuery(sql);
            return rs.next();
        } catch (Throwable e) {
            logger.error("isInactiveNode error:" + node, e);
        }
        return false;
    }

    public void tempInactiveNode(InternalNode node) {
        if (logger.isDebugEnabled()) {
            logger.debug("tempInactiveNode:" + node);
        }
        try (Connection conn = getConnection()) {
            String sql = String.format(
                "update %s set `STATUS`=%d,`GMT_MODIFIED`=CURRENT_TIMESTAMP where `CLUSTER`='%s' and `NODEID`='%s'",
                GmsSystemTables.NODE_INFO, STATUS_TEMP_INACTIVE, node.getCluster(), node.getNodeIdentifier());
            doExecuteUpdate(sql, conn);
        } catch (Throwable e) {
            logger.error("tempInactiveNode error:" + node, e);
            throw new RuntimeException("tempInactiveNode error:" + e.getMessage());
        }
    }

    private String injectNode(ResultSet rs, Set<InternalNode> activeNodes, Set<InternalNode> otherActiveNodes,
                              Set<InternalNode> inactiveNodes,
                              Set<InternalNode> shuttingDownNodes,
                              Set<String> htapInstIds)
        throws SQLException {
        String leaderId = null;
        String nodeId = rs.getString("NODEID");
        String version = rs.getString("VERSION");
        String cluster = rs.getString("CLUSTER");
        String instId = rs.getString("INST_ID");
        String host = rs.getString("IP");
        int port = rs.getInt("PORT");
        int rpcPort = rs.getInt("RPC_PORT");
        int role = rs.getInt("ROLE");
        int status = rs.getInt("STATUS");
        long timeAlive = rs.getLong("TIMEALIVE");

        InternalNode node = new InternalNode(nodeId, cluster, instId, host, port, rpcPort, new NodeVersion(version),
            (role & ROLE_COORDINATOR) == ROLE_COORDINATOR,
            (role & ROLE_WORKER) == ROLE_WORKER,
            (role & ROLE_BLACKLIST) == ROLE_BLACKLIST,
            (role & ROLE_HTAP) == ROLE_HTAP);

        if ((role & ROLE_MASTER) == ROLE_MASTER) {
            node.setMaster(true);
        }

        if ((role & ROLE_HTAP) == ROLE_HTAP) {
            htapInstIds.add(instId);
        }

        if (status == STATUS_SHUTDOWN) {
            shuttingDownNodes.add(node);
        } else if (timeAlive > ACTIVE_LEASE || status == STATUS_INACTIVE) {
            inactiveNodes.add(node);
        } else if (status == STATUS_TEMP_INACTIVE) {
            if ((role & ROLE_LEADER) == ROLE_LEADER) {
                node.setLeader(true);
                leaderId = node.getNodeIdentifier();
            }
            inactiveNodes.add(node);
        } else {
            if ((role & ROLE_LEADER) == ROLE_LEADER) {
                node.setLeader(true);
                leaderId = node.getNodeIdentifier();
            }

            if (!node.isInBlacklist()) {
                updateActiveNodes(instId, node, activeNodes, otherActiveNodes, role);
            }
        }
        return leaderId;
    }

    protected int doExecuteUpdate(String sql, Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            return statement.executeUpdate(sql);
        }
    }

    public void refreshNode() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        try (Connection conn = getConnection()) {
            refreshAllNodeInternal(conn);
        } catch (Throwable e) {
            logger.warn("refresh all node error", e);
            //如果出现任何异常的话，就需要立即重置leader， 避免出现双leader问题
            if (localNode != null && localNode.isLeader()) {
                localNode.setLeader(false);
            }
        }
    }

    protected void refreshAllNodeInternal(Connection conn) throws Throwable {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        try (Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql)) {
            Set<InternalNode> activeNodes = new HashSet<>();
            Set<InternalNode> otherActiveNodes = new HashSet<>();
            Set<InternalNode> inactiveNodes = new HashSet<>();
            Set<InternalNode> shuttingDownNodes = new HashSet<>();
            Set<String> htapInstIds = new HashSet<>();
            String leaderId = null;
            while (resultSet.next()) {
                String ret = injectNode(
                    resultSet, activeNodes, otherActiveNodes, inactiveNodes, shuttingDownNodes, htapInstIds);
                if (!StringUtils.isEmpty(ret)) {
                    leaderId = ret;
                }
            }
            checkLeader(conn, leaderId);

            if (localNode.isWorker()) {
                //本节点是worker节点的话，不写0库也有效
                activeNodes.add(localNode);
            }
            //主实例可以选择所有只读实例的worker节点， 只读实例只能选择本只读实例节点
            //otherActiveNodes 存储只读实例的活跃节点
            nodeManager.updateNodes(activeNodes, otherActiveNodes, inactiveNodes, shuttingDownNodes);

        } catch (Throwable e) {
            throw e;
        }
    }

    /**
     * @param role 附加角色，比如给node加上leader角色
     */
    protected String replaceTableMetaSql(Node node, int role) {
        return "replace into " + tableName
            + " (`CLUSTER`, `INST_ID`, `NODEID`, `VERSION`, `IP`, `PORT`, `RPC_PORT`, `ROLE`, `STATUS`) "
            + "values ('" + node.getCluster() + "', '" + node.getInstId() + "', '" + node.getNodeIdentifier() + "', '"
            + node.getVersion() + "', '" + node.getHost()
            + "', " + node.getPort() + ", " + node.getRpcPort() + ", " + (getRole(node) | role) + ", " + STATUS_ACTIVE
            + ")";
    }

    protected String insertOrUpdateTableMetaSql(Node node, int role) {
        return "insert into " + tableName
            + " (`CLUSTER`, `INST_ID`, `NODEID`, `VERSION`, `IP`, `PORT`, `RPC_PORT`, `ROLE`, `STATUS`) "
            + "values ('" + node.getCluster() + "', '" + node.getInstId() + "', '" + node.getNodeIdentifier() + "', '"
            + node.getVersion() + "', '" + node.getHost()
            + "', " + node.getPort() + ", " + node.getRpcPort() + ", " + (getRole(node) | role) + ", " + STATUS_ACTIVE
            + ")" + " ON DUPLICATE KEY UPDATE " +
            " `VERSION` = '" + node.getVersion() + "', " +
            " `INST_ID` = '" + node.getInstId() + "', " +
            " `IP` = '" + node.getHost() + "', " +
            " `PORT` = '" + node.getPort() + "', " +
            " `RPC_PORT` = '" + node.getRpcPort() + "', " +
            " `ROLE` = '" + (getRole(node) | role) + "', " +
            " `STATUS` = '" + STATUS_ACTIVE + "'";
    }

    protected int getRole(Node node) {
        int role = 0;
        if (node.isWorker()) {
            role = role | ROLE_WORKER;
        }
        if (node.isCoordinator()) {
            role = role | ROLE_COORDINATOR;
        }
        if (node.isLeader()) {
            role = role | ROLE_LEADER;
        }
        if (node.isInBlacklist()) {
            role = role | ROLE_BLACKLIST;
        }
        if (node.isHtap()) {
            role = role | ROLE_HTAP;
        }

        if (ConfigDataMode.isMasterMode()) {
            role = role | ROLE_MASTER;
        }

        return role;
    }

    public Node getLocalNode() {
        return this.localNode;
    }

    protected void updateLocalNode(int status) {
        if (logger.isDebugEnabled()) {
            logger.debug("updateLocalNode:" + localNode.getNodeIdentifier() + ",status=" + status);
        }
        try (Connection conn = getConnection()) {
            int rowcount = doExecuteUpdate(updateTableMetaSql(status), conn);
            if (rowcount == 0 && !isInactiveNode(localNode)) {
                //节点状态没有更新成功，记录被清除， 如果本节点是leader，需要清除leader状态
                if (localNode.isLeader()) {
                    localNode.setLeader(false);
                }
                //重新注册该节点
                synchronized (this) {
                    if (!localNode.isLeader()) {
                        //避免和tryMarkLeader地方有并发安全问题
                        doExecuteUpdate(insertOrUpdateTableMetaSql(localNode, 0), conn);
                    }
                }
            }
        } catch (Throwable e) {
            logger.error("updateLocalNode error", e);
        }
    }
}
