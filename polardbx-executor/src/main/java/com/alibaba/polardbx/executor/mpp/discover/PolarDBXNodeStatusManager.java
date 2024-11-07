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

package com.alibaba.polardbx.executor.mpp.discover;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.NODE_INFO;

public class PolarDBXNodeStatusManager extends NodeStatusManager {

    protected ScheduledFuture checkDelayFuture;

    public PolarDBXNodeStatusManager(InternalNodeManager nodeManager, InternalNode localNode) {
        super(nodeManager, NODE_INFO, localNode);
    }

    @Override
    protected void doInit() {
        try (Connection conn = getConnection()) {

            doExecuteUpdate(deleteOldNodeSql, conn);

            doExecuteUpdate(insertOrUpdateTableMetaSql(localNode, 0), conn);

            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("Mpp-Leader-Factory", true));
            injectFuture = scheduledExecutorService
                .scheduleWithFixedDelay(new Notify0dbTask(), 0L, KEEPALIVE_INTERVAR,
                    TimeUnit.SECONDS);

            checkFuture =
                scheduledExecutorService.scheduleWithFixedDelay(
                    new CheckAllNodeTask(), 0L, ACTIVE_LEASE, TimeUnit.SECONDS);
            logger.warn("injectNode " + localNode + " over");
        } catch (Throwable t) {
            logger.error("init PolarDBXNodeStatusManager error:", t);
            //如果初始化异常的话，就一定要抛出来
            throw new TddlNestableRuntimeException(t);
        }

        ExecUtils.syncNodeStatus(SystemDbHelper.DEFAULT_DB_NAME);
    }

    @Override
    protected void doDestroy(boolean stop) {
        if (injectFuture != null) {
            injectFuture.cancel(true);
        }
        if (checkFuture != null) {
            checkFuture.cancel(true);
        }
        if (checkDelayFuture != null) {
            checkDelayFuture.cancel(true);
        }
        updateLocalNode(STATUS_SHUTDOWN);
        //触发SYNC服务通知别的节点删除本节点
        ExecUtils.syncNodeStatus(SystemDbHelper.DEFAULT_DB_NAME);
        if (localNode.isLeader()) {
            localNode.setLeader(false);
        }
    }

    private class Notify0dbTask implements Runnable {
        @Override
        public void run() {
            GmsNodeManager.GmsNode gmsNode = GmsNodeManager.getInstance().getLocalNode();
            //FIXME 现在server_info节点中不存在的节点，也允许使用
            if (gmsNode == null || gmsNode.status == ServerInfoRecord.SERVER_STATUS_READY) {
                updateLocalNode(STATUS_ACTIVE);
            }
        }
    }

    private class CheckAllNodeTask implements Runnable {
        @Override
        public void run() {
            refreshNode();
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return MetaDbUtil.getConnection();
    }

    @Override
    protected String updateTableMetaSql(int status) {
        //STATUS_TEMP_INACTIVE状态一分钟内允许激活
        if (status == STATUS_ACTIVE) {
            GmsNodeManager.GmsNode gmsNode = GmsNodeManager.getInstance().getLocalNode();
            //polarx只读实例，需要处理节点类型的变化
            if (gmsNode != null && !ConfigDataMode.isMasterMode()) {
                boolean changeHtapRole = false;

                if (ConfigDataMode.isRowSlaveMode()) {
                    if (gmsNode.instType == ServerInfoRecord.INST_TYPE_HTAP_SLAVE && !localNode.isHtap()) {
                        localNode.setHtap(true);
                        changeHtapRole = true;
                    } else if (gmsNode.instType != ServerInfoRecord.INST_TYPE_HTAP_SLAVE && localNode.isHtap()) {
                        localNode.setHtap(false);
                        changeHtapRole = true;
                    }
                } else {
                    if (!localNode.isHtap() && DynamicConfig.getInstance().allowColumnarBindMaster()) {
                        localNode.setHtap(true);
                        changeHtapRole = true;
                    } else if (localNode.isHtap() && !DynamicConfig.getInstance().allowColumnarBindMaster()) {
                        localNode.setHtap(false);
                        changeHtapRole = true;
                    }
                }

                if (logger.isDebugEnabled()) {
                    logger.debug(
                        gmsNode.getServerKey() + ":gmsNode.instType=" + gmsNode.instType + ",localNode.isHtap()="
                            + localNode.isHtap() + ",changeHtapRole=" + changeHtapRole + ",getRole(localNode)="
                            + getRole(localNode));
                }

                if (changeHtapRole) {
                    return String.format(
                        "update %s set `IP`='%s',`STATUS`=%d,`GMT_MODIFIED`=CURRENT_TIMESTAMP,`ROLE`=%d "
                            + "where `CLUSTER`='%s' and `NODEID`='%s'",
                        tableName, localNode.getHost(), status, getRole(localNode), localNode.getCluster(),
                        localNode.getNodeIdentifier());
                }
            }

            return String.format(
                "update %s set `IP`='%s',`STATUS`=%d,`GMT_MODIFIED`=CURRENT_TIMESTAMP where `CLUSTER`='%s' and "
                    + "`NODEID`='%s' and (`STATUS`!=3 or TIMESTAMPDIFF(SECOND,`GMT_MODIFIED`,CURRENT_TIMESTAMP)>60)",
                tableName, localNode.getHost(), status, localNode.getCluster(), localNode.getNodeIdentifier());
        } else {
            return String.format(
                "update %s set `IP`='%s',`STATUS`=%d,`GMT_MODIFIED`=CURRENT_TIMESTAMP where `CLUSTER`='%s' and "
                    + "`NODEID`='%s'",
                tableName, localNode.getHost(), status, localNode.getCluster(), localNode.getNodeIdentifier());
        }
    }

    @Override
    public boolean forceToBeLeader() {
        boolean ret = false;
        if (ConfigDataMode.needInitMasterModeResource() && !localNode.isLeader()) {
            try (Connection conn = MetaDbUtil.getConnection()) {
                try {
                    if (tryGetLock(conn, 2 * ACTIVE_LEASE)) {
                        try (Statement statement = conn.createStatement()) {
                            conn.setAutoCommit(false);
                            //to be leader
                            markLeader(statement);
                            localNode.setLeader(true);
                            ret = true;
                        } catch (Throwable t) {
                            conn.rollback();
                        } finally {
                            conn.setAutoCommit(true);
                        }
                    }
                } catch (SQLException e) {
                    logger.warn("force to be leader error!", e);
                } finally {
                    releaseLock(conn);
                }
            } catch (SQLException e) {
                logger.warn("get gms connection!", e);
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
            }

            //notify all nodes
            ExecUtils.syncNodeStatus(SystemDbHelper.DEFAULT_DB_NAME);
        }
        if (localNode.isLeader()) {
            ret = true;
        }
        return ret;
    }

    @Override
    protected void checkLeader(Connection conn, String leaderId) {
        //只有PolarDB-X主实例才选主
        if (ConfigDataMode.isMasterMode()) {
            if (StringUtils.isEmpty(leaderId)) {
                //先清除自己leader角色,重新竞选
                resetLeaderStatus();
                try {
                    if (tryGetLock(conn, 0)) {
                        tryMarkLeader(conn);
                    }
                } catch (SQLException e) {
                    logger.warn("checkLeader error", e);
                } finally {
                    releaseLock(conn);
                }
            } else if (!localNode.getNodeIdentifier().equalsIgnoreCase(leaderId)) {
                //存在leader,但是leader不是当前的自己，那么马上清理自己的leader角色
                resetLeaderStatus();
            } else {
                //可能由于metaDb异常会清理leader，这里重置下leader
                localNode.setLeader(true);
            }
        }
    }

    private void resetLeaderStatus() {
        localNode.setLeader(false);
        List<Node> coordinators = nodeManager.getAllNodes().getAllCoordinators();
        if (coordinators != null && !coordinators.isEmpty()) {
            for (Node node : coordinators) {
                if (node.isLeader() && node.getNodeIdentifier().equalsIgnoreCase(localNode.getNodeIdentifier())) {
                    node.setLeader(false);
                }
            }
        }
    }

    private boolean tryGetLock(Connection conn, long timeout) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery(
                String.format("SELECT GET_LOCK('" + localNode.getCluster() + "', %d) ", timeout))) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Throwable e) {
            logger.warn("tryGetLock error", e);
            return false;
        }
    }

    private boolean releaseLock(Connection conn) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT RELEASE_LOCK('" + localNode.getCluster() + "') ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Exception e) {
            logger.warn("releaseLock error", e);
            return false;
        }
    }

    private void tryMarkLeader(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            conn.setAutoCommit(false);
            ResultSet rs = statement.executeQuery(checkLeaderSql);
            boolean beLeader = false;
            if (!rs.next()) {
                markLeader(statement);
            } else {
                String nodeId = rs.getString("NODEID");
                if (localNode.getNodeIdentifier().equalsIgnoreCase(nodeId)) {
                    beLeader = true;
                }
            }
            conn.commit();
            if (beLeader) {
                //竞选成功广而告之
                ExecUtils.syncNodeStatus(SystemDbHelper.DEFAULT_DB_NAME);
                localNode.setLeader(true);
            }
        } catch (Throwable t) {
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void markLeader(Statement statement) throws SQLException {
        logger.warn("setLeader:" + localNode);
        statement.executeUpdate(deleteLeaderSql);
        synchronized (this) {
            statement.executeUpdate(insertOrUpdateTableMetaSql(localNode, ROLE_LEADER));
        }
    }
}