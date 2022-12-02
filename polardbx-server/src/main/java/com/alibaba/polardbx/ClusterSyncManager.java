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

package com.alibaba.polardbx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorTemplate;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * server集群多机通知
 *
 * @author agapple 2015年3月26日 下午6:53:29
 * @since 5.1.19
 */
@Activate(order = 2)
public class ClusterSyncManager extends AbstractLifecycle implements ISyncManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterSyncManager.class);

    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, boolean throwExceptions) {
        return doSync(action, schemaName, SyncScope.DEFAULT_SYNC_SCOPE, null, throwExceptions);
    }

    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                                boolean throwExceptions) {
        return doSync(action, schemaName, scope, null, throwExceptions);
    }

    @Override
    public void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler, boolean throwExceptions) {
        doSync(action, schemaName, SyncScope.DEFAULT_SYNC_SCOPE, handler, throwExceptions);
    }

    @Override
    public void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
                     boolean throwExceptions) {
        doSync(action, schemaName, scope, handler, throwExceptions);
    }

    private List<List<Map<String, Object>>> doSync(IGmsSyncAction action, String schemaName,
                                                   SyncScope scope, ISyncResultHandler handler,
                                                   boolean throwExceptions) {
        final List<List<Map<String, Object>>> results = Collections.synchronizedList(new ArrayList(1));
        final List<Pair<NodeInfo, List<Map<String, Object>>>> resultsForHandler =
            Collections.synchronizedList(new ArrayList(1));

        // Perform the sync action locally first.
        final NodeInfo localNode = GmsNodeManager.getInstance().getLocalNode();
        List<Map<String, Object>> localResult = null;

        if (scope == null) {
            scope = SyncScope.DEFAULT_SYNC_SCOPE;
        }

        switch (scope) {
        case MASTER_ONLY:
            if (GmsNodeManager.getInstance().isCurrentNodeMaster()) {
                localResult = ExecUtils.resultSetToList((ResultCursor) action.sync());
            }
            break;
        case SLAVE_ONLY:
            if (GmsNodeManager.getInstance().isCurrentNodeReadOnly()) {
                localResult = ExecUtils.resultSetToList((ResultCursor) action.sync());
            }
            break;
        case ALL:
        case CURRENT_ONLY:
        default:
            localResult = ExecUtils.resultSetToList((ResultCursor) action.sync());
            break;
        }

        List<NodeInfo> originSyncNodes = GmsNodeManager.getInstance().getNodesBySyncScope(scope);

        List<NodeInfo> syncNodes = new ArrayList<>();
        synchronized (originSyncNodes) {
            syncNodes.addAll(originSyncNodes);
        }

        if (GeneralUtil.isNotEmpty(syncNodes)) {
            sync(resultsForHandler, localNode, syncNodes, action, schemaName, throwExceptions);
            for (Pair<NodeInfo, List<Map<String, Object>>> result : resultsForHandler) {
                results.add(result.getValue());
            }
        }

        if (localResult != null) {
            results.add(localResult);
            resultsForHandler.add(new Pair<>(localNode, localResult));
        }

        if (handler != null) {
            handler.handle(resultsForHandler);
        }

        return results;
    }

    private void sync(List<Pair<NodeInfo, List<Map<String, Object>>>> resultsForHandler, NodeInfo localNode,
                      List<NodeInfo> remoteNodes, IGmsSyncAction action, String schemaName, boolean throwExceptions) {
        // Use thread pool for manager port to avoid conflict with server port.
        ExecutorTemplate template = new ExecutorTemplate(CobarServer.getInstance().getManagerExecutor());

        Map<String, String> nodeExceptions = new HashMap<>();

        for (final NodeInfo remoteNode : remoteNodes) {
            if (remoteNode == null || remoteNode.equals(localNode)) {
                // The node info is null (for defence) or already do sync action for local node.
                continue;
            }

            final String sql = buildRequestSql(action, schemaName);

            template.submit(() -> {
                boolean checked = false;
                try (Connection conn = remoteNode.getManagerDataSource().getConnection()) {

                    // 先验证链接可用性
                    conn.createStatement().execute("show @@config");
                    checked = true;

                    Statement stmt = conn.createStatement();
                    stmt.execute(sql);

                    resultsForHandler.add(new Pair<>(remoteNode, ExecUtils.resultSetToList(stmt.getResultSet())));
                } catch (Throwable e) {
                    nodeExceptions.put(remoteNode.getManagerKey(), e.getMessage());
                    logger.error(String.format("Failed to SYNC to '%s' %s check for %s. Caused by: %s",
                        remoteNode.getManagerKey(), checked ? "after" : "before", action.getClass().getSimpleName(),
                        e.getMessage()), e);
                }
            });
        }

        // 同步等待所有结果
        template.waitForResult();

        if (throwExceptions && GeneralUtil.isNotEmpty(nodeExceptions)) {
            StringBuilder buf = new StringBuilder();
            buf.append("Failed to SYNC to the following nodes:").append("\n");
            nodeExceptions.forEach((key, value) -> buf.append(key).append(" - ").append(value).append(";\n"));
            throw GeneralUtil.nestedException(buf.toString());
        }
    }

    @Override
    public List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {

        NodeInfo localNode =
            GmsNodeManager.getInstance().getLocalNode();

        List<NodeInfo> remoteNodes =
            GmsNodeManager.getInstance().getRemoteNodes();

        if (GeneralUtil.isEmpty(remoteNodes) || localNode == null ||
            TStringUtil.equals(localNode.getServerKey(), serverKey)) {
            // If there are no other nodes at all or the sync target is local
            // server, then do the sync action locally only.
            return ExecUtils.resultSetToList((ResultCursor) action.sync());
        }

        final String sql = buildRequestSql(action, schemaName);

        DataSource dataSource = getDataSource(serverKey, remoteNodes);

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return ExecUtils.resultSetToList(stmt.getResultSet());
        } catch (SQLException e) {
            String errMsg = "Failed to SYNC to '" + serverKey + "'. Caused by: " + e.getMessage();
            logger.error(errMsg, e);
            throw GeneralUtil.nestedException(errMsg, e);
        }
    }

    private String buildRequestSql(IGmsSyncAction action, String schema) {
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        return "SYNC " + schema + " " + data;
    }

    private DataSource getDataSource(String serverKey, List<NodeInfo> remoteNodes) {
        for (NodeInfo remoteNode : remoteNodes) {
            if (TStringUtil.equals(remoteNode.getServerKey(), serverKey)) {
                return remoteNode.getManagerDataSource();
            }
        }
        throw GeneralUtil.nestedException("Not found the sync target server '" + serverKey + "' from node list");
    }
}
