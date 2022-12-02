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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.transaction.sync.RequestSnapshotSeqSyncAction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Task to send a timestamp to storage nodes in order to keep their latest timestamp up-to-date.
 * Only works on PolarDB-X cluster.
 */
public class PurgeTsoTimerTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PurgeTsoTimerTask.class);

    private static final String PURGE_SEQ_QUERY = "SET GLOBAL innodb_purge_seq = ?";

    private static final String PURGE_SEQ_QUERY_SQL = "SET GLOBAL innodb_purge_seq = ";

    private final AsyncTaskQueue asyncQueue;

    public PurgeTsoTimerTask(AsyncTaskQueue asyncQueue) {
        this.asyncQueue = asyncQueue;
    }

    @Override
    public void run() {
        boolean hasLeadership = ExecUtils.hasLeadership(null);
        if (!hasLeadership) {
            return;
        }

        IGmsSyncAction action = new RequestSnapshotSeqSyncAction();
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(
            action, SystemDbHelper.DEFAULT_DB_NAME);

        Long minSnapshotTime = null;

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                Long time = DataTypes.LongType.convertFrom(row.get("TSO"));
                if (time != null) {
                    if (minSnapshotTime == null) {
                        minSnapshotTime = time;
                    } else {
                        minSnapshotTime = Math.min(minSnapshotTime, time);
                    }
                }
            }
        }

        logger.warn("get the global innodb_purge_seq: " + minSnapshotTime);
        if (minSnapshotTime != null) {
            List<Future> futures = new ArrayList<>();
            if (DynamicConfig.getInstance().isKeepTsoBasedCDC()
                && ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME) != null) {
                final Long timestamp = minSnapshotTime;
                TopologyHandler topologyHandler =
                    ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME).getTopologyHandler();
                for (Group group : topologyHandler.getMatrix().getGroups()) {
                    if (!DbGroupInfoManager.isVisibleGroup(group)) {
                        continue;
                    }
                    String groupName = group.getName();
                    IGroupExecutor groupExecutor = topologyHandler.get(groupName);
                    DataSource dataSource = groupExecutor.getDataSource();
                    // Send heartbeat to each group simultaneously
                    futures.add(asyncQueue.submit(() -> {
                        registerPurgeTso(dataSource, timestamp);
                    }));
                }

                // Also send heartbeat to MetaDB
                DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
                futures.add(asyncQueue.submit(() -> {
                    registerPurgeTso(dataSource, timestamp);
                }));
            } else {
                final Long timestamp = minSnapshotTime;
                Map<String, StorageInstHaContext> storageStatusMap =
                    StorageHaManager.getInstance().getStorageHaCtxCache();
                Iterator<StorageInstHaContext> iterator = storageStatusMap.values().stream().iterator();
                while (iterator.hasNext()) {
                    StorageInstHaContext instHaContext = iterator.next();
                    if (instHaContext != null && instHaContext.isMasterMode()) {
                        doPurge(instHaContext, minSnapshotTime);
                    }
                }
                // Also send heartbeat to MetaDB
                DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
                futures.add(asyncQueue.submit(() -> {
                    registerPurgeTso(dataSource, timestamp);
                }));
            }
            AsyncUtils.waitAll(futures);
        }
    }

    private void registerPurgeTso(DataSource dataSource, Long timestamp) {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(PURGE_SEQ_QUERY)) {
            ps.setLong(1, timestamp);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to send timestamp purge", e);
        }
    }

    private void doPurge(StorageInstHaContext instHaContext, long timestamp) {
        try (Connection salveConn = DbTopologyManager.getConnectionForStorage(instHaContext)) {
            Statement stmt = null;
            try {
                stmt = salveConn.createStatement();
                stmt.executeUpdate(PURGE_SEQ_QUERY_SQL + timestamp);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (Throwable e) {
            logger.error("Failed to send timestamp purge for " + instHaContext.getStorageInstId(), e);
        }
    }
}
