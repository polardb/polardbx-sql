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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.privilege.quarantine.QuarantineConfigAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chenghui.lch
 */
public class ReadOnlyInstConfigCleaner extends AbstractLifecycle {

    private final static Logger logger = LoggerFactory.getLogger(DbTopologyManager.class);

    private ScheduledExecutorService roInstConfigCleanTaskExecutor;
    private long checkRemovedRoInstPeriod = 30000;
    private ReadOnlyInstConfigCleanTask readOnlyInstConfigCleanTask = new ReadOnlyInstConfigCleanTask();

    protected static ReadOnlyInstConfigCleaner instance = new ReadOnlyInstConfigCleaner();

    protected static class ReadOnlyInstConfigCleanTask implements Runnable {
        @Override
        public void run() {
            if (!LeaderStatusBridge.getInstance().hasLeadership()) {
                return;
            }

            try {

                try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

                    List<String> removedInstIdList = new ArrayList<>();
                    //find the removed columnar read only instId
                    ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
                    serverInfoAccessor.setConnection(metaDbConn);
                    removedInstIdList.addAll(serverInfoAccessor.getAllRemovedColumnarReadOnlyInstIdList());

                    //find the removed row read only instId
                    StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                    storageInfoAccessor.setConnection(metaDbConn);
                    removedInstIdList.addAll(storageInfoAccessor.getAllRemovedReadOnlyInstIdList());
                    if (removedInstIdList.isEmpty()) {
                        return;
                    }

                    metaDbConn.setAutoCommit(false);
                    for (int i = 0; i < removedInstIdList.size(); i++) {
                        String roInstId = removedInstIdList.get(i);
                        ReadOnlyInstConfigCleaner.cleanRemovedReadOnlyInstConfigs(roInstId, metaDbConn);
                    }
                    metaDbConn.commit();

                    metaDbConn.setAutoCommit(true);
                } catch (Throwable ex) {
                    MetaDbLogUtil.META_DB_LOG.error(ex);
                    logger.error(ex);
                }

            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }
    }

    protected ReadOnlyInstConfigCleaner() {
    }

    @Override
    protected void doInit() {

        // Only Master inst allow to do the clean task of removed read-only inst task
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }

        roInstConfigCleanTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("RoInstConfigCleanTaskExecutor", true));

        roInstConfigCleanTaskExecutor.scheduleAtFixedRate(
            readOnlyInstConfigCleanTask,
            checkRemovedRoInstPeriod,
            checkRemovedRoInstPeriod,
            TimeUnit.MILLISECONDS);
    }

    public static ReadOnlyInstConfigCleaner getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    private static void cleanRemovedReadOnlyInstConfigs(String readOnlyInstId, Connection metaDbConn)
        throws SQLException {
        // Check if the readOnlyInstId is a read-only inst_id
        if (ServerInstIdManager.getInstance().isMasterInstId(readOnlyInstId)) {
            // Cannot clear configs about master inst
            return;
        }

        // Check if the status of the read-only inst_id
        ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
        serverInfoAccessor.setConnection(metaDbConn);
        List<ServerInfoRecord> serverInfos = serverInfoAccessor.getRemovedServerInfoByInstId(readOnlyInstId);
        if (serverInfos.isEmpty()) {
            return;
        }

        // clear the group_detail_info for the removed read-only inst
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);
        groupDetailInfoAccessor.deleteGroupDetailInfoByInstId(readOnlyInstId);

        // clear the inst_config for the removed read-only inst
        InstConfigAccessor instConfigAccessor = new InstConfigAccessor();
        instConfigAccessor.setConnection(metaDbConn);
        instConfigAccessor.deleteInstConfigsByInstId(readOnlyInstId);

        // clear the dataIds for the removed read-only inst
        ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
        configListenerAccessor.setConnection(metaDbConn);
        configListenerAccessor.deleteAllDataIdsByInstId(readOnlyInstId);

        // clear the node_info for the removed read-only inst
        deleteNodeInfoByInstId(readOnlyInstId, metaDbConn);

        // inst_lock is used for lock inst when the inst is overdue
        // clear the inst_lock info for the the removed read-only inst
        InstLockAccessor instLockAccessor = new InstLockAccessor();
        instLockAccessor.setConnection(metaDbConn);
        instLockAccessor.deleteInstLockByInstId(readOnlyInstId);

        // quarantine_config is the ip white list of read-only inst
        // clear the quarantine_config for the removed read-only inst
        QuarantineConfigAccessor quarantineConfigAccessor = new QuarantineConfigAccessor();
        quarantineConfigAccessor.setConnection(metaDbConn);
        quarantineConfigAccessor.deleteQuarntineConfigsByInstId(readOnlyInstId);

        // clear the storage_info for the removed read-only inst
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConn);
        storageInfoAccessor.clearRemovedReadOnlyStorageInfosByInstId(readOnlyInstId);

        // clear the server_info for the removed read-only inst
        serverInfoAccessor.clearRemovedReadOnlyServerInfosByInstId(readOnlyInstId);
    }

    protected static void deleteNodeInfoByInstId(String instId, Connection conn) throws SQLException {
        String deleteNodeInfoSql = String.format("delete from node_info where inst_id='%s'", instId);
        MetaDbUtil.delete(deleteNodeInfoSql, conn);
    }

}
