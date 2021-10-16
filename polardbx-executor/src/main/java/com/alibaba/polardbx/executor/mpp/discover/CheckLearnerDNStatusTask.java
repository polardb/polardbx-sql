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

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.PolarDBXStatus;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.sync.SyncScope.MASTER_ONLY;

/**
 * check the dn status every 2 seconds
 */
public class CheckLearnerDNStatusTask implements Runnable {

    public static final int frequency = 2;

    protected static final Logger logger = LoggerFactory.getLogger(CheckLearnerDNStatusTask.class);

    private static final String SHOW_SLAVE_STATUS = "show slave status";

    private static final String ACTIVE_SESSION =
        "select sum(case when command <> 'Sleep' then 1 else 0 end) active_session from INFORMATION_SCHEMA.PROCESSLIST";

    protected InternalNodeManager nodeManager;

    public CheckLearnerDNStatusTask(InternalNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public void run() {
        InternalNode localNode = ServiceProvider.getInstance().getServer().getLocalNode();
        if (localNode != null && localNode.isLeader() &&
            nodeManager.getAllNodes().getOtherActiveNodes() != null &&
            nodeManager.getAllNodes().getOtherActiveNodes().size() > 0) {
            Map<String, List<InternalNode>> instIdsNodes = new HashMap<>();
            for (InternalNode node : nodeManager.getAllNodes().getOtherActiveNodes()) {
                if (!instIdsNodes.containsKey(node.getInstId())) {
                    instIdsNodes.put(node.getInstId(), new ArrayList<>());
                }
                instIdsNodes.get(node.getInstId()).add(node);
            }

            if (instIdsNodes.size() > 1) {

                Map<String, PolarDBXStatus> polarDBXStatusMap = new HashMap<>();

                //check the slave status if here exist multi read-only instances.
                Map<String, List<StorageInfoRecord>> instIdStorageList = new HashMap<>();
                for (Map.Entry<String, List<InternalNode>> entry : instIdsNodes.entrySet()) {
                    String instId = entry.getKey();
                    try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                        storageInfoAccessor.setConnection(metaDbConn);
                        List<StorageInfoRecord> storageList = storageInfoAccessor.getStorageInfosByInstIdAndKind(
                            instId,
                            StorageInfoRecord.INST_KIND_SLAVE);
                        instIdStorageList.put(instId, storageList);
                    } catch (Throwable ex) {
                        logger.warn("get slave nodes failed!", ex);
                    }
                }

                for (Map.Entry<String, List<StorageInfoRecord>> storageEntry : instIdStorageList.entrySet()) {

                    long delaySecond = 0;
                    long activeSession = 0;
                    List<StorageInfoRecord> valueInfos = storageEntry.getValue();
                    for (StorageInfoRecord slaveStorageInfo : valueInfos) {
                        try (Connection salveConn = DbTopologyManager.getConnectionForStorage(slaveStorageInfo)) {
                            Statement stmt = null;
                            try {
                                stmt = salveConn.createStatement();
                                stmt.execute(SHOW_SLAVE_STATUS);
                                ResultSet result = stmt.getResultSet();
                                if (result.next()) {
                                    Object ret = result.getObject("Seconds_Behind_Master");
                                    if (ret != null) {
                                        delaySecond = Math.max(delaySecond, Long.valueOf(String.valueOf(ret)));
                                    } else {
                                        delaySecond = Math.max(0, delaySecond);
                                    }
                                }
                            } finally {
                                if (stmt != null) {
                                    stmt.close();
                                }
                            }

                            try {
                                stmt = salveConn.createStatement();
                                stmt.execute(ACTIVE_SESSION);
                                ResultSet result = stmt.getResultSet();
                                if (result.next()) {
                                    activeSession += result.getLong(1);
                                }
                            } finally {
                                if (stmt != null) {
                                    stmt.close();
                                }
                            }

                        } catch (Throwable e) {
                            logger.error("check slave delay error!", e);
                        }
                    }

                    boolean highDelay = false;
                    if (delaySecond > MppConfig.getInstance().getLearnerDelayThreshold()) {
                        highDelay = true;
                        logger.warn(storageEntry.getKey() + " delay " + delaySecond + " seconds behind master!");
                    }

                    boolean highLoad = false;
                    if (activeSession > MppConfig.getInstance().getLearnerLoadThreshold() * valueInfos.size()) {
                        highLoad = true;
                        logger.warn(storageEntry.getKey() + " has " + activeSession + " active session!");
                    }
                    polarDBXStatusMap.put(storageEntry.getKey(), new PolarDBXStatus(highDelay, highLoad));
                }
                if (polarDBXStatusMap.size() > 1) {
                    try {
                        SyncManagerHelper
                            .sync(new RefreshPolarDBStatusSyncAction(polarDBXStatusMap), null, MASTER_ONLY);
                    } catch (Throwable e) {
                        logger.error("check slave delay error!", e);
                    }
                }
            }
        }
    }
}