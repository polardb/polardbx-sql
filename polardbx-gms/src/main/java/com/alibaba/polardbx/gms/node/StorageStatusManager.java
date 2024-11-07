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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.RefreshStorageStatusSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class StorageStatusManager extends AbstractLifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(StorageLearnerStatusTask.class);
    private static final String SHOW_SLAVE_STATUS = "show slave status";

    private static final String ACTIVE_SESSION =
        "select sum(case when command <> 'Sleep' then 1 else 0 end) active_session from INFORMATION_SCHEMA.PROCESSLIST";

    private static StorageStatusManager instance = new StorageStatusManager();

    private static long KEEPALIVE_INTERVAL = 1L;

    private static int WINDOW_SIZE = 3;

    private static int UNNOTIFY_SYNC_THRESHOLD = 60;

    private Map<String, StorageStatus> statusMap = new HashMap<>();

    public static StorageStatusManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isMasterMode()) {
            ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("Storage-Status-Factory", true));
            scheduledExecutorService
                .scheduleWithFixedDelay(new StorageLearnerStatusTask(), 0L, KEEPALIVE_INTERVAL,
                    TimeUnit.SECONDS);
        }
    }

    public void setStorageStatus(Map<String, StorageStatus> statusMap) {
        this.statusMap = statusMap;
    }

    public Map<String, StorageStatus> getStorageStatus() {
        return statusMap;
    }

    public class StorageLearnerStatusTask implements Runnable {

        private LinkedList<Map<String, StorageStatus>> windowStatus = new LinkedList<Map<String, StorageStatus>>();

        private int unnotifyNum = 0;

        private Map<String, StorageStatus> calculateSendStatus(Map<String, StorageStatus> currentElement) {
            if (windowStatus.size() >= WINDOW_SIZE) {
                windowStatus.removeFirst();
            }
            windowStatus.addLast(currentElement);

            Map<String, StorageStatus> maybeSendStatus = new HashMap<>();
            if (windowStatus.size() >= WINDOW_SIZE) {
                Map<String, List<StorageStatus>> storageIdStatus = new HashMap<>();
                for (Map<String, StorageStatus> status : windowStatus) {
                    for (Map.Entry<String, StorageStatus> entry : status.entrySet()) {
                        storageIdStatus.computeIfAbsent(
                            entry.getKey(), b -> new ArrayList<>()).add(entry.getValue());
                    }
                }
                for (Map.Entry<String, List<StorageStatus>> entry : storageIdStatus.entrySet()) {
                    StorageStatus lastStorageStatus = entry.getValue().get(entry.getValue().size() - 1);
                    StorageStatus clone = lastStorageStatus.clone();
                    boolean busy = entry.getValue().stream().anyMatch(t -> t.isBusy());
                    boolean delay = entry.getValue().stream().anyMatch(t -> t.isDelay());
                    clone.setBusy(busy);
                    clone.setDelay(delay);
                    maybeSendStatus.put(entry.getKey(), clone);
                }
            }
            return maybeSendStatus;
        }

        @Override
        public void run() {

            try {
                if (LeaderStatusBridge.getInstance().hasLeadership()) {
                    //1. detection the master && htap learner.
                    Map<String, StorageStatus> polarDBXStatusMap = new HashMap<>();
                    //storageStatusMap only storageIds of master && htap-learner.
                    Map<String, StorageInstHaContext> storageStatusMap =
                        StorageHaManager.getInstance().getStorageHaCtxCache();
                    Iterator<StorageInstHaContext> iterator = storageStatusMap.values().stream().iterator();
                    while (iterator.hasNext()) {
                        StorageInstHaContext instHaContext = iterator.next();
                        if (instHaContext != null) {
                            detection(() -> {
                                    if (instHaContext.isMasterMode()) {
                                        return DbTopologyManager.getFollowerConnectionForStorage(instHaContext);
                                    } else {
                                        return DbTopologyManager.getConnectionForStorage(instHaContext);
                                    }
                                }, instHaContext.getInstId(), instHaContext.getStorageInstId(),
                                polarDBXStatusMap);
                        }
                    }

                    Map<String, StorageStatus> maybeSendStatus = null;
                    if (polarDBXStatusMap.size() > 0) {
                        maybeSendStatus = calculateSendStatus(polarDBXStatusMap);
                    }

                    if (maybeSendStatus != null && maybeSendStatus.size() > 0) {
                        boolean change = false;
                        for (Map.Entry<String, StorageStatus> entry : maybeSendStatus.entrySet()) {
                            StorageStatus storageStatus = entry.getValue();
                            StorageStatus last = statusMap.get(entry.getKey());
                            if (last == null || !last.satisfy(storageStatus)) {
                                change = true;
                                break;
                            }
                        }
                        if (change) {
                            unnotifyNum = 0;
                        } else {
                            unnotifyNum++;
                        }
                        if (change || unnotifyNum > UNNOTIFY_SYNC_THRESHOLD) {
                            GmsSyncManagerHelper
                                .sync(new RefreshStorageStatusSyncAction(polarDBXStatusMap),
                                    SystemDbHelper.DEFAULT_DB_NAME,
                                    SyncScope.NOT_COLUMNAR_SLAVE);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("check slave delay error!", t);
            }
        }

        private void detection(Supplier<Connection> connectionSupplier, String instId, String storageId,
                               Map<String, StorageStatus> polarDBXStatusMap) {
            long delaySecond = 0;
            long activeSession = 0;
            Connection salveConn = null;
            try {
                salveConn = connectionSupplier.get();
                Statement stmt = null;
                if (salveConn == null) {
                    return;
                }
                try {
                    stmt = salveConn.createStatement();
                    stmt.execute(SHOW_SLAVE_STATUS);
                    ResultSet result = stmt.getResultSet();
                    if (result.next()) {
                        Object ret = result.getObject("Seconds_Behind_Master");
                        boolean running = result.getBoolean("Slave_SQL_Running");
                        if (running) {
                            if (ret != null) {
                                delaySecond = Long.valueOf(String.valueOf(ret));
                            } else {
                                delaySecond = Integer.MAX_VALUE;
                                //logger.debug("Slave_SQL_Running maybe shutdown!");
                            }
                        } else {
                            delaySecond = Integer.MAX_VALUE;
                            //logger.debug("Slave_SQL_Running shutdown!");
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
                activeSession = 0;
                delaySecond = Integer.MAX_VALUE;
                logger.warn("check slave status error for " + storageId, e);
            } finally {
                if (salveConn != null) {
                    try {
                        salveConn.close();
                    } catch (Throwable t) {
                        //ignore
                    }
                }
            }
            boolean isBusy = activeSession >= DynamicConfig.getInstance().getBusyThreshold();
            boolean isDelay = delaySecond >= DynamicConfig.getInstance().getDelayThreshold();
            if (isDelay) {
//                logger.warn("The storage id " + storageId + " is delay");
            }
            polarDBXStatusMap.put(storageId,
                new StorageStatus(instId, delaySecond, activeSession, isBusy, isDelay));
        }
    }
}
