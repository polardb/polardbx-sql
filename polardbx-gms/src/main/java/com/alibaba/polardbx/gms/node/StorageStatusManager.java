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
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.gms.sync.SyncScope.ALL;

public class StorageStatusManager extends AbstractLifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(StorageLearnerStatusTask.class);
    private static final String SHOW_SLAVE_STATUS = "show slave status";

    private static final String ACTIVE_SESSION =
        "select sum(case when command <> 'Sleep' then 1 else 0 end) active_session from INFORMATION_SCHEMA.PROCESSLIST";

    private static StorageStatusManager instance = new StorageStatusManager();

    private static long KEEPALIVE_INTERVAR = 3L;

    private Map<String, StorageStatus> statusMap = new HashMap<>();

    /**
     * 允许承担来自于主CN的请求路由给该只读实例集合
     */
    private Set<String> allowedReadLearnerIds = new HashSet<>();
    /**
     * 允许承担来自于主CN的请求路由给该只读DN集合
     */
    private Map<String, StorageStatus> allowReadLearnerStorageMap = new HashMap<>();

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
                .scheduleWithFixedDelay(new StorageLearnerStatusTask(), 0L, KEEPALIVE_INTERVAR,
                    TimeUnit.SECONDS);
        }
    }

    public void setStorageStatus(Map<String, StorageStatus> statusMap) {

        this.statusMap = statusMap;
        Map<String, StorageStatus> allowReadLearnerStorageMap = new HashMap<>();
        if (!ConfigDataMode.isMasterMode()) {
            //pick the learners belong to the current pxc for the slave
            String currentId = ServerInstIdManager.getInstance().getInstId();
            Set<String> storageIds = ServerInstIdManager.getInstance().getInstId2StorageIds().get(currentId);

            Iterator<Map.Entry<String, StorageStatus>> iterator = this.statusMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, StorageStatus> iter = iterator.next();
                if (storageIds.contains(iter.getKey())) {
                    allowReadLearnerStorageMap.put(iter.getKey(), iter.getValue());
                }
            }
        } else {
            Iterator<Map.Entry<String, StorageStatus>> iterator = this.statusMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, StorageStatus> iter = iterator.next();
                StorageStatus ret = iter.getValue();
                if (allowedReadLearnerIds.contains(ret.getInstId())) {
                    allowReadLearnerStorageMap.put(iter.getKey(), iter.getValue());
                }
            }
        }
        this.allowReadLearnerStorageMap = allowReadLearnerStorageMap;
    }

    public Map<String, StorageStatus> getStorageStatus() {
        return statusMap;
    }

    public Map<String, StorageStatus> getAllowReadLearnerStorageMap() {
        return allowReadLearnerStorageMap;
    }

    public class StorageLearnerStatusTask implements Runnable {

        @Override
        public void run() {

            try {
                if (LeaderStatusBridge.getInstance().hasLeadership()) {
                    Map<String, StorageStatus> polarDBXStatusMap = new HashMap<>();
                    Map<String, StorageInstHaContext> storageStatusMap =
                        StorageHaManager.getInstance().getStorageHaCtxCache();
                    Iterator<StorageInstHaContext> iterator = storageStatusMap.values().stream().iterator();
                    while (iterator.hasNext()) {
                        StorageInstHaContext instHaContext = iterator.next();
                        if (instHaContext != null && !instHaContext.isMasterMode()) {

                            long delaySecond = 0;
                            long activeSession = 0;
                            try (Connection salveConn = DbTopologyManager.getConnectionForStorage(instHaContext)) {
                                Statement stmt = null;
                                try {
                                    stmt = salveConn.createStatement();
                                    stmt.execute(SHOW_SLAVE_STATUS);
                                    ResultSet result = stmt.getResultSet();
                                    if (result.next()) {
                                        Object ret = result.getObject("Seconds_Behind_Master");
                                        if (ret != null) {
                                            delaySecond = Long.valueOf(String.valueOf(ret));
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
                                logger.warn("check slave status error for " + instHaContext.getStorageInstId(), e);
                            }
                            boolean isBusy = activeSession >= DynamicConfig.getInstance().getBusyThreshold();
                            boolean isDelay = delaySecond >= DynamicConfig.getInstance().getDelayThreshold();

                            polarDBXStatusMap
                                .put(instHaContext.getStorageInstId(), new StorageStatus(
                                    instHaContext.getInstId(), delaySecond, activeSession, isBusy, isDelay));
                        }
                    }
                    if (polarDBXStatusMap.size() > 0) {
                        GmsSyncManagerHelper
                            .sync(new RefreshStorageStatusSyncAction(polarDBXStatusMap), SystemDbHelper.DEFAULT_DB_NAME,
                                ALL);
                    }
                }
            } catch (Throwable t) {
                logger.error("check slave delay error!", t);
            }
        }
    }

    public void allowedReadLearnerIds(Set<String> allowedReadLearnerIds) {
        this.allowedReadLearnerIds = allowedReadLearnerIds;
    }
}
