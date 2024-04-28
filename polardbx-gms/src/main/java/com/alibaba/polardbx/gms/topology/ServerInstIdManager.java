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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class ServerInstIdManager extends AbstractLifecycle {

    protected static ServerInstIdManager instance = new ServerInstIdManager();

    protected String instId = null;
    protected volatile String masterInstId = null;
    protected volatile int instType = ServerInfoRecord.INST_TYPE_MASTER;
    protected volatile Map<String, Set<String>> instId2StorageIds = new HashMap<>();
    protected volatile Set<String> htapLearnerInstIds = new HashSet<>();

    protected ServerInstIdManager() {
    }

    public static ServerInstIdManager getInstance() {
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
        loadMasterInstId();
        loadAllInstIdAndStorageIdSet();
        int instType = loadAllHtapInstIds();
        initInstType(instType);
    }

    public void reload() {
        loadMasterInstId();
        loadAllInstIdAndStorageIdSet();
        int instType = loadAllHtapInstIds();
        initInstType(instType);
        //here register the new storageIds listener after load the new learner InstId.
        registerLearnerStorageInstId();
    }

    public void registerLearnerStorageInstId() {
        if (ConfigDataMode.isMasterMode()) {
            StorageHaManager.getInstance().registerLearnerStorageInstId();
            StorageHaManager.getInstance().updateGroupConfigVersion();
        }
    }

    protected void loadMasterInstId() {
        String masterInstId = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
            serverInfoAccessor.setConnection(metaDbConn);

            // First to try to get master inst id from server_info
            masterInstId = serverInfoAccessor.getMasterInstIdFromMetaDb();
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            if (masterInstId == null) {
                // First to try to get master inst id from storage_info
                masterInstId = storageInfoAccessor.getServerMasterInstIdFromStorageInfo();
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        if (masterInstId == null) {
            // If it is failed to get master inst id from server_info and storage_info,
            // then try to get get master inst id from system.properties that is from server.properties
            masterInstId = InstIdUtil.getMasterInstId();
        }
        this.masterInstId = masterInstId;
    }

    public synchronized void loadAllInstIdAndStorageIdSet() {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            Map<String, Set<String>> instId2StorageIds = storageInfoAccessor.getServerStorageInstIdMapFromStorageInfo();
            this.instId2StorageIds = instId2StorageIds;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    /**
     * load the htap instIds, and return the current instId' type.
     */
    public synchronized int loadAllHtapInstIds() {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessor();
            serverInfoAccessor.setConnection(connection);
            this.htapLearnerInstIds = serverInfoAccessor.getAllHTAPReadOnlyInstIdList();

            return serverInfoAccessor.getServerTypeByInstId(InstIdUtil.getInstId());
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public List<StorageInfoRecord> getSlaveStorageInfosByMasterStorageInstId(String masterStorageId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            return storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(masterStorageId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected void initInstType(int type) {
        String instId = InstIdUtil.getInstId();
        if (instId.equalsIgnoreCase(masterInstId)) {
            instType = ServerInfoRecord.INST_TYPE_MASTER;
        } else {
            if (type == ServerInfoRecord.INST_TYPE_MASTER) {
                throw GeneralUtil.nestedException("The type of storage is invalid!");
            } else if (type == ServerInfoRecord.INST_TYPE_COLUMNAR_SLAVE) {
                instType = ServerInfoRecord.INST_TYPE_COLUMNAR_SLAVE;
            } else {
                instType = ServerInfoRecord.INST_TYPE_ROW_SLAVE;
            }
        }
    }

    public boolean isMasterInstId(String instId) {
        return masterInstId.equalsIgnoreCase(instId);
    }

    public String getMasterInstId() {
        return masterInstId;
    }

    public Set<String> getAllHTAPReadOnlyInstIdSet() {
        return this.htapLearnerInstIds;
    }

    public Set<String> getAllReadOnlyInstIdSet() {
        return this.instId2StorageIds.keySet().stream().filter(s -> !isMasterInstId(s)).collect(Collectors.toSet());
    }

    public Map<String, Set<String>> getInstId2StorageIds() {
        return instId2StorageIds;
    }

    public String getInstId() {
        return InstIdUtil.getInstId();
    }

    public int getInstType() {
        return instType;
    }

}
