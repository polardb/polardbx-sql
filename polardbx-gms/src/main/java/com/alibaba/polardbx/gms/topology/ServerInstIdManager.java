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
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class ServerInstIdManager extends AbstractLifecycle {

    protected static ServerInstIdManager instance = new ServerInstIdManager();

    protected String instId = null;
    protected volatile String masterInstId = null;
    protected volatile int instType = ServerInfoRecord.INST_TYPE_MASTER;
    protected volatile Set<String> allReadOnlyInstIdSet = new HashSet<>();

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
        loadAllReadOnlyInstIdSet();
        initInstType();
    }

    public void reload() {
        loadMasterInstId();
        loadAllReadOnlyInstIdSet();
        initInstType();
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

    protected void loadAllReadOnlyInstIdSet() {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            // Try to get all read-only inst id list from storage_info
            List<String> roInstList = storageInfoAccessor.getServerReadOnlyInstIdSetFromStorageInfo();

            Set<String> newAllReadOnlyInstIdSet = new HashSet<>();
            newAllReadOnlyInstIdSet.addAll(roInstList);

            allReadOnlyInstIdSet = newAllReadOnlyInstIdSet;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected void initInstType() {
        String instId = InstIdUtil.getInstId();
        if (instId.equalsIgnoreCase(masterInstId)) {
            instType = ServerInfoRecord.INST_TYPE_MASTER;
        } else {
            instType = ServerInfoRecord.INST_TYPE_SLAVE;
        }
    }

    public boolean isMasterInst() {
        return instType == ServerInfoRecord.INST_TYPE_MASTER;
    }

    public boolean isMasterInstId(String instId) {
        return masterInstId.equalsIgnoreCase(instId);
    }

    public String getMasterInstId() {
        return masterInstId;
    }

    public Set<String> getAllReadOnlyInstIdSet() {
        return allReadOnlyInstIdSet;
    }

    public String getInstId() {
        return InstIdUtil.getInstId();
    }

    public int getInstType() {
        return instType;
    }

}
