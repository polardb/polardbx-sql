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

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.HaSwitcher;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.rpc.XConfig;
import lombok.val;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manager for handling HA of storage
 *
 * @author chenghui.lch
 */
public class StorageHaManager extends AbstractLifecycle {

    /**
     * The log for storage check ha log
     */
    public static final Logger CHECK_HA_LOGGER = LoggerFactory.getLogger("CHECK_HA_LOG");

    private static final Logger logger = LoggerFactory.getLogger(StorageHaManager.class);
    /**
     * <pre>
     *     Key: storageInstId
     *     Val: StorageInstHaContext that maintains the leader and other nodes
     * </pre>
     */
    protected volatile Map<String, StorageInstHaContext> storageHaCtxCache = new ConcurrentHashMap<>();
    /**
     * StorageInstHaContext of MetaDB
     */
    protected volatile StorageInstHaContext metaDbStorageHaCtx;

    /**
     * <pre>
     *     key: groupKeyUpperCase@storageInstId
     *     val: switcher list of group that is a call back to switch datasource for groups
     * </pre>
     */
    protected Map<String, Set<HaSwitcher>> groupSwitcherMap = new ConcurrentHashMap<>();

    protected HaSwitcher metaDbHaSwitcher;
    protected final static int MAX_QUEUE_LEN = 10000;
    protected volatile int checkStorageTaskPeriod = 5000; // 5s
    protected ScheduledExecutorService checkStorageHaTaskExecutor = null;
    protected int refreshStorageInfoOfMetaDbTarkIntervalDelay = checkStorageTaskPeriod * 3; // 15s
    protected int refreshStorageInfoOfMetaDbTarkInterval = checkStorageTaskPeriod * 12; // 60s
    protected ScheduledExecutorService refreshStorageInfoOfMetaDbExecutor = null;
    protected int storageHaManagerExecutorPoolSize = 8;
    protected ThreadPoolExecutor storageHaManagerTaskExecutor = null;
    protected int parallelSwitchDsCountPerStorageInst = 4;
    protected CheckStorageHaTask checkStorageHaTask = new CheckStorageHaTask(this);
    protected RefreshMasterStorageInfosTask refreshMasterStorageInfosTask = new RefreshMasterStorageInfosTask(this);
    protected static StorageHaManager instance = new StorageHaManager();

    /**
     * Enable maintain primary-zone
     * TODO(moyi) not cache it but use ConnectionParams directly
     */
    private volatile boolean enablePrimaryZoneMaintain = false;

    /**
     * Primary zone supplier, to decouple the LocalityManager and StorageHaManager
     */
    private Supplier<PrimaryZoneInfo> primaryZoneInfoSupplier = null;

    protected static class StorageInfoConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            String instId = InstIdUtil.getInstIdFromStorageInfoDataId(dataId);
            StorageHaManager.getInstance().updateStorageInstHaContext(instId);
        }
    }

    public static StorageHaManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    protected StorageHaManager() {

    }

    @Override
    protected void doInit() {

        metaDbHaSwitcher = MetaDbDataSource.getInstance().getMetaDbHaSwitcher();
        loadStorageHaContext();

        storageHaManagerTaskExecutor =
            ExecutorUtil.createBufferedExecutor("StorageHaManagerTaskExecutor", storageHaManagerExecutorPoolSize);
        checkStorageHaTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("CheckStorageHaTaskExecutor", true));

        checkStorageHaTaskExecutor = initStorageHaCheckTaskExecutor(checkStorageTaskPeriod, checkStorageHaTask);
        MetaDbConfigManager.getInstance()
            .register(MetaDbDataIdBuilder.getStorageInfoDataId(InstIdUtil.getInstId()), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getStorageInfoDataId(InstIdUtil.getInstId()),
                new StorageInfoConfigListener());
        if (!ServerInstIdManager.getInstance().isMasterInst()) {
            String serverMasterInstId = ServerInstIdManager.getInstance().getMasterInstId();
            MetaDbConfigManager.getInstance()
                .register(MetaDbDataIdBuilder.getStorageInfoDataId(serverMasterInstId), null);
            MetaDbConfigManager.getInstance()
                .bindListener(MetaDbDataIdBuilder.getStorageInfoDataId(serverMasterInstId),
                    new StorageInfoConfigListener());
        }

        if (ConfigDataMode.isMasterMode()) {
            refreshStorageInfoOfMetaDbExecutor =
                Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("RefreshStorageInfoOfMetaDbTaskExecutor", true));
            refreshStorageInfoOfMetaDbExecutor
                .scheduleAtFixedRate(refreshMasterStorageInfosTask, refreshStorageInfoOfMetaDbTarkIntervalDelay,
                    refreshStorageInfoOfMetaDbTarkInterval, TimeUnit.MILLISECONDS);
        }

    }

    public void adjustStorageHaTaskPeriod(int newPeriod) {

        if (this.checkStorageHaTaskExecutor != null && (newPeriod == this.checkStorageTaskPeriod)) {
            return;
        }
        try {
            /**
             * try to wait 5 min to shutdown original haTask to finish. 
             */
            int maxAwaitTermination = 300;
            if (this.checkStorageHaTaskExecutor != null) {
                this.checkStorageHaTaskExecutor.shutdown();
                try {
                    this.checkStorageHaTaskExecutor.awaitTermination(maxAwaitTermination, TimeUnit.SECONDS);
                } catch (Throwable ex) {
                    // ignore
                    MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.warn("Failed to shutdown old checkStorageHaTaskExecutor", ex);
                }
            }
            this.checkStorageHaTaskExecutor = initStorageHaCheckTaskExecutor(newPeriod, this.checkStorageHaTask);
            this.checkStorageTaskPeriod = newPeriod;
            MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(String
                .format("Successful to adjust the HA task period, old period/new period is %s/%s",
                    this.checkStorageTaskPeriod, newPeriod));
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.error(String
                .format("Failed to adjust the HA task period, old period/new period is %s/%s",
                    this.checkStorageTaskPeriod, newPeriod), ex);
        }

    }

    protected ScheduledExecutorService initStorageHaCheckTaskExecutor(int checkStorageTaskPeriod,
                                                                      Runnable checkStorageHaTask) {
        ScheduledExecutorService checkStorageHaTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("CheckStorageHaTaskExecutor", true));
        checkStorageHaTaskExecutor.scheduleAtFixedRate(
            checkStorageHaTask,
            checkStorageTaskPeriod,
            checkStorageTaskPeriod,
            TimeUnit.MILLISECONDS);
        return checkStorageHaTaskExecutor;
    }

    public void registerHaSwitcher(String storageInstId, String dbName, String groupKey, HaSwitcher haSwitcher) {
        String haGroupKey = GroupInfoUtil.buildHaGroupKey(storageInstId, groupKey);
        Set<HaSwitcher> switcherSet = groupSwitcherMap.get(haGroupKey);
        HaSwitcher groupSwitchProxy = new GroupHaSwitcherProxy(storageInstId, dbName, groupKey, haSwitcher, this);
        if (switcherSet == null) {
            switcherSet = new HashSet<>();
            switcherSet.add(groupSwitchProxy);
            groupSwitcherMap.put(haGroupKey, switcherSet);
        } else {
            switcherSet.add(groupSwitchProxy);
        }
    }

    public void unregisterHaSwitcher(String storageInstId, String groupKey, HaSwitcher haSwitcher) {
        String haGroupKey = GroupInfoUtil.buildHaGroupKey(storageInstId, groupKey);
        Set<HaSwitcher> switcherSet = groupSwitcherMap.get(haGroupKey);
        if (switcherSet != null) {
            if (switcherSet.contains(haSwitcher)) {
                switcherSet.remove(haSwitcher);
            }
            if (switcherSet.isEmpty()) {
                groupSwitcherMap.remove(haGroupKey);
            }
        }
    }

    public void clearHaSwitcher(String dbName) {
        String currInstId = InstIdUtil.getInstId();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoRecord> groupDetailInfoRecords =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(currInstId, dbName);
            if (!ServerInstIdManager.getInstance().isMasterInst()) {
                String masterInstId = ServerInstIdManager.getInstance().getMasterInstId();
                List<GroupDetailInfoRecord> groupDetailInfoRecordsOfMasterInstId =
                    groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(masterInstId, dbName);
                groupDetailInfoRecords.addAll(groupDetailInfoRecordsOfMasterInstId);
            }
            for (int i = 0; i < groupDetailInfoRecords.size(); i++) {
                GroupDetailInfoRecord groupDetailInfoRecord = groupDetailInfoRecords.get(i);
                String groupName = groupDetailInfoRecord.groupName;
                String storageInstId = groupDetailInfoRecord.storageInstId;
                String haGroupKey = GroupInfoUtil.buildHaGroupKey(storageInstId, groupName);
                groupSwitcherMap.remove(haGroupKey);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to clear Group HaSwitcher", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Failed to clear Group HaSwitchers for db", ex);
        }
    }

    public String getMetaDbStorageInstId() {
        String storageInstId = "";
        if (metaDbStorageHaCtx != null) {
            storageInstId = metaDbStorageHaCtx.storageInstId;
        }
        return storageInstId;
    }

    private static int getAndCheckXport(String addr, StorageInstHaContext haCtx, StorageNodeHaInfo haInfo) {
        final boolean isVip = haCtx.storageVipAddr != null && haCtx.storageVipAddr.equals(addr);
        if (isVip) {
            if (XConfig.VIP_WITH_X_PROTOCOL) {
                Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(addr);
                MetaDbLogUtil.META_DB_LOG.info("Xport is vip: " + addr);
                return nodeIpPort.getValue();
            } else if (XConfig.GALAXY_X_PROTOCOL) {
                final int expected = haInfo.getXPort();
                MetaDbLogUtil.META_DB_LOG.info("Got xport of galaxy vip node " + addr + " is " + expected);
                return expected;
            }
        } else if (!isVip && haInfo != null) {
            final int expected = haInfo.getXPort();
            MetaDbLogUtil.META_DB_LOG.info("Got xport node " + addr + " is " + expected);
            return expected;
        }
        if (null == haInfo) {
            MetaDbLogUtil.META_DB_LOG.info("Target haInfo not found: " + addr + " Xport not available.");
        } else {
            MetaDbLogUtil.META_DB_LOG.info("Target is vip: " + addr + " Xport not available.");
        }
        return -1;
    }

    public HaSwitchParams getStorageHaSwitchParamsWithReadLock(String storageInstId) {
        StorageInstHaContext storageInstHaContext = storageHaCtxCache.get(storageInstId);
        if (storageInstHaContext == null) {
            List<String> newStorageInstIdList = new ArrayList<>();
            newStorageInstIdList.add(storageInstId);
            addStorageInsts(newStorageInstIdList);
            storageInstHaContext = storageHaCtxCache.get(storageInstId);
        }
        if (storageInstHaContext == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("No found storage inst id for %s", storageInstId));
        }
        HaSwitchParams params = null;
        try {
            storageInstHaContext.getHaLock().readLock().lock();
            params = getStorageHaSwitchParams(storageInstId);
        } finally {
            storageInstHaContext.getHaLock().readLock().unlock();
        }
        return params;
    }

    public HaSwitchParams getStorageHaSwitchParams(String storageInstId) {
        StorageInstHaContext storageInstHaContext = storageHaCtxCache.get(storageInstId);
        if (storageInstHaContext == null) {
            List<String> newStorageInstIdList = new ArrayList<>();
            newStorageInstIdList.add(storageInstId);
            addStorageInsts(newStorageInstIdList);
            storageInstHaContext = storageHaCtxCache.get(storageInstId);
        }
        if (storageInstHaContext == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("No found storage inst id for %s", storageInstId));
        }
        HaSwitchParams haSwitchParams = new HaSwitchParams();
        haSwitchParams.userName = storageInstHaContext.user;
        haSwitchParams.passwdEnc = storageInstHaContext.encPasswd;
        haSwitchParams.storageInstId = storageInstId;
        haSwitchParams.storageConnPoolConfig = StorageHaManager.getConnPoolConfigFromManager();
        haSwitchParams.storageHaInfoMap = storageInstHaContext.allStorageNodeHaInfoMap;
        haSwitchParams.curAvailableAddr = storageInstHaContext.currAvailableNodeAddr;
        haSwitchParams.xport = getAndCheckXport(storageInstHaContext.currAvailableNodeAddr, storageInstHaContext,
            storageInstHaContext.allStorageNodeHaInfoMap.get(storageInstHaContext.currAvailableNodeAddr));
        haSwitchParams.phyDbName = null;
        haSwitchParams.storageKind = storageInstHaContext.storageKind;
        return haSwitchParams;
    }

    public HaSwitchParams getStorageHaSwitchParamsForInitGroupDs(String instId,
                                                                 String dbName,
                                                                 String groupName) {

        GroupDetailInfoRecord groupDetailInfoRecord = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            groupDetailInfoRecord =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, dbName, groupName);
            if (groupDetailInfoRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("No find any group detail for [%s/%s/%s]", instId, dbName, groupName));
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        String storageInstId = null;
        HaSwitchParams haSwitchParams = null;
        try {
            String phyDbName = DbTopologyManager.getPhysicalDbNameByGroupKey(dbName, groupName);
            storageInstId = groupDetailInfoRecord.storageInstId;
            if (SystemDbHelper.DEFAULT_DB_NAME.equals(dbName)) {
                storageInstId = metaDbStorageHaCtx.getStorageInstId();
            }
            haSwitchParams = getStorageHaSwitchParamsWithReadLock(storageInstId);
            if (haSwitchParams == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("No find any storage ha info for [%s/%s/%s/%s]", instId, dbName, groupName,
                        storageInstId));
            }
            haSwitchParams.phyDbName = phyDbName;
        } catch (Throwable ex) {
            throw ex;
        }
        return haSwitchParams;
    }

    protected synchronized void updateStorageInstHaContext(String instId) {
        List<String> storageInstIdListToBeAdded = new ArrayList<>();
        List<String> storageInstIdListToBeDeleted = new ArrayList<>();
        List<String> storageInstIdListToBeUpdated = new ArrayList<>();
        Map<String, List<StorageInfoRecord>> newStorageInstNodeInfoMap = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            // Get storage all nodes of one polardbx inst
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);

            // Group the storage node by storageInstId
            // key: storageInstId, val: the node list of storage inst
            for (int i = 0; i < storageInfoRecords.size(); i++) {
                StorageInfoRecord storageInfo = storageInfoRecords.get(i);
                String storageInstId = storageInfo.storageInstId;
                List<StorageInfoRecord> storageNodeList = newStorageInstNodeInfoMap.get(storageInstId);
                if (storageNodeList == null) {
                    storageNodeList = new ArrayList<>();
                    newStorageInstNodeInfoMap.put(storageInstId, storageNodeList);
                }
                storageNodeList.add(storageInfo);
            }

            // When curr inst is a slave inst, it need listen the all storage info change both from
            // server master inst and server salve inst.
            // Because the input params "instId" may be a masterInstId or a slaveInstId,
            // here need just fetch all storageInstId from the input param "instId".
            Map<String, StorageInstHaContext> storagInstHaCtxCacheOfInstId =
                new HashMap<String, StorageInstHaContext>();
            Set<String> allStorageInstIdInCache = this.storageHaCtxCache.keySet();
            for (String storageInstIdInCache : allStorageInstIdInCache) {
                StorageInstHaContext haContext = this.storageHaCtxCache.get(storageInstIdInCache);
                if (haContext.instId.equalsIgnoreCase(instId)) {
                    storagInstHaCtxCacheOfInstId.put(haContext.storageInstId, haContext);
                }
            }

            // Find storageInst list to be added
            for (String storageInstIdVal : newStorageInstNodeInfoMap.keySet()) {
                if (!storagInstHaCtxCacheOfInstId.containsKey(storageInstIdVal)) {
                    storageInstIdListToBeAdded.add(storageInstIdVal);
                }
            }

            // Find storageInst list to be deleted
            for (String storageInstIdVal : storagInstHaCtxCacheOfInstId.keySet()) {
                if (!newStorageInstNodeInfoMap.containsKey(storageInstIdVal)) {
                    storageInstIdListToBeDeleted.add(storageInstIdVal);
                }
            }

            // Find storageInst list To be updated
            for (String storageInstIdVal : storagInstHaCtxCacheOfInstId.keySet()) {
                if (newStorageInstNodeInfoMap.containsKey(storageInstIdVal)) {
                    storageInstIdListToBeUpdated.add(storageInstIdVal);
                }
            }

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
        }

        if (!storageInstIdListToBeUpdated.isEmpty()) {
            refreshStorageInsts(storageInstIdListToBeUpdated, newStorageInstNodeInfoMap);
        }

        // Add new storage inst, this is may be scale-out scenario
        if (!storageInstIdListToBeAdded.isEmpty()) {
            addStorageInsts(storageInstIdListToBeAdded);
        }

        // Delete old storage inst, this is may be scale-in scenario
        if (!storageInstIdListToBeDeleted.isEmpty()) {
            removeStorageInsts(storageInstIdListToBeDeleted);
        }
    }

    public final Map<String, StorageInstHaContext> refreshAndGetStorageInstHaContextCache() {
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getStorageInfoDataId(InstIdUtil.getInstId()));
        if (!ServerInstIdManager.getInstance().isMasterInst()) {
            MetaDbConfigManager.getInstance().sync(ServerInstIdManager.getInstance().getMasterInstId());
        }
        return storageHaCtxCache;
    }

    public Map<String, StorageInstHaContext> getStorageHaCtxCache() {
        return this.storageHaCtxCache;
    }

    protected synchronized void addStorageInsts(List<String> storageInstIdListToBeAdded) {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            for (int i = 0; i < storageInstIdListToBeAdded.size(); i++) {
                String storageInstId = storageInstIdListToBeAdded.get(i);
                // get storage nodes by storageInstId
                List<StorageInfoRecord> storageNodes =
                    storageInfoAccessor.getStorageInfosByStorageInstId(storageInstId);
                if (storageNodes.isEmpty()) {
                    continue;
                }
                // init StorageInst HA context for each group of storage nodes
                StorageInstHaContext storageInstHaContext = buildStorageInstHaContext(storageNodes);
                storageHaCtxCache.put(storageInstId, storageInstHaContext);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, "Failed to add storage inst");
        }
    }

    protected synchronized void removeStorageInsts(List<String> storageInstIdListToBeDeleted) {
        for (int i = 0; i < storageInstIdListToBeDeleted.size(); i++) {
            String storageInstId = storageInstIdListToBeDeleted.get(i);
            storageHaCtxCache.remove(storageInstId);
        }
    }

    /**
     * Update the hosts for storage inst
     */
    protected synchronized void refreshStorageInsts(List<String> storageInstIdListToBeUpdated,
                                                    Map<String, List<StorageInfoRecord>> newStorageInstNodeInfoMap) {

        for (int i = 0; i < storageInstIdListToBeUpdated.size(); i++) {
            String storageInstId = storageInstIdListToBeUpdated.get(i);
            StorageInstHaContext storageInstHaContext = this.storageHaCtxCache.get(storageInstId);
            Map<String, StorageInfoRecord> newStorageNodeInfos = new HashMap<>();
            List<StorageInfoRecord> newStorageNodes = newStorageInstNodeInfoMap.get(storageInstId);
            String vipInfo = null;
            for (int j = 0; j < newStorageNodes.size(); j++) {
                StorageInfoRecord storageInfo = newStorageNodes.get(j);
                String ip = storageInfo.ip;
                Integer port = storageInfo.port;
                int isVip = storageInfo.isVip;
                String addrStr = AddressUtils.getAddrStrByIpPort(ip, port);
                if (isVip == StorageInfoRecord.IS_VIP_TRUE) {
                    vipInfo = addrStr;
                    if (storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
                        storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                        // if current storage inst is a xcluster inst,
                        // then its vip info should be ignored in storageInfos of storageInstHaContext
                        continue;
                    }
                }
                newStorageNodeInfos.put(addrStr, storageInfo);
            }
            storageInstHaContext.storageNodeInfos = newStorageNodeInfos;
            if (vipInfo != null) {
                storageInstHaContext.storageVipAddr = vipInfo;
            }
        }
    }

    protected void loadStorageHaContext() {

        Map<String, StorageInstHaContext> newStorageHaCtxCache = new ConcurrentHashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            List<StorageInfoRecord> storageInfoRecords = new ArrayList<>();

            // Get storage all nodes of one polardbx inst, current instId maybe a master/read-only inst
            loadAllStorageInfoForCurrentInst(storageInfoAccessor, InstIdUtil.getInstId(), storageInfoRecords);

            // group storage node by storageInstId
            Map<String, List<StorageInfoRecord>> storageInstNodeInfoMap = new HashMap<>();
            for (int i = 0; i < storageInfoRecords.size(); i++) {
                StorageInfoRecord storageInfo = storageInfoRecords.get(i);
                String storageInstId = storageInfo.storageInstId;
                List<StorageInfoRecord> storageNodeList = storageInstNodeInfoMap.get(storageInstId);
                if (storageNodeList == null) {
                    storageNodeList = new ArrayList<>();
                    storageInstNodeInfoMap.put(storageInstId, storageNodeList);
                }
                storageNodeList.add(storageInfo);
            }

            // init StorageInst HA context for each x-cluster group of storage nodes
            for (Map.Entry<String, List<StorageInfoRecord>> storageInstNodesItem : storageInstNodeInfoMap.entrySet()) {
                String storageInstId = storageInstNodesItem.getKey();
                List<StorageInfoRecord> storageInstNodes = storageInstNodesItem.getValue();
                StorageInstHaContext storageInstHaContext = buildStorageInstHaContext(storageInstNodes);
                newStorageHaCtxCache.put(storageInstId, storageInstHaContext);
                if (storageInstHaContext.storageKind == StorageInfoRecord.INST_KIND_META_DB) {
                    this.metaDbStorageHaCtx = storageInstHaContext;
                }
            }
            this.storageHaCtxCache = newStorageHaCtxCache;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.info(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                "Failed to init storage HA context from metaDb，err is " + ex.getMessage());
        }
    }

    protected void loadAllStorageInfoForCurrentInst(StorageInfoAccessor storageInfoAccessor, String currInstId,
                                                    List<StorageInfoRecord> storageInfoRecords) {

        // load storage infos for current inst
        List<StorageInfoRecord> storageInfoRecordsOfCurrInstId =
            storageInfoAccessor.getStorageInfosByInstId(currInstId);
        storageInfoRecords.addAll(storageInfoRecordsOfCurrInstId);

        // if current inst is a slave inst, then load all storage info for server master inst id
        if (!ServerInstIdManager.getInstance().isMasterInst()) {
            List<StorageInfoRecord> storageInfoRecordsOfServerMasterInstId =
                storageInfoAccessor.getStorageInfosByInstId(ServerInstIdManager.getInstance().getMasterInstId());
            storageInfoRecords.addAll(storageInfoRecordsOfServerMasterInstId);
        }
    }

    protected StorageInstHaContext buildStorageInstHaContext(
        List<StorageInfoRecord> storageNodes) {
        assert storageNodes.size() > 0;
        StorageInfoRecord firstNode = storageNodes.get(0);
        String instId = firstNode.instId;
        String storageInstId = firstNode.storageInstId;
        String storageMasterInstId = firstNode.storageMasterInstId;
        String user = firstNode.user;
        String encPasswd = firstNode.passwdEnc;
        int storageType = firstNode.storageType;
        int instKind = firstNode.instKind;
        String storageVipAddrStr = null;
        List<String> addrList = new ArrayList<>();
        Map<String, StorageInfoRecord> addrStorageNodeMap = new HashMap<>();

        for (int i = 0; i < storageNodes.size(); i++) {
            StorageInfoRecord storageNode = storageNodes.get(i);
            if (storageNode.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                storageVipAddrStr = AddressUtils.getAddrStrByIpPort(storageNode.ip, storageNode.port);
                if (storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
                    storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                    // if current storage inst is a xcluster inst,
                    // then its vip info should be ignored in getStorageRole info
                    continue;
                }
            }
            String addr = AddressUtils.getAddrStrByIpPort(storageNode.ip, storageNode.port);
            addrList.add(addr);
            addrStorageNodeMap.put(addr, storageNode);
        }

        boolean allowFetchRoleOnlyFromLeader = true;
        if (instKind == StorageInfoRecord.INST_KIND_SLAVE) {
            allowFetchRoleOnlyFromLeader = false;
        }
        Map<String, StorageNodeHaInfo> storageNodeHaInfoMap =
            StorageHaChecker
                .checkAndFetchRole(addrList, storageVipAddrStr, 1 == storageNodes.size() ? firstNode.xport : -1, user,
                    PasswdUtil.decrypt(encPasswd), storageType, instKind, allowFetchRoleOnlyFromLeader);

        return StorageInstHaContext
            .buildStorageInstHaContext(instId,
                storageInstId,
                storageMasterInstId,
                user,
                encPasswd,
                storageType,
                instKind,
                storageVipAddrStr,
                storageNodeHaInfoMap,
                addrStorageNodeMap);
    }

    protected static class GroupHaSwitcherProxy implements HaSwitcher {
        protected HaSwitcher haSwitcher;
        protected String storageInstId;
        protected String dbName;
        protected String groupName;
        protected String phyDbName;
        protected StorageHaManager storageHaManager;

        @Override
        public int hashCode() {
            return haSwitcher.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return haSwitcher.equals(obj);
        }

        @Override
        public String toString() {
            return haSwitcher.toString();
        }

        public GroupHaSwitcherProxy(String storageInstId, String dbName, String groupName,
                                    HaSwitcher haSwitcher, StorageHaManager storageHaManager) {
            this.storageHaManager = storageHaManager;
            this.haSwitcher = haSwitcher;
            this.storageInstId = storageInstId;
            this.dbName = dbName;
            this.groupName = groupName;
            this.phyDbName = DbTopologyManager.getPhysicalDbNameByGroupKey(dbName, groupName);
        }

        @Override
        public void doHaSwitch(HaSwitchParams haSwitchParams) {
            HaSwitchParams haParams = new HaSwitchParams();
            haParams.userName = haSwitchParams.userName;
            haParams.passwdEnc = haSwitchParams.passwdEnc;
            haParams.phyDbName = this.phyDbName;
            haParams.storageInstId = haSwitchParams.storageInstId;
            haParams.storageConnPoolConfig = haSwitchParams.storageConnPoolConfig;
            haParams.curAvailableAddr = haSwitchParams.curAvailableAddr;
            haParams.storageHaInfoMap = haSwitchParams.storageHaInfoMap;
            haParams.xport = haSwitchParams.xport;
            haParams.storageKind = haSwitchParams.storageKind;
            haSwitcher.doHaSwitch(haParams);
        }
    }

    protected static class GroupHaSwitchTask implements Runnable {

        protected StorageHaManager storageHaManager;
        protected List<String> groupList;
        protected HaSwitchParams haSwitchParams;
        protected Map<String, Throwable> grpExceptionMap;

        public GroupHaSwitchTask(List<String> groupList, HaSwitchParams haSwitchParams,
                                 StorageHaManager storageHaManager) {
            this.groupList = groupList;
            this.haSwitchParams = haSwitchParams;
            this.storageHaManager = storageHaManager;
            this.grpExceptionMap = new HashMap<>();
        }

        @Override
        public void run() {
            try {
                doSwitchDsTask();
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }

        protected void doSwitchDsTask() {
            for (String groupKey : groupList) {
                int grpHaSwitcherCnt = 0;
                try {
                    String storageInstId = haSwitchParams.storageInstId;
                    String haGroupKey = GroupInfoUtil.buildHaGroupKey(storageInstId, groupKey);
                    Set<HaSwitcher> haSwitcherSet = storageHaManager.groupSwitcherMap.get(haGroupKey);

                    if (haSwitcherSet == null || haSwitcherSet.isEmpty()) {
                        continue;
                    }

                    grpHaSwitcherCnt = haSwitcherSet.size();
                    for (HaSwitcher haSwitcher : haSwitcherSet) {
                        haSwitcher.doHaSwitch(haSwitchParams);
                    }
                } catch (Throwable ex) {
                    MetaDbLogUtil.META_DB_LOG.error(ex);
                    this.grpExceptionMap.put(groupKey, ex);
                } finally {
                    logSwitchDsTask(groupKey, grpHaSwitcherCnt, haSwitchParams, grpExceptionMap);
                }
            }
        }

        protected void logSwitchDsTask(String grpKey, int grpHaSwitcherCnt, HaSwitchParams haSwitchParams,
                                       Map<String, Throwable> grpExceptionMap) {
            String curAvailableAddr = haSwitchParams.curAvailableAddr;
            String storageInstId = haSwitchParams.storageInstId;
            Throwable ex = grpExceptionMap.get(grpKey);
            boolean isSucc = ex == null;
            String taskInfo = String.format("newLeader/dnId=[%s/%s]", curAvailableAddr, storageInstId);
            String switchDsTaskLogInfo = String
                .format("grpKey=[%s],grpSwitcherCnt=[%s],isSucc=[%s],taskInfo=[%s],exMsg=[%s]", grpKey,
                    grpHaSwitcherCnt, isSucc, taskInfo, isSucc ? "" : ex.getMessage());
            MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(switchDsTaskLogInfo);
        }

    }

    protected static class StorageHaSwitchTask implements Runnable {

        protected String storageInstId;
        protected StorageInstHaContext haContext;
        protected StorageHaManager storageHaManager;
        protected String newAvailableAddr;
        protected Map<String, StorageNodeHaInfo> newStorageNodeHaInfoMap;

        // Task execution results
        private int groupToBeSwitched = 0;
        private int switchTaskCount = 0;
        private String oldAddress;
        private int newXport = 0;
        private List<String> allGrpListToBeSwitched = new ArrayList<>();

        public StorageHaSwitchTask(StorageInstHaContext haContext, StorageHaManager storageHaManager,
                                   String newAvailableAddr,
                                   Map<String, StorageNodeHaInfo> newStorageNodeHaInfoMap) {
            this.haContext = haContext;
            this.storageInstId = haContext.storageInstId;
            this.storageHaManager = storageHaManager;
            this.newAvailableAddr = newAvailableAddr;
            this.newStorageNodeHaInfoMap = newStorageNodeHaInfoMap;
            this.oldAddress = haContext.currAvailableNodeAddr;
        }

        @Override
        public void run() {
            try {
                boolean success = false;
                long startTs = System.currentTimeMillis();
                Throwable ex = null;
                try {
                    haContext.getHaLock().writeLock().lock();
                    success = submitGroupHaSwitchTasks(haContext, newStorageNodeHaInfoMap, storageHaManager);
                } catch (Throwable e) {
                    if (haContext.storageKind == StorageInfoRecord.INST_KIND_META_DB) {
                        MetaDbLogUtil.META_DB_LOG.error("Failed to do MetaDB DataSource HASwitch due to ", e);
                    } else {
                        MetaDbLogUtil.META_DB_LOG.error("Failed to do Group DataSource HASwitch due to ", e);
                    }
                } finally {
                    changeHaStatus(success);
                    haContext.getHaLock().writeLock().unlock();
                    long switchEndTs = System.currentTimeMillis();
                    doStorageInstHaLog(success, this.allGrpListToBeSwitched.size(), this.switchTaskCount,
                        startTs, switchEndTs, this.oldAddress, this.newAvailableAddr, this.newXport);
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }

        private void changeHaStatus(boolean success) {
            synchronized (haContext) {
                if (success) {
                    // all group switch tasks are successful
                    if (haContext.haStatus == StorageInstHaContext.StorageHaStatus.SWITCHING) {
                        haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
                        haContext.currAvailableNodeAddr = this.newAvailableAddr;
                    }
                } else {
                    // change the status to NORMAL to wait for next switch task to retry
                    haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
                }
            }
        }

        protected boolean submitGroupHaSwitchTasks(StorageInstHaContext haContext,
                                                   Map<String, StorageNodeHaInfo> addrWithRoleMap,
                                                   StorageHaManager storageHaManager) {
            // available node addr has changed, so need to do ha switch
            HaSwitchParams haSwitchParams = new HaSwitchParams();
            haSwitchParams.userName = haContext.user;
            haSwitchParams.passwdEnc = haContext.encPasswd;
            haSwitchParams.storageInstId = haContext.storageInstId;
            haSwitchParams.storageConnPoolConfig = getConnPoolConfigFromManager();
            haSwitchParams.storageHaInfoMap = addrWithRoleMap;
            haSwitchParams.curAvailableAddr = this.newAvailableAddr;
            haSwitchParams.xport = newXport =
                getAndCheckXport(this.newAvailableAddr, haContext, addrWithRoleMap.get(this.newAvailableAddr));
            haSwitchParams.storageKind = haContext.storageKind;
            int storageInstKind = haContext.storageKind;

            if (storageInstKind == StorageInfoRecord.INST_KIND_META_DB) {
                // do HA switch for metaDb first,
                // because the group switch tasks of non-metadb need use meta db conn
                Throwable taskEx = null;
                try {
                    storageHaManager.metaDbHaSwitcher.doHaSwitch(haSwitchParams);
                } catch (Throwable ex) {
                    MetaDbLogUtil.META_DB_LOG.error("Failed to do HA switch for meta db", ex);
                    taskEx = ex;
                }
                if (taskEx != null) {
                    // change the status of storage inst to NORMAL to accept the next switch task to do retry
                    haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
                    return false;
                }
            }

            boolean allGrpSwitchTaskSucc = true;

            List<GroupDetailInfoRecord> groupInfoListToBeDoSwitched = new ArrayList<>();
            try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                groupDetailInfoAccessor.setConnection(conn);
                groupInfoListToBeDoSwitched =
                    groupDetailInfoAccessor.getGroupDetailInfoByStorageInstId(storageInstId);
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                allGrpSwitchTaskSucc = false;
            }

            int groupCountOfBeDoSwitched = groupInfoListToBeDoSwitched.size();

            // Compute the count of group for each SwitchDsTask
            int groupCntPerSwitchDsTask =
                (int) (Math.ceil(1.0 * groupCountOfBeDoSwitched
                    / storageHaManager.parallelSwitchDsCountPerStorageInst));

            List<List<String>> grpsList = new ArrayList<List<String>>();
            List<String> tmpGrpList = new ArrayList<>();
            for (int i = 0; i < groupInfoListToBeDoSwitched.size(); i++) {
                if (i % groupCntPerSwitchDsTask == 0) {
                    tmpGrpList = new ArrayList<>();
                    grpsList.add(tmpGrpList);
                }
                tmpGrpList.add(groupInfoListToBeDoSwitched.get(i).groupName);
                allGrpListToBeSwitched.add(groupInfoListToBeDoSwitched.get(i).groupName);
            }

            // Submit SwitchDsTasks
            int taskCount = grpsList.size();
            this.switchTaskCount = taskCount;
            CountDownLatch latch = new CountDownLatch(taskCount);
            List<GroupHaSwitchTask> taskList = new ArrayList<>(taskCount);
            for (List<String> grpList : grpsList) {
                GroupHaSwitchTask haSwitchDsTask = new GroupHaSwitchTask(grpList, haSwitchParams, storageHaManager);
                taskList.add(haSwitchDsTask);
                storageHaManager.storageHaManagerTaskExecutor.submit(() -> {
                    try {
                        haSwitchDsTask.run();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // wait for all tasks
            ExecutorUtil.awaitCountDownLatch(latch);

            allGrpSwitchTaskSucc &= taskList.stream().allMatch(x -> x.grpExceptionMap.isEmpty());

            return allGrpSwitchTaskSucc;
        }

        protected void doStorageInstHaLog(boolean isSucc, int groupCnt, int taskCnt, long beginTs, long endTs,
                                          String oldAddr, String newAddr, int Xport) {
            String storageInstHaLog = "";

            String allGrpListStr = String.join(",", allGrpListToBeSwitched);
            String allRoleInfoStr = buildStorageNodeHaInfoLogMsg(this.newStorageNodeHaInfoMap);
            if (isSucc) {
                storageInstHaLog =
                    String.format(
                        "StorageInst[%s] do HA switch successfully, SwitchInfo is [timeCost(ms)=%s, groupCnt=%s, taskCnt=%s, beginTime=%s, endTime=%s, newAddr=%s, oldAddr=%s, newXport=%s, allGrpList=[%s], roleInfos=[%s]]",
                        storageInstId, endTs - beginTs, groupCnt, taskCnt, beginTs, endTs, newAddr, oldAddr,
                        Xport, allGrpListStr, allRoleInfoStr);
            } else {
                storageInstHaLog =
                    String.format(
                        "StorageInst[%s] do HA switch failed, SwitchInfo is [timeCost(ms)=%s, groupCnt=%s, taskCnt=%s, beginTime=%s, endTime=%s, newAddr=%s, oldAddr=%s, newXport=%s, allGrpList=[%s], roleInfos=[%s]]",
                        storageInstId, endTs - beginTs, groupCnt, taskCnt, beginTs, endTs, newAddr, oldAddr,
                        Xport, allGrpListStr, allRoleInfoStr);
            }
            logger.info(storageInstHaLog);
            MetaDbLogUtil.META_DB_LOG.info(storageInstHaLog);
            MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(storageInstHaLog);
        }
    }

    protected static class CheckStorageRoleInfoTask implements Runnable {

        protected String storageInstId;
        protected StorageInstHaContext haContext;
        /**
         * key: availableAddrStr
         * val: nodeRoleInfos
         * key: nodeAddr
         * val: nodeRole
         */
        protected Map<String, Pair<String, Map<String, StorageNodeHaInfo>>> checkResults;

        public CheckStorageRoleInfoTask(String storageInstId,
                                        StorageInstHaContext haContext,
                                        Map<String, Pair<String, Map<String, StorageNodeHaInfo>>> checkResults) {
            this.storageInstId = storageInstId;
            this.haContext = haContext;
            this.checkResults = checkResults;
        }

        @Override
        public void run() {
            doCheckRoleForStorageNodes();
        }

        protected void doCheckRoleForStorageNodes() {

            String user = haContext.user;
            String encPasswd = haContext.encPasswd;
            int storageType = haContext.storageType;
            int storageKind = haContext.storageKind;
            String vipAddr = haContext.storageVipAddr;

            List<String> addrList = new ArrayList<>();
            addrList.addAll(haContext.storageNodeInfos.keySet());

            Map<String, StorageNodeHaInfo> addrWithRoleMap = null;
            boolean allowFetchRoleOnlyFromLeader = true;
            if (storageKind == StorageInfoRecord.INST_KIND_SLAVE) {
                allowFetchRoleOnlyFromLeader = false;
            }
            final StorageInfoRecord vipInfo = haContext.getNodeInfoByAddress(
                null == vipAddr ? (addrList.isEmpty() ? null : addrList.get(0)) : vipAddr);
            addrWithRoleMap =
                StorageHaChecker
                    .checkAndFetchRole(addrList, vipAddr, null == vipInfo ? -1 : vipInfo.xport, user,
                        PasswdUtil.decrypt(encPasswd), storageType, storageKind, allowFetchRoleOnlyFromLeader);

            boolean isStorageMasterInst = storageKind != StorageInfoRecord.INST_KIND_SLAVE;
            String availableAddr = null;
            for (Map.Entry<String, StorageNodeHaInfo> addrWithRoleItem : addrWithRoleMap.entrySet()) {
                String addr = addrWithRoleItem.getKey();
                StorageNodeHaInfo haInfo = addrWithRoleItem.getValue();
                if (isStorageMasterInst) {
                    if (haInfo.role == StorageRole.LEADER && haInfo.isHealthy) {
                        availableAddr = addr;
                        break;
                    }
                } else {
                    if (haInfo.role == StorageRole.LEARNER && haInfo.isHealthy) {
                        availableAddr = addr;
                        break;
                    }
                }
            }
            checkResults.put(storageInstId, new Pair<>(availableAddr, addrWithRoleMap));
        }
    }

    private List<StorageInstHaContext> getStorageNodesAndMetaDB() {
        List<StorageInstHaContext> nodes = new ArrayList<>(storageHaCtxCache.values());
        nodes.add(metaDbStorageHaCtx);
        return nodes;
    }

    /**
     * Change replicas in a zone to specified role
     */
    public void changeRoleByZone(String zone, StorageRole newRole) {
        Set<String> changed = new HashSet<>();
        for (val storageNode : getStorageNodesAndMetaDB()) {
            for (val replica : storageNode.getReplicaByZone(zone)) {
                if (!changed.contains(replica.getHostPort())) {
                    changeRoleOfStorageNode(storageNode.getStorageInstId(), replica.getHostPort(), newRole);
                    changed.add(replica.getHostPort());
                }
            }
        }
    }

    /**
     * Change replica of datanode to specified role
     *
     * @param address replica address of a datanode
     * @param newRole new role
     */
    public void changeRoleByAddress(String address, StorageRole newRole) {
        Set<String> instances = getStorageNodesAndMetaDB().stream()
            .filter(x -> x.hasReplica(address))
            .filter(x -> !x.getNodeHaInfoByAddress(address).getRole().equals(newRole))
            .map(x -> x.getStorageInstId())
            .collect(Collectors.toSet());
        if (instances.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "could not find replica " + address);
        }
        for (String instance : instances) {
            changeRoleOfStorageNode(instance, address, newRole);
        }

        logger.warn(String.format("change replica(%s) to %s", address, newRole));
    }

    /**
     * Change role of a xpaxos replica
     */
    public void changeRoleOfStorageNode(String instanceId, String replica, StorageRole newRole) {
        assert StringUtils.isNotEmpty(replica);

        StorageInstHaContext storageNode = null;
        if (instanceId.equals(metaDbStorageHaCtx.storageInstId)) {
            storageNode = metaDbStorageHaCtx;
        } else {
            storageNode = storageHaCtxCache.get(instanceId);
        }
        if (storageNode == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("could not find instance %s", instanceId));
        }
        StorageInfoRecord replicaInfo = storageNode.getNodeInfoByAddress(replica);
        StorageNodeHaInfo replicaHaInfo = storageNode.getNodeHaInfoByAddress(replica);
        if (replicaInfo == null) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_GMS_GENERIC,
                String.format("replica %s not exists in datanode(%s)", replica, instanceId));
        }

        String oldLeaderAddr = storageNode.getLeaderNode().getHostPort();
        String user = storageNode.getUser();
        String passwd = PasswdUtil.decrypt(storageNode.getEncPasswd());
        StorageRole currentRole = replicaHaInfo.getRole();
        try {
            if (currentRole.equals(newRole)) {
                logger.warn(String.format("storage node already %s, no need to change", newRole));
                return;
            }

            StorageHaChecker.changePaxosRoleOfNode(oldLeaderAddr, user, passwd, replica, currentRole, newRole);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                String.format("change role of %s@%s from %s to %s failed", replica, instanceId, currentRole, newRole));
        }
    }

    /**
     * Check leader of storage nodes and change leader to primary-zone if necessary.
     */
    private void checkStorageNodeAtPrimaryZone(String primaryZone) {
        if (TStringUtil.isBlank(primaryZone)) {
            return;
        }
        logger.debug("try to change leader of all storage node to zone " + primaryZone);

        Set<String> changed = new HashSet<>();
        for (StorageInstHaContext node : getStorageNodesAndMetaDB()) {
            StorageInfoRecord leader = node.getLeaderNode();
            if (primaryZone.equals(leader.getAzoneId())) {
                continue;
            }
            // Make sure we won't change a same storage node multiple times
            if (changed.contains(leader.getHostPort())) {
                continue;
            }
            List<StorageInfoRecord> candidates = node.getReplicaByZone(primaryZone);
            if (candidates.isEmpty()) {
                continue;
            }
            String newLeaderAddr = candidates.get(0).getHostPort();
            if (!node.getAllStorageNodeHaInfoMap().get(newLeaderAddr).isHealthy) {
                logger.warn(String.format("candidate(%s) at zone %s is not healthy, could not change",
                    newLeaderAddr, primaryZone));
                continue;
            }

            String oldLeaderAddr = leader.getHostPort();
            String user = node.getUser();
            String passwd = PasswdUtil.decrypt(node.getEncPasswd());
            try {
                StorageHaChecker.changeLeaderOfNode(node.storageInstId, oldLeaderAddr, user, passwd, newLeaderAddr);
                changed.add(leader.getHostPort());
                MetaDbLogUtil.META_DB_LOG.info(String.format("change leader to %s for storage inst %s",
                    newLeaderAddr, node.getStorageInstId()));
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                    String.format("failed to change %s to leader", oldLeaderAddr));
            }
        }

        // If any storage changed, the HA Task should be triggered
        if (!changed.isEmpty()) {
            MetaDbLogUtil.META_DB_LOG.info(String.format("leaders of (%s) has been changed, do HA tasks", changed));
            CheckStorageHaTask task = new CheckStorageHaTask(this);
            task.setDoHaAsync(false);
            task.run();
        }
    }

    public void setPrimaryZoneSupplier(Supplier<PrimaryZoneInfo> supplier) {
        this.primaryZoneInfoSupplier = supplier;
    }

    private PrimaryZoneInfo getPrimaryZone() {
        return this.primaryZoneInfoSupplier.get();
    }

    public synchronized void setEnablePrimaryZoneMaintain(boolean enable) {
        this.enablePrimaryZoneMaintain = enable;

        logger.info(String.format("%s primaryZoneMaintain", enable ? "enable" : "disable"));
    }

    /**
     * Change primary zone of the cluster
     * It has two effects:
     * 1. If primary zone is specified, background thread will maintain the primary status
     * 2. If election weight is specified, all storage node's election weight will be changed at once
     */
    public void changePrimaryZone(PrimaryZoneInfo primaryZoneInfo) {
        // check whether all zone is valid
        if (!primaryZoneInfo.isEmpty()) {
            Map<String, Boolean> allZones = new HashMap<>();
            for (PrimaryZoneInfo.ZoneProperty zone : primaryZoneInfo.getZoneList()) {
                allZones.put(zone.zone, false);
            }
            for (StorageInstHaContext storageNode : getStorageNodesAndMetaDB()) {
                for (StorageInfoRecord replica : storageNode.getStorageInfo()) {
                    if (allZones.containsKey(replica.getAzoneId())) {
                        allZones.put(replica.getAzoneId(), true);
                    }
                }
            }

            for (val kv : allZones.entrySet()) {
                if (!kv.getValue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        String.format("zone %s not exists", kv.getKey()));
                }
            }
        }

        // if election weight changed, all storage node need to be changed
        if (this.enablePrimaryZoneMaintain) {
            for (StorageInstHaContext storageNode : getStorageNodesAndMetaDB()) {
                changeElectionWeight(storageNode, primaryZoneInfo);
                logger.info("change primary_zone of storage node " + storageNode.getStorageInstId()
                    + " to " + primaryZoneInfo);
            }
        }

        logger.info("system primary_zone become " + primaryZoneInfo);
    }

    private void changeElectionWeight(StorageInstHaContext storage, PrimaryZoneInfo primaryZoneInfo) {
        for (StorageInfoRecord replica : storage.getStorageInfo()) {
            if (replica.storageType != StorageInfoRecord.STORAGE_TYPE_XCLUSTER &&
                replica.storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                logger.warn("storage is not xcluster, should not set election_weight");
                continue;
            }
            StorageNodeHaInfo ha = storage.getAllStorageNodeHaInfoMap().get(replica.getHostPort());
            if (ha.getRole().equals(StorageRole.LEARNER)) {
                logger.warn(String.format("ignore set election_weight for learner replica %s", replica.getHostPort()));
                continue;
            }

            int weight = PrimaryZoneInfo.DEFAULT_ZONE_WEIGHT;
            PrimaryZoneInfo.ZoneProperty zone = primaryZoneInfo.getZone(replica.getAzoneId());
            if (zone != null) {
                weight = zone.weight;
            }

            String leaderAddress = storage.getLeaderNode().getHostPort();
            String user = storage.user;
            String passwd = PasswdUtil.decrypt(storage.getEncPasswd());
            String replicaAddress = replica.getHostPort();
            try {
                StorageHaChecker.changeElectionWeight(leaderAddress, user, passwd, replicaAddress, weight);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                    String.format("failed to change election_weight of replica %s", replica));
            }
        }
    }

    /**
     * CheckPrimaryZoneTask check the leader is located at specified zone.
     * If not, submit a leader task to make sure the
     */
    protected class CheckPrimaryZoneTask implements Runnable {

        public CheckPrimaryZoneTask() {
        }

        @Override
        public void run() {
            if (!enablePrimaryZoneMaintain) {
                return;
            }
            if (!LeaderStatusBridge.getInstance().hasLeaderShip()) {
                return;
            }
            PrimaryZoneInfo primaryZone = getInstance().getPrimaryZone();
            if (primaryZone.hasLeader()) {
                checkStorageNodeAtPrimaryZone(primaryZone.getFirstZone());
            }
        }
    }

    protected static void logHaTask(long timeCost,
                                    Map<String, Pair<String, Map<String, StorageNodeHaInfo>>> allDnRoleInfos,
                                    Map<String, Pair<Boolean, String>> shouldHaFlags,
                                    Throwable haCheckEx) {

        if (!StorageHaChecker.enablePrintHaCheckTaskLog) {
            return;
        }
        String haStorageInstInfo = "";
        boolean logWarning = false;
        for (Map.Entry<String, Pair<String, Map<String, StorageNodeHaInfo>>> oneDnRoleInfo : allDnRoleInfos
            .entrySet()) {
            if (StringUtils.isEmpty(haStorageInstInfo)) {
                haStorageInstInfo += ",";
            }
            String storageInstId = oneDnRoleInfo.getKey();

            Pair<Boolean, String> checkHaRs = shouldHaFlags.get(storageInstId);
            Boolean shouldHa = checkHaRs.getKey();
            String oldLeader = checkHaRs.getValue();

            Pair<String, Map<String, StorageNodeHaInfo>> roleInfosPair = oneDnRoleInfo.getValue();
            String newLeader = roleInfosPair.getKey();
            if (shouldHa) {
                logWarning = false;
                String allRolInfos = buildStorageNodeHaInfoLogMsg(roleInfosPair.getValue());
                haStorageInstInfo +=
                    String.format("{dnId=%s,shouldHa=%s,newLeader=%s,oldLeader=%s,roleInfos=[%s]}", storageInstId,
                        shouldHa, newLeader, oldLeader, allRolInfos);
            } else {
                haStorageInstInfo +=
                    String.format("{dnId=%s,shouldHa=%s,leader=%s}", storageInstId, shouldHa, newLeader);
            }

        }

        String logMsg = String
            .format("HaCheckTimeCost(ms): [%s], isSucc:[%s], allDnRoleInfo: [%s]", timeCost / (1000000),
                haCheckEx == null, haStorageInstInfo);
        if (logWarning) {
            CHECK_HA_LOGGER.warn(logMsg);
        } else {
            CHECK_HA_LOGGER.info(logMsg);
        }
    }

    /**
     * 1. If the actual leader of a StorageInstance(get from xpaxos) is different from primary_zone configuration,
     * change the actual leader
     * 2. If the actual leader of a StorageInstance(get from xpaxos) is different from in-memory setting,
     * change in-memory setting and any related datasources.
     */
    protected static class CheckStorageHaTask implements Runnable {

        // do HA async or sync ?
        private boolean doHaAsync = true;
        protected StorageHaManager storageHaManager;

        public CheckStorageHaTask(StorageHaManager storageHaManager) {
            this.storageHaManager = storageHaManager;
        }

        @Override
        public void run() {
            runInner();
        }

        public void setDoHaAsync(boolean async) {
            this.doHaAsync = async;
        }

        protected void runInner() {

            // check and submit task for all storage insts
            doCheckAndSubmitTaskIfNeed(this.storageHaManager.storageHaCtxCache);
        }

        protected void doCheckAndSubmitTaskIfNeed(Map<String, StorageInstHaContext> storageInstIdAndHaContextMap) {

            long beginTs = System.nanoTime();
            long endTs = -1;
            Map<String, Pair<String, Map<String, StorageNodeHaInfo>>> storageInstNewRoleInfoMap =
                new ConcurrentHashMap<>();
            Map<String, Pair<Boolean, String>> shouldHaFlags = new HashMap<>();
            Throwable haCheckEx = null;
            try {

                // Check role for all storage node of curr inst (can optimise to use multi-thread to speed up )
                Map<String, StorageInstHaContext> storageHaCache = storageInstIdAndHaContextMap;
                CountDownLatch countDownLatch = new CountDownLatch(storageHaCache.size());
                for (Map.Entry<String, StorageInstHaContext> storageInstItem : storageHaCache.entrySet()) {
                    CheckStorageRoleInfoTask checkRoleInfoTask =
                        new CheckStorageRoleInfoTask(storageInstItem.getKey(), storageInstItem.getValue(),
                            storageInstNewRoleInfoMap);
                    this.storageHaManager.storageHaManagerTaskExecutor.submit(() -> {
                        try {
                            checkRoleInfoTask.run();
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
                }

                // wait for all tasks
                ExecutorUtil.awaitCountDownLatch(countDownLatch);

                // Submit HA switch ds tasks
                for (String storageInstIdVal : storageInstNewRoleInfoMap.keySet()) {
                    String newAvailableAddr = storageInstNewRoleInfoMap.get(storageInstIdVal).getKey();
                    Map<String, StorageNodeHaInfo> addrWithRoleMap =
                        storageInstNewRoleInfoMap.get(storageInstIdVal).getValue();
                    StorageInstHaContext haCache = storageHaCache.get(storageInstIdVal);
                    if (StringUtils.isEmpty(newAvailableAddr)) {
                        haCache.isCurrAvailableNodeAddrHealthy = false;
                    } else {
                        StorageNodeHaInfo healthyVal = addrWithRoleMap.get(newAvailableAddr);
                        haCache.isCurrAvailableNodeAddrHealthy = healthyVal != null && healthyVal.isHealthy;
                    }

                    boolean isMasterStorageInst = haCache.getStorageKind() != StorageInfoRecord.INST_KIND_SLAVE;
                    boolean shouldHa = false;
                    if (checkIfAvailableAddrChanged(newAvailableAddr, haCache.currAvailableNodeAddr)) {
                        if (haCache.haStatus == StorageInstHaContext.StorageHaStatus.NORMAL) {
                            shouldHa = true;
                        }
                    }

                    if (!shouldHa && ConfigDataMode.enableSlaveReadForPolarDbX()
                        && haCache.storageKind == StorageInfoRecord.INST_KIND_MASTER) {
                        Set<String> newFollows = new HashSet<>();
                        Set<String> oldFollows = new HashSet<>();

                        if (addrWithRoleMap != null) {
                            for (Map.Entry<String, StorageNodeHaInfo> entry : addrWithRoleMap.entrySet()) {
                                if (entry.getValue().getRole() == StorageRole.FOLLOWER) {
                                    newFollows.add(entry.getKey());
                                }
                            }
                        }

                        Map<String, StorageNodeHaInfo> oldHaInfo = haCache.getAllStorageNodeHaInfoMap();
                        if (oldHaInfo != null) {
                            for (Map.Entry<String, StorageNodeHaInfo> entry : oldHaInfo.entrySet()) {
                                if (entry.getValue().getRole() == StorageRole.FOLLOWER) {
                                    oldFollows.add(entry.getKey());
                                }
                            }
                        }
                        if (!newFollows.equals(oldFollows)) {
                            shouldHa = true;
                        }
                    }

                    // Refresh all the HaInfos for all storageNode in memory
                    haCache.allStorageNodeHaInfoMap = addrWithRoleMap;

                    /**
                     * For all storageMasterInst, check if exists multi-leaders
                     */
                    if (shouldHa && isMasterStorageInst) {
                        /**
                         * Check if the roles of storage contain multi-leaders
                         */
                        int leaderCnt = 0;
                        for (Map.Entry<String, StorageNodeHaInfo> addrRoleItem : addrWithRoleMap.entrySet()) {
                            if (addrRoleItem.getValue().getRole() == StorageRole.LEADER) {
                                leaderCnt++;
                            }
                        }
                        if (leaderCnt > 1) {
                            /**
                             *  Find multi leader in roleMap, so current storage inst is not healthy,
                             *  reject to submit ha task.
                             */
                            shouldHa = false;
                            MetaDbLogUtil.META_DB_LOG.warn(String.format(
                                "Reject to do haTask for storage[%s] because of finding multi leaders, leader count is [%s]",
                                storageInstIdVal, leaderCnt));
                        }
                    }

                    // shouldHaFlags is used for doing HA logs
                    shouldHaFlags.put(storageInstIdVal, new Pair<>(shouldHa,
                        haCache.currAvailableNodeAddr == null ? "noAvailableAddr" : haCache.currAvailableNodeAddr));
                    if (shouldHa) {
                        submitHaSwitchTask(newAvailableAddr, addrWithRoleMap, haCache);
                    }
                }
                endTs = System.nanoTime();
            } catch (Throwable ex) {
                haCheckEx = ex;
                MetaDbLogUtil.META_DB_LOG.error(ex);
            } finally {
                StorageHaManager.logHaTask(endTs - beginTs, storageInstNewRoleInfoMap, shouldHaFlags, haCheckEx);
            }
        }

        private void submitHaSwitchTask(String newAvailableAddr, Map<String, StorageNodeHaInfo> addrWithRoleMap,
                                        StorageInstHaContext haCache) {
            StorageHaSwitchTask storageHaSwitchTask = null;
            synchronized (haCache) {
                if (haCache.haStatus == StorageInstHaContext.StorageHaStatus.NORMAL) {
                    haCache.haStatus = StorageInstHaContext.StorageHaStatus.SWITCHING;
                    storageHaSwitchTask =
                        new StorageHaSwitchTask(haCache, storageHaManager, newAvailableAddr,
                            addrWithRoleMap);
                }
            }
            if (doHaAsync) {
                this.storageHaManager.storageHaManagerTaskExecutor.submit(storageHaSwitchTask);
            } else {
                storageHaSwitchTask.run();
            }
        }

        protected boolean checkIfAvailableAddrChanged(String availableAddr, String currAvailableNodeAddr) {
            if (availableAddr != null && !availableAddr.equalsIgnoreCase(currAvailableNodeAddr)) {
                return true;
            } else {
                return false;
            }
        }
    }

    protected static class RefreshMasterStorageInfosTask implements Runnable {

        protected StorageHaManager manager;

        public RefreshMasterStorageInfosTask(StorageHaManager manager) {
            this.manager = manager;
        }

        @Override
        public void run() {
            try {
                doRefreshMasterStorageInfos();
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                logger.error("Failed to refresh storage infos of MetaDB", ex);
            }

        }

        protected void doRefreshMasterStorageInfos() {

            /**
             * Get all master storage inst id set from Memory.
             */
            List<String> masterStorageInstIdSet = new ArrayList<>();
            masterStorageInstIdSet.addAll(manager.storageHaCtxCache.keySet());

            /**
             * For each master storage inst id of x-cluster type,
             * use StorageHaChecker.checkAndFetchRole to fetch all roles for all hosts
             */
            // key : storageInstId, val: the rs of StorageHaChecker.checkAndFetchRole
            Map<String, Map<String, StorageNodeHaInfo>> storageHaCheckRsMap = new HashMap<>();
            for (int i = 0; i < masterStorageInstIdSet.size(); i++) {
                String storageInstId = masterStorageInstIdSet.get(i);
                StorageInstHaContext haContext = manager.storageHaCtxCache.get(storageInstId);
                if (haContext.storageType != StorageInfoRecord.STORAGE_TYPE_XCLUSTER &&
                    haContext.storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER) {
                    continue;
                }
                if (haContext.storageKind == StorageInfoRecord.INST_KIND_SLAVE) {
                    // ignore slave storage
                    continue;
                }

                List<String> addrList = new ArrayList<>();
                addrList.addAll(haContext.storageNodeInfos.keySet());

                String user = haContext.user;
                String passwd = PasswdUtil.decrypt(haContext.encPasswd);
                int stoageInstKind = haContext.storageKind;
                final StorageInfoRecord vipInfo = haContext.getNodeInfoByAddress(
                    null == haContext.storageVipAddr ? (addrList.isEmpty() ? null : addrList.get(0)) :
                        haContext.storageVipAddr);
                Map<String, StorageNodeHaInfo> hostHaInfoMap = StorageHaChecker
                    .checkAndFetchRole(addrList, haContext.storageVipAddr, null == vipInfo ? -1 : vipInfo.xport, user,
                        passwd, haContext.storageType, stoageInstKind, true);
                storageHaCheckRsMap.putIfAbsent(storageInstId, hostHaInfoMap);

            }
            if (storageHaCheckRsMap.isEmpty()) {
                return;
            }

            boolean needUpgradeOpVersion = false;
            String masterInstId = ServerInstIdManager.getInstance().getMasterInstId();
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

                metaDbConn.setAutoCommit(false);

                /**
                 * batch fetch all StorageNodeHaInfo and group by storage_inst_id
                 */
                List<String> storageInstIdSet = new ArrayList<>();
                storageInstIdSet.addAll(storageHaCheckRsMap.keySet());
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(metaDbConn);
                Map<String, List<StorageInfoRecord>> storageNodeRecMaps =
                    storageInfoAccessor.getStorageInfosByStorageInstIdList(storageInstIdSet);

                for (Map.Entry<String, Map<String, StorageNodeHaInfo>> storageHaCheckRsItem : storageHaCheckRsMap
                    .entrySet()) {

                    String storageInstId = storageHaCheckRsItem.getKey();
                    Map<String, StorageNodeHaInfo> haCheckRsMap = storageHaCheckRsItem.getValue();

                    /**
                     * Fetch the storageInfo record list of one storage_inst_id
                     */
                    List<StorageInfoRecord> storageInfoRecordList = storageNodeRecMaps.get(storageInstId);

                    Set<String> addrSetFromMetaDb = new HashSet<>();
                    for (int i = 0; i < storageInfoRecordList.size(); i++) {
                        StorageInfoRecord storageInfoRecord = storageInfoRecordList.get(i);
                        String addr = AddressUtils.getAddrStrByIpPort(storageInfoRecord.ip, storageInfoRecord.port);
                        addrSetFromMetaDb.add(addr);
                    }

                    boolean containLeaderInfo = false;
                    StorageNodeHaInfo leaderInfo = null;

                    /**
                     * Check if storage_info of MetaDB miss some hosts that are the new leader,
                     * if true, add new host into MetaDB for each master storage inst id by trans
                     */
                    for (Map.Entry<String, StorageNodeHaInfo> haCheckRsItem : haCheckRsMap.entrySet()) {
                        String addrChecked = haCheckRsItem.getKey();
                        StorageNodeHaInfo haInfo = haCheckRsItem.getValue();

                        if (haInfo.getRole() == StorageRole.LEADER) {
                            containLeaderInfo = true;
                            leaderInfo = haInfo;
                        }

                        if (haInfo.getRole() == StorageRole.LEARNER) {
                            // The storage nodes of storage master inst
                            // should NOT include the storage nodes of Learner,
                            // so ignore them.
                            continue;
                        }

                        if (addrSetFromMetaDb.contains(addrChecked)) {
                            continue;
                        }

                        StorageInfoRecord storageInfoRecord =
                            buildNewStorageInfoRecord(haInfo, storageInfoRecordList.get(0));
                        storageInfoAccessor.addStorageInfo(storageInfoRecord);
                        needUpgradeOpVersion = true;
                    }

                    /**
                     * If HaCheck Result is empty, this must be a invalid result,
                     * So ignore doing clean unused storageInfos
                     */
                    if (haCheckRsMap.isEmpty()) {
                        continue;
                    } else {
                        /**
                         * Check If HaCheck Result has contain leader info, if has no leader info ,
                         * , this may be a invalid result, so ignore doing clean unused storageInfos
                         */
                        if (!containLeaderInfo) {
                            continue;
                        }
                    }

                    /**
                     * Check if storage_info of MetaDB leave behind some hosts that have been offline,
                     * if true, remove unused host from metaDB for each master storage inst id by trans
                     */
                    for (int i = 0; i < storageInfoRecordList.size(); i++) {
                        StorageInfoRecord storageInfoRecord = storageInfoRecordList.get(i);
                        String addr = AddressUtils.getAddrStrByIpPort(storageInfoRecord.ip, storageInfoRecord.port);
                        if (!haCheckRsMap.containsKey(addr)) {
                            if (addr.equalsIgnoreCase(leaderInfo.addr)) {
                                /**
                                 * Make sure that cannot remove leader infos
                                 */
                                continue;
                            }

                            if (storageInfoRecord.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                                /**
                                 * Make sure that cannot remove vip info of storage
                                 */
                                continue;
                            }

                            storageInfoAccessor.removeStorageInfo(storageInfoRecord.storageInstId, storageInfoRecord.ip,
                                storageInfoRecord.port);
                            needUpgradeOpVersion = true;
                        }
                    }
                }

                if (needUpgradeOpVersion) {
                    MetaDbConfigManager.getInstance()
                        .notify(MetaDbDataIdBuilder.getStorageInfoDataId(masterInstId), metaDbConn);
                }
                metaDbConn.commit();

            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
            if (needUpgradeOpVersion) {
                MetaDbConfigManager.getInstance()
                    .sync(MetaDbDataIdBuilder.getStorageInfoDataId(masterInstId));
            }
        }

        protected StorageInfoRecord buildNewStorageInfoRecord(StorageNodeHaInfo haInfo, StorageInfoRecord oldRecord) {
            StorageInfoRecord newRecord = oldRecord.copy();
            Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(haInfo.addr);
            newRecord.ip = nodeIpPort.getKey();
            newRecord.port = nodeIpPort.getValue();
            newRecord.xport = haInfo.getXPort();
            newRecord.isVip = StorageInfoRecord.IS_VIP_FALSE;
            return newRecord;
        }
    }

    protected static ConnPoolConfig getConnPoolConfigFromManager() {
        return ConnPoolConfigManager.getInstance().getConnPoolConfig();
    }

    /**
     * @return the storage instance's cpu cores.
     */
    public int getStorageCpuCore() {
        GmsNodeManager.GmsNode gmsnode = GmsNodeManager.getInstance().getLocalNode();
        if (gmsnode != null && storageHaCtxCache != null && !storageHaCtxCache.isEmpty()) {
            for (StorageInstHaContext context : storageHaCtxCache.values()) {
                if (context.instId.equals(gmsnode.instId)) {
                    return context.storageNodeInfos.get(context.currAvailableNodeAddr).cpuCore;
                }
            }
        }
        throw new UnsupportedOperationException("getStorageCpuCore");
    }

    private static String buildStorageNodeHaInfoLogMsg(Map<String, StorageNodeHaInfo> nodeHaInfoMap) {

        String allRoleInfoStr = "";
        for (StorageNodeHaInfo nodeInfo : nodeHaInfoMap.values()) {
            if (!allRoleInfoStr.isEmpty()) {
                allRoleInfoStr += ",";
            }
            String nodeInfoStr = String
                .format("[%s/%s/%s/%s]", nodeInfo.getAddr(), nodeInfo.getRole(), nodeInfo.isHealthy(),
                    nodeInfo.getXPort());
            allRoleInfoStr += nodeInfoStr;
        }

        return allRoleInfoStr;
    }
}
