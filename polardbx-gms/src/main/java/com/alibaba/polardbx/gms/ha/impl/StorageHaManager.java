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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
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
import com.alibaba.polardbx.rpc.perf.SwitchoverPerfCollection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.google.common.collect.Sets;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manager for handling HA of storage
 *
 * @author chenghui.lch
 */
public class StorageHaManager extends AbstractLifecycle {

    /**
     * {@link UNAVAILABLE_ACCESS_FOR_LEARNER} only used for  removing the error connection pools which is from RO-DN in time.
     * Once the RO-DN crash, CN probes the error connection which come from this DN, thus CN will reset the DN address
     * by the {@link UNAVAILABLE_ACCESS_FOR_LEARNER}. Afterwards, the HA will be triggered, and the error connection pools
     * will be removed.
     * <p>
     * If the RO-DN recover, CN will use available address which is different with the {@link UNAVAILABLE_ACCESS_FOR_LEARNER}.
     * Afterwards, the HA will be triggered, and the available connection pools will be added.
     * <p>
     * Only used for auto removing the conn pools of all ro-dn list on polarx-master-inst
     * by submitting StorageHaSwitchTask
     * <pre>
     *     see OptimizedGroupConfigManager.resetByPolarDBXDataSourceWrapper(java.util.List)
     * </pre>
     */
    public static final String UNAVAILABLE_URL_FOR_LEARNER = "unavailable_access_for_learner";

    /**
     * Only used for auto removing the conn pools of all ro-dn list on polarx-master-inst
     * by submitting StorageHaSwitchTask
     */
    public static final String UNAVAILABLE_ACCESS_FOR_LEARNER = UNAVAILABLE_URL_FOR_LEARNER + ":3306";

    public static final String AREA_TYPE_HA_SWITCHER = "HaSwitcher";
    public static final String AREA_TYPE_HA_CHECKER = "HaChecker";
    public static final String AREA_TYPE_META_DB = "MetaDB";

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
    protected volatile int checkStorageTaskPeriod = DynamicConfig.storageHaTaskPeriodDefault; // 2s

    protected int refreshStorageInfoOfMetaDbTaskIntervalDelay = checkStorageTaskPeriod * 3; // 15s
    protected int refreshStorageInfoOfMetaDbTaskInterval = checkStorageTaskPeriod * 12; // 60s

    /**
     * The executor for task of CheckStorageHaTask,
     * a Scheduler for Scheduling CheckStorageHaTask by the interval of 5 second
     */
    protected ScheduledExecutorService checkStorageHaTaskExecutor = null;

    /**
     * The executor for task of RefreshMasterStorageInfosTask
     * which is used for refresh the latest node infos of xdb on system table storage_info of metadb,
     * a Scheduler for Scheduling RefreshMasterStorageInfosTask by the interval of 15 second
     */
    protected ScheduledExecutorService refreshStorageInfoOfMetaDbExecutor = null;

    /**
     * The executor for task of StorageHaSwitchTask,
     * each StorageHaSwitchTask handles a dn to do ha.
     * default pool size is 8
     */
    protected ThreadPoolExecutor storageHaManagerTaskExecutor = null;
    protected int storageHaManagerExecutorPoolSize = 8;
    protected int storageHaManagerExecutorQueueSize = 40960;

    /**
     * The executor for subtask GroupHaSwitchTasks from StorageHaSwitchTask,
     * each GroupHaSwitchTasks handles one or more groups to do ha
     * default pool size is 16
     */
    protected ThreadPoolExecutor groupHaTaskExecutor = null;
    protected int groupHaTaskExecutorPoolSize = 16;
    protected int groupHaTaskExecutorQueueSize = 40960;

    /**
     * The executor for subtask CheckStorageRoleInfoTask from CheckStorageHaTask,
     * one CheckStorageRoleInfoTask handles the fetching node role for one dn
     * default pool size is 16
     */
    protected ThreadPoolExecutor checkDnRoleTaskExecutor = null;
    protected int checkDnRoleTaskExecutorPoolSize = 16;
    protected int checkDnRoleTaskExecutorQueueSize = 40960;

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

    private Set<String> listenerReadOnlyInstIdSet = new HashSet<>();

    private boolean allowFollowRead;

    public interface StorageInfoConfigSubListener extends ConfigListener {
        String getSubListenerName();
    }

    protected Map<String, StorageInfoConfigSubListener> storageInfoSubListenerMap = new ConcurrentHashMap<>();

    protected static class StorageInfoConfigListener implements ConfigListener {

        public StorageInfoConfigListener() {
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            String instId = InstIdUtil.getInstIdFromStorageInfoDataId(dataId);
            ServerInstIdManager.getInstance().loadAllInstIdAndStorageIdSet();
            ServerInstIdManager.getInstance().loadAllHtapInstIds();
            StorageHaManager.getInstance().updateStorageInstHaContext(instId);
            if (ConfigDataMode.isMasterMode()) {
                StorageHaManager.getInstance().updateGroupConfigVersion();
            }

            try {
                Map<String, StorageInfoConfigSubListener> storageInfoSubListenerMap =
                    StorageHaManager.getInstance().getStorageInfoSubListenerMap();
                for (Map.Entry<String, StorageInfoConfigSubListener> subListenerItem : storageInfoSubListenerMap.entrySet()) {
                    String subListenerName = subListenerItem.getKey();
                    StorageInfoConfigSubListener subListenerVal = subListenerItem.getValue();
                    try {
                        subListenerVal.onHandleConfig(dataId, newOpVersion);
                    } catch (Throwable ex) {
                        logger.warn(String.format("Failed to handle subListener[%s], err is %s", subListenerName,
                            ex.getMessage()), ex);
                    }
                }
            } catch (Throwable ex) {
                logger.warn(ex);
            }
        }
    }

    protected Map<String, StorageInfoConfigSubListener> getStorageInfoSubListenerMap() {
        return storageInfoSubListenerMap;
    }

    /**
     * Register a sub-listener
     * <pre>
     * The list of sub-listeners which is called by StorageInfoConfigListener,
     * sub-listeners is used for those non-gms module to handle the change of rw-dn-list.
     * the sub-listener should just to refresh some in-memory meta data.
     * </pre>
     */
    public void registerStorageInfoSubListener(StorageInfoConfigSubListener subListener) {
        if (subListener == null) {
            return;
        }
        String subListenerName = subListener.getSubListenerName();
        storageInfoSubListenerMap.putIfAbsent(subListenerName, subListener);
    }

    /**
     * Unegister a sub-listener
     */
    public void unregisterStorageInfoSubListener(String subListenerName) {
        if (StringUtils.isEmpty(subListenerName)) {
            return;
        }
        storageInfoSubListenerMap.remove(subListenerName);
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
            ExecutorUtil.createBufferedExecutor("StorageHaManagerTaskExecutor", storageHaManagerExecutorPoolSize,
                storageHaManagerExecutorQueueSize);
        checkStorageHaTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("CheckStorageHaTaskExecutor", true));

        checkStorageHaTaskExecutor = initStorageHaCheckTaskExecutor(checkStorageHaTask);

        groupHaTaskExecutor =
            ExecutorUtil.createBufferedExecutor("GroupHaTaskExecutor", groupHaTaskExecutorPoolSize,
                groupHaTaskExecutorQueueSize);

        checkDnRoleTaskExecutor =
            ExecutorUtil.createBufferedExecutor("CheckDnRoleTaskExecutor", checkDnRoleTaskExecutorPoolSize,
                checkDnRoleTaskExecutorQueueSize);

        if (ConfigDataMode.isMasterMode()) {
            registerStorageInfoConfigListener(InstIdUtil.getInstId());
            registerLearnerStorageInstId();
        } else {
            if (ConfigDataMode.needDNResource()) {
                registerStorageInfoConfigListener(InstIdUtil.getInstId());
                String serverMasterInstId = ServerInstIdManager.getInstance().getMasterInstId();
                registerStorageInfoConfigListener(serverMasterInstId);
            }
        }

        if (ConfigDataMode.isMasterMode()) {
            refreshStorageInfoOfMetaDbExecutor =
                Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("RefreshStorageInfoOfMetaDbTaskExecutor", true));
            refreshStorageInfoOfMetaDbExecutor
                .scheduleAtFixedRate(refreshMasterStorageInfosTask, refreshStorageInfoOfMetaDbTaskIntervalDelay,
                    refreshStorageInfoOfMetaDbTaskInterval, TimeUnit.MILLISECONDS);
        }

    }

    private void registerStorageInfoConfigListener(String instId) {
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getStorageInfoDataId(instId), null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getStorageInfoDataId(instId),
            new StorageInfoConfigListener());
    }

    public synchronized void adjustStorageHaTaskPeriod(int newPeriod) {
        MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(String
            .format("Successful to adjust the HA task period, old period/new period is %s/%s",
                this.checkStorageTaskPeriod, newPeriod));
        this.checkStorageTaskPeriod = newPeriod;
    }

    protected ScheduledExecutorService initStorageHaCheckTaskExecutor(Runnable checkStorageHaTask) {
        ScheduledExecutorService checkStorageHaTaskExecutor =
            Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("CheckStorageHaTaskExecutor", true));
        checkStorageHaTaskExecutor.scheduleAtFixedRate(
            checkStorageHaTask,
            100,
            100,
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
            if (ConfigDataMode.isRowSlaveMode()) {
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

    public static int getAndCheckXport(String addr, boolean isVip, StorageInstHaContext haCtx,
                                       StorageNodeHaInfo haInfo) {

        if (UNAVAILABLE_ACCESS_FOR_LEARNER.equalsIgnoreCase(addr)) {
            MetaDbLogUtil.META_DB_LOG.info("this is unavailable_access_for_learner, so the Xport not available.");
            return -1;
        }
        if (isVip) {
            if (XConfig.VIP_WITH_X_PROTOCOL) {
                Pair<String, Integer> nodeIpPort = AddressUtils.getIpPortPairByAddrStr(addr);
                MetaDbLogUtil.META_DB_LOG.info("Xport is vip: " + addr);
                return nodeIpPort.getValue();
            } else if (XConfig.GALAXY_X_PROTOCOL) {
                final int expected = haInfo.getXPort();
                MetaDbLogUtil.META_DB_LOG.info("Got xport of galaxy vip node " + addr + " is " + expected);
                return expected;
            } else if (XConfig.OPEN_XRPC_PROTOCOL) {
                final int expected = haInfo.getXPort();
                MetaDbLogUtil.META_DB_LOG.info("Got xport of xrpc vip node " + addr + " is " + expected);
                return expected;
            }
        } else if (haInfo != null) {
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

    /**
     * Compute the xport to be used for one healthy leader/learner node
     */
    public static int getAndCheckXportDryRun(String addr, boolean isVip, StorageInstHaContext haCtx,
                                             StorageNodeHaInfo haInfo) {

        int tmpXport = -1;
        try {
            ConnPoolConfig poolConfig = StorageHaManager.getConnPoolConfigFromManager();
            tmpXport = getAndCheckXport(addr, isVip, haCtx, haInfo);
            // Note this is Xproto for **STORAGE** node. First check global setting then use the metaDB inst_config.
            if (XConnectionManager.getInstance().getStorageDbPort() != 0) {
                tmpXport =
                    XConnectionManager.getInstance().getStorageDbPort(); // Disabled or force set by server.properties.
            } else if (poolConfig.xprotoStorageDbPort != null) {
                if (poolConfig.xprotoStorageDbPort != 0) {
                    tmpXport = poolConfig.xprotoStorageDbPort; // Disabled or force set by inst_config.
                } // else auto set by HA.
            } else {
                // Bad config? Disable it.
                tmpXport = -1;
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.warn(ex);
        }
        return tmpXport;
    }

    public HaSwitchParams getStorageHaSwitchParamsWithReadLock(String storageInstId, boolean autoUnlock) {
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
            params.autoUnlock = autoUnlock;
        } finally {
            if (autoUnlock) {
                storageInstHaContext.getHaLock().readLock().unlock();
            } else {
                params.haLock = storageInstHaContext.getHaLock();
            }
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
        haSwitchParams.xport =
            getAndCheckXport(storageInstHaContext.currAvailableNodeAddr, storageInstHaContext.currIsVip,
                storageInstHaContext,
                storageInstHaContext.allStorageNodeHaInfoMap.get(storageInstHaContext.currAvailableNodeAddr));
        haSwitchParams.phyDbName = null;
        haSwitchParams.storageKind = storageInstHaContext.storageKind;
        haSwitchParams.instId = storageInstHaContext.instId;

        return haSwitchParams;
    }

    public void getStorageHaSwitchParamsForInitGroupDs(Set<String> instIds,
                                                       String dbName,
                                                       String groupName,
                                                       List<HaSwitchParams> outputHaSwitchParamsWithReadLock) {

        List<GroupDetailInfoRecord> groupDetailInfoRecords = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            groupDetailInfoRecords =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instIds, dbName, groupName);
            if (groupDetailInfoRecords == null || groupDetailInfoRecords.size() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("No find any group detail for [%s/%s/%s]", instIds, dbName, groupName));
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        if (outputHaSwitchParamsWithReadLock == null) {
            return;
        }

        String phyDbName = null;
        try {
            phyDbName = DbTopologyManager.getPhysicalDbNameByGroupKeyFromMetaDb(dbName, groupName);
        } catch (Throwable ex) {
            throw ex;
        }

        for (GroupDetailInfoRecord groupDetailInfoRecord : groupDetailInfoRecords) {
            try {
                String storageInstId = null;
                storageInstId = groupDetailInfoRecord.storageInstId;
                if (SystemDbHelper.DEFAULT_DB_NAME.equals(dbName)) {
                    storageInstId = metaDbStorageHaCtx.getStorageInstId();
                }
                HaSwitchParams haSwitchParam = getStorageHaSwitchParamsWithReadLock(storageInstId, false);
                if (haSwitchParam == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("No find any storage ha info for [%s/%s/%s/%s]", groupDetailInfoRecord.instId,
                            dbName, groupName,
                            storageInstId));
                }
                haSwitchParam.phyDbName = phyDbName;
                outputHaSwitchParamsWithReadLock.add(haSwitchParam);
            } catch (Throwable ex) {
                if (InstIdUtil.getInstId() == groupDetailInfoRecord.instId) {
                    throw ex;
                } else {
                    //ignore the haSwitchParam if the instId is not InstIdUtil.getInstId().
                    logger.warn(String.format(
                        "No find any other storage ha info for [%s/%s/%s]", groupDetailInfoRecord.instId, dbName,
                        groupName), ex);
                }
            }
        }
        return;
    }

    public synchronized void registerLearnerStorageInstId() {
        Set<String> allReadOnlyInstIdSet = ServerInstIdManager.getInstance().getAllReadOnlyInstIdSet();

        Set<String> newReadOnlyInstIdSet = Sets.difference(allReadOnlyInstIdSet, listenerReadOnlyInstIdSet);

        Set<String> removeReadOnlyInstIdSet = Sets.difference(listenerReadOnlyInstIdSet, allReadOnlyInstIdSet);

        for (String instId : newReadOnlyInstIdSet) {
            //The master instId only listener the learner's dataId which maybe not write in time.
            // Here the init version is -1, thus the listener can be invoke once the learner instId write the dataId.
            MetaDbConfigManager.getInstance().bindListener(
                MetaDbDataIdBuilder.getStorageInfoDataId(instId), -1, new StorageInfoConfigListener());
        }

        //make sure clean up the StorageInstHaContext firstly, because the dataId maybe clean by learner.
        for (String instId : removeReadOnlyInstIdSet) {
            StorageHaManager.getInstance().updateStorageInstHaContext(instId);
        }

        //make sure clean up the useless bind.
        for (String instId : removeReadOnlyInstIdSet) {
            MetaDbConfigManager.getInstance().unbindListener(MetaDbDataIdBuilder.getStorageInfoDataId(instId));
        }

        logger.warn(String
            .format("register the learner info for [%s], add the [%s], remove the [%s]", allReadOnlyInstIdSet,
                newReadOnlyInstIdSet, removeReadOnlyInstIdSet));
        this.listenerReadOnlyInstIdSet.clear();
        this.listenerReadOnlyInstIdSet.addAll(allReadOnlyInstIdSet);
    }

    public synchronized void updateGroupConfigVersion() {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            metaDbConn.setAutoCommit(true);

            String instId = InstIdUtil.getInstId();
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);

            // Find all group detail info of master inst
            List<GroupDetailInfoRecord> groupDetails =
                groupDetailInfoAccessor.getGroupDetailInfoByInstId(instId);

            for (GroupDetailInfoRecord groupDetailInfoRecord : groupDetails) {
                //ignore the buildInDB which needn't the separation of reading and writing!
                if (SystemDbHelper.isDBBuildIn(groupDetailInfoRecord.getDbName())) {
                    continue;
                }
                String dataId = MetaDbDataIdBuilder.getGroupConfigDataId(
                    instId, groupDetailInfoRecord.getDbName(), groupDetailInfoRecord.groupName);
                try {
                    boolean ret = MetaDbConfigManager.getInstance().localSync(dataId);
                    if (!ret) {
                        logger.warn(String.format("update the group version for [%s] failed!", dataId));
                    }
                } catch (Throwable t) {
                    //ignore
                }
            }
        } catch (Throwable t) {
            MetaDbLogUtil.META_DB_LOG.error(t);
            throw GeneralUtil.nestedException(t);
        }
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
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getAliveStorageInfosByInstId(instId);

            // Group the storage node by storageInstId
            // key: storageInstId, val: the node list of storage inst
            newStorageInstNodeInfoMap = groupStorageRecInfosByStorageInstId(storageInfoRecords);

            // When curr inst is a slave inst, it need listen the all storage info change both from
            // server master inst and server salve inst.
            // Because the input params "instId" may be a masterInstId or a slaveInstId,
            // here need just fetch all storageInstId from the input param "instId".
            Map<String, StorageInstHaContext> storageInstHaCtxCacheOfInstId =
                new HashMap<String, StorageInstHaContext>();
            Set<String> allStorageInstIdInCache = this.storageHaCtxCache.keySet();
            for (String storageInstIdInCache : allStorageInstIdInCache) {
                StorageInstHaContext haContext = this.storageHaCtxCache.get(storageInstIdInCache);
                if (haContext.instId.equalsIgnoreCase(instId)) {
                    storageInstHaCtxCacheOfInstId.put(haContext.storageInstId, haContext);
                }
            }

            // Find storageInst list to be added
            for (String storageInstIdVal : newStorageInstNodeInfoMap.keySet()) {
                if (!storageInstHaCtxCacheOfInstId.containsKey(storageInstIdVal)) {
                    storageInstIdListToBeAdded.add(storageInstIdVal);
                }
            }

            // Find storageInst list to be deleted
            for (String storageInstIdVal : storageInstHaCtxCacheOfInstId.keySet()) {
                if (!newStorageInstNodeInfoMap.containsKey(storageInstIdVal)) {
                    storageInstIdListToBeDeleted.add(storageInstIdVal);
                }
            }

            // Find storageInst list To be updated
            for (String storageInstIdVal : storageInstHaCtxCacheOfInstId.keySet()) {
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

    private Map<String, List<StorageInfoRecord>> groupStorageRecInfosByStorageInstId(
        List<StorageInfoRecord> storageInfoRecords) {
        Map<String, List<StorageInfoRecord>> newStorageInstNodeInfoMap = new HashMap<>();
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
        return newStorageInstNodeInfoMap;
    }

    public synchronized void reloadStorageInstsBySpecifyingStorageInstIdList(List<String> targetDnIdList) {
        List<String> tmpDnIdList = new ArrayList<>();
        boolean isReloadAll = targetDnIdList.isEmpty();
        Set<String> allDnSet = this.storageHaCtxCache.keySet();
        if (isReloadAll) {
            tmpDnIdList.addAll(allDnSet);
        } else {
            tmpDnIdList.addAll(targetDnIdList);
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            // Get storage all nodes of one polardbx inst
            Map<String, List<StorageInfoRecord>> targetDnRecListInfoMap =
                storageInfoAccessor.getStorageInfosByStorageInstIdList(tmpDnIdList);
            if (tmpDnIdList.size() != targetDnRecListInfoMap.keySet().size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    "Failed to reload storage because some storageInsts not exists");
            }
            refreshStorageInsts(tmpDnIdList, targetDnRecListInfoMap);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw new TddlNestableRuntimeException(ex);
        }
    }

    public synchronized void refreshStorageInstsBySetting(String targetStorageInstId,
                                                          String newVipAddr,
                                                          String newUser,
                                                          String newEncPasswd) {
        boolean isSucc = false;
        List<Pair<String, Pair<String, String>>> setItems = new ArrayList<>();
        try {
            StorageInstHaContext haContext = this.storageHaCtxCache.get(targetStorageInstId);
            if (haContext == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Failed to refresh storage because the target storageInst[%s] does not exist",
                        targetStorageInstId));
            }

            if (haContext.getStorageKind() != StorageInfoRecord.INST_KIND_META_DB) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format(
                        "Failed to refresh storageInst[%s] because only metadb are allowed to do refresh storage",
                        targetStorageInstId));
            }

            String oldVipAddr = haContext.storageVipAddr;
            if (newVipAddr != null && !newVipAddr.isEmpty()) {
                haContext.storageVipAddr = newVipAddr;
                Pair<String, String> oldNewVals = new Pair<>(oldVipAddr, newVipAddr);
                Pair<String, Pair<String, String>> setItem = new Pair<>("vipAddr", oldNewVals);
                setItems.add(setItem);
            }

            String oldVipUser = haContext.storageVipUser;
            if (newUser != null && !newUser.isEmpty()) {
                haContext.storageVipUser = newUser;
                Pair<String, String> oldNewVals = new Pair<>(oldVipUser, newUser);
                Pair<String, Pair<String, String>> setItem = new Pair<>("user", oldNewVals);
                setItems.add(setItem);
            }

            String oldVipEncPasswd = haContext.storageVipEncPasswd;
            if (newEncPasswd != null && !newEncPasswd.isEmpty()) {
                haContext.storageVipEncPasswd = newEncPasswd;
                Pair<String, String> oldNewVals = new Pair<>(oldVipEncPasswd, newEncPasswd);
                Pair<String, Pair<String, String>> setItem = new Pair<>("encPasswd", oldNewVals);
                setItems.add(setItem);
            }
            isSucc = true;
        } catch (Throwable ex) {
            throw ex;
        } finally {
            logRefreshStorage(isSucc, true, targetStorageInstId, setItems);
        }
    }

    private void logRefreshStorage(boolean isSucc,
                                   boolean fromRefresh,
                                   String dnId,
                                   List<Pair<String, Pair<String, String>>> setItems) {

        StringBuilder logContent = new StringBuilder("");

        String cmdType = "reload storage";
        if (fromRefresh) {
            cmdType = "refresh storage";
        }
        if (isSucc) {
            logContent.append(cmdType).append(" for ");
            logContent.append("dnId=").append(dnId);
            for (int i = 0; i < setItems.size(); i++) {
                if (i > 0) {
                    logContent.append(",");
                }
                Pair<String, Pair<String, String>> item = setItems.get(i);
                String key = item.getKey();
                String oldVal = item.getValue().getKey();
                String newVal = item.getValue().getValue();
                logContent.append("[key=").append(key).append(",").append("oldVal=").append(oldVal).append(",")
                    .append("newVal=").append(newVal).append("]");
            }
        } else {
            logContent.append("failed to ").append(cmdType).append(" for ").append("dnId=[%s]").append(dnId);
        }
        MetaDbLogUtil.META_DB_LOG.warn(logContent.toString());

    }

    public final Map<String, StorageInstHaContext> refreshAndGetStorageInstHaContextCache() {
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getStorageInfoDataId(InstIdUtil.getInstId()));
        if (!ConfigDataMode.isMasterMode()) {
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
            String vipAddr = null;
            StorageInfoRecord vipRecInfo = null;
            for (int j = 0; j < newStorageNodes.size(); j++) {
                StorageInfoRecord storageInfo = newStorageNodes.get(j);
                String ip = storageInfo.ip;
                Integer port = storageInfo.port;
                int isVip = storageInfo.isVip;
                String addrStr = AddressUtils.getAddrStrByIpPort(ip, port);
                if (isVip == StorageInfoRecord.IS_VIP_TRUE) {
                    vipAddr = addrStr;
                    vipRecInfo = storageInfo;
                    if (storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
                        storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER ||
                        storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
                        // if current storage inst is a xcluster inst,
                        // then its vip info should be ignored in storageInfos of storageInstHaContext
                        continue;
                    }
                }
                newStorageNodeInfos.put(addrStr, storageInfo);
            }

            storageInstHaContext.storageNodeInfos = newStorageNodeInfos;
            List<Pair<String, Pair<String, String>>> setItems = new ArrayList<>();
            if (vipAddr != null) {

                String oldVipAddr = storageInstHaContext.storageVipAddr;
                String oldVipUser = storageInstHaContext.storageVipUser;
                String oldVipEncPasswd = storageInstHaContext.storageVipEncPasswd;

                storageInstHaContext.storageVipInfo = vipRecInfo;
                storageInstHaContext.storageVipAddr = vipAddr;
                storageInstHaContext.storageVipUser = vipRecInfo.user;
                storageInstHaContext.storageVipEncPasswd = vipRecInfo.passwdEnc;

                if (!StringUtils.equals(oldVipAddr, vipAddr)) {
                    Pair<String, String> oldNewVals = new Pair<>(oldVipAddr, vipAddr);
                    Pair<String, Pair<String, String>> setItem = new Pair<>("vipAddr", oldNewVals);
                    setItems.add(setItem);
                }

                if (!StringUtils.equals(oldVipUser, vipRecInfo.user)) {
                    Pair<String, String> oldNewVals = new Pair<>(oldVipUser, vipRecInfo.user);
                    Pair<String, Pair<String, String>> setItem = new Pair<>("user", oldNewVals);
                    setItems.add(setItem);
                }

                if (!StringUtils.equals(oldVipEncPasswd, vipRecInfo.passwdEnc)) {
                    Pair<String, String> oldNewVals = new Pair<>(oldVipEncPasswd, vipRecInfo.passwdEnc);
                    Pair<String, Pair<String, String>> setItem = new Pair<>("encPasswd", oldNewVals);
                    setItems.add(setItem);
                }

                logRefreshStorage(true, false, storageInstId, setItems);
            }
        }
    }

    protected void loadStorageHaContext() {

        Map<String, StorageInstHaContext> newStorageHaCtxCache = loadStorageHaContextFromMetaDb();
        for (Map.Entry<String, StorageInstHaContext> haCtxItem : newStorageHaCtxCache.entrySet()) {
            StorageInstHaContext haContext = haCtxItem.getValue();
            if (haContext.storageKind == StorageInfoRecord.INST_KIND_META_DB) {
                this.metaDbStorageHaCtx = haContext;
            }
        }
        this.storageHaCtxCache.putAll(newStorageHaCtxCache);
    }

    protected Map<String, StorageInstHaContext> loadStorageHaContextFromMetaDb() {
        Map<String, StorageInstHaContext> newStorageHaCtxCache = new ConcurrentHashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            List<StorageInfoRecord> storageInfoRecords = new ArrayList<>();

            // Get storage all nodes of one polardbx inst, current instId maybe a master/read-only inst
            loadAllStorageInfoForCurrentInst(storageInfoAccessor, InstIdUtil.getInstId(), storageInfoRecords);

            // group storage node by storageInstId
            Map<String, List<StorageInfoRecord>> storageInstNodeInfoMap =
                groupStorageRecInfosByStorageInstId(storageInfoRecords);

            // init StorageInst HA context for each x-cluster group of storage nodes
            for (Map.Entry<String, List<StorageInfoRecord>> storageInstNodesItem : storageInstNodeInfoMap.entrySet()) {
                String storageInstId = storageInstNodesItem.getKey();
                List<StorageInfoRecord> storageInstNodes = storageInstNodesItem.getValue();
                StorageInstHaContext storageInstHaContext = buildStorageInstHaContext(storageInstNodes);
                newStorageHaCtxCache.put(storageInstId, storageInstHaContext);
            }
            return newStorageHaCtxCache;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.info(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                "Failed to load storage HA context from metaDb，err is " + ex.getMessage());
        }
    }

    protected void loadAllStorageInfoForCurrentInst(StorageInfoAccessor storageInfoAccessor, String currInstId,
                                                    List<StorageInfoRecord> storageInfoRecords) {

        // load storage infos for current inst, but should contain learner storages for master.
        List<StorageInfoRecord> storageInfoRecordsOfCurrInstId = null;
        if (ConfigDataMode.isMasterMode()) {
            storageInfoRecordsOfCurrInstId =
                storageInfoAccessor.getAliveStorageInfos();
            storageInfoRecords.addAll(storageInfoRecordsOfCurrInstId);
        } else if (ConfigDataMode.isRowSlaveMode()) {
            storageInfoRecordsOfCurrInstId =
                storageInfoAccessor.getAliveStorageInfosByInstId(currInstId);
            storageInfoRecords.addAll(storageInfoRecordsOfCurrInstId);
            List<StorageInfoRecord> storageInfoRecordsOfServerMasterInstId =
                storageInfoAccessor.getAliveStorageInfosByInstId(
                    ServerInstIdManager.getInstance().getMasterInstId());
            storageInfoRecords.addAll(storageInfoRecordsOfServerMasterInstId);
        } else if (ConfigDataMode.isColumnarMode()) {
            //fetch the StorageInfoRecord about GMS
            List<StorageInfoRecord> storageList =
                storageInfoAccessor.getStorageInfosByInstIdAndKind(ServerInstIdManager.getInstance().getMasterInstId(),
                    StorageInfoRecord.INST_KIND_META_DB);
            storageInfoRecords.addAll(storageList);
        }
    }

    protected StorageInstHaContext buildStorageInstHaContext(
        List<StorageInfoRecord> storageNodes) {
        assert storageNodes.size() > 0;
        StorageInfoRecord firstNode = storageNodes.get(0);
        String storageVipAddrStr = null;
        List<Pair<String, Boolean>> addrList = new ArrayList<>();
        Map<String, StorageInfoRecord> addrStorageNodeMap = new HashMap<>();
        StorageInfoRecord storageVipInfo = null;
        for (int i = 0; i < storageNodes.size(); i++) {
            StorageInfoRecord storageNode = storageNodes.get(i);
            if (storageNode.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                storageVipAddrStr = AddressUtils.getAddrStrByIpPort(storageNode.ip, storageNode.port);
                storageVipInfo = storageNode;
                if (storageNode.storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
                    storageNode.storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER ||
                    storageNode.storageType == StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
                    // if current storage inst is a xcluster inst,
                    // then its vip info should be ignored in getStorageRole info
                    continue;
                }
            }
            String addr = AddressUtils.getAddrStrByIpPort(storageNode.ip, storageNode.port);
            addrList.add(new Pair<>(addr, storageNode.isVip == StorageInfoRecord.IS_VIP_TRUE));
            addrStorageNodeMap.put(addr, storageNode);
        }

        if (storageVipInfo == null) {
            /**
             * if no found any vip info, then treat first node as vip info
             */
            storageVipInfo = firstNode;
        }
        final boolean noVipAddr;
        if (storageVipAddrStr == null) {
            /**
             * if no found any vip info, then treat first node addr  as vip addr
             */
            storageVipAddrStr = AddressUtils.getAddrStrByIpPort(storageVipInfo.ip, storageVipInfo.port);
            noVipAddr = true;
        } else {
            noVipAddr = false;
        }
        String instId = storageVipInfo.instId;
        String storageInstId = storageVipInfo.storageInstId;
        String storageMasterInstId = storageVipInfo.storageMasterInstId;
        int instKind = storageVipInfo.instKind;
        int storageType = storageVipInfo.storageType;
        String user = storageVipInfo.user;
        String encPasswd = storageVipInfo.passwdEnc;

        boolean allowFetchRoleOnlyFromLeader = true;
        if (instKind == StorageInfoRecord.INST_KIND_SLAVE) {
            allowFetchRoleOnlyFromLeader = false;
        }
        final boolean noCluster = storageType != StorageInfoRecord.STORAGE_TYPE_XCLUSTER &&
            storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER &&
            storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER;
        Map<String, StorageNodeHaInfo> storageNodeHaInfoMap =
            StorageHaChecker
                .checkAndFetchRole(addrList, (noCluster && noVipAddr) ? null : storageVipAddrStr,
                    1 == storageNodes.size() ? firstNode.xport : -1, user, PasswdUtil.decrypt(encPasswd), storageType,
                    instKind, allowFetchRoleOnlyFromLeader);

        return StorageInstHaContext
            .buildStorageInstHaContext(instId,
                storageInstId,
                storageMasterInstId,
                user,
                encPasswd,
                storageType,
                instKind,
                storageVipAddrStr,
                storageVipInfo,
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
            this.phyDbName = DbTopologyManager.getPhysicalDbNameByGroupKeyFromMetaDb(dbName, groupName);
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
            haParams.instId = haSwitchParams.instId;
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
                        CHECK_HA_LOGGER.warn("Can't Trigger HA Task for " + haGroupKey);
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
        protected boolean newIsVip;
        protected String newAvailableAddrUser;
        protected String newAvailableAddrEncPasswd;
        protected Map<String, StorageNodeHaInfo> newStorageNodeHaInfoMap;

        // Task execution results
        private int groupToBeSwitched = 0;
        private int switchTaskCount = 0;
        private String oldAddress;
        private int newXport = 0;
        private List<String> allGrpListToBeSwitched = new ArrayList<>();

        public StorageHaSwitchTask(StorageInstHaContext haContext,
                                   StorageHaManager storageHaManager,
                                   String newAvailableAddr,
                                   boolean newIsVip,
                                   String newAvailableAddrUser,
                                   String newAvailableAddrEncPasswd,
                                   Map<String, StorageNodeHaInfo> newStorageNodeHaInfoMap) {
            this.haContext = haContext;
            this.storageInstId = haContext.storageInstId;
            this.storageHaManager = storageHaManager;
            this.newAvailableAddr = newAvailableAddr;
            this.newIsVip = newIsVip;
            this.newAvailableAddrUser = newAvailableAddrUser;
            this.newAvailableAddrEncPasswd = newAvailableAddrEncPasswd;
            this.newStorageNodeHaInfoMap = newStorageNodeHaInfoMap;
            this.oldAddress = haContext.currAvailableNodeAddr;
        }

        @Override
        public void run() {
            try {
                boolean success = false;
                long startTs = System.currentTimeMillis();
                Throwable ex = null;
                AtomicBoolean isFetchWriteLock = new AtomicBoolean(false);
                try {
                    fetchWriteLockOfHaParams(isFetchWriteLock);
                    if (!isFetchWriteLock.get()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format("Failed to fetch the write lock of HaParams of dn[%s]",
                                haContext.getStorageInstId()));
                    }
                    success = submitGroupHaSwitchTasks(haContext, newStorageNodeHaInfoMap, storageHaManager);
                } catch (Throwable e) {
                    if (haContext.storageKind == StorageInfoRecord.INST_KIND_META_DB) {
                        MetaDbLogUtil.META_DB_LOG.error("Failed to do MetaDB DataSource HASwitch due to ", e);
                    } else {
                        MetaDbLogUtil.META_DB_LOG.error("Failed to do Group DataSource HASwitch due to ", e);
                    }
                } finally {
                    changeHaStatus(haContext, success);
                    if (isFetchWriteLock.get()) {
                        haContext.getHaLock().writeLock().unlock();
                    }
                    long switchEndTs = System.currentTimeMillis();
                    doStorageInstHaLog(success, this.allGrpListToBeSwitched.size(), this.switchTaskCount,
                        startTs, switchEndTs, this.oldAddress, this.newAvailableAddr, this.newIsVip, this.newXport);
                }

                // record smooth switchover events
                if (success && DynamicConfig.getInstance().isEnableSmoothSwitchover()) {
                    final SwitchoverPerfCollection collector =
                        XConnectionManager.getInstance().getSwitchoverPerfCollector(storageInstId);
                    if (collector != null) {
                        EventLogger.log(EventType.SMOOTH_SWITCHOVER_SUMMARY,
                            storageInstId + ',' + oldAddress + ',' + newAvailableAddrEncPasswd + ',' + collector);
                    }
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }

        private void fetchWriteLockOfHaParams(AtomicBoolean isFetchWriteLock) throws InterruptedException {
            boolean fetchLockSucc =
                haContext.getHaLock().writeLock()
                    .tryLock(StorageInstHaContext.HA_WRITE_LOCK_WAIT_TIMEOUT, TimeUnit.SECONDS);
            if (!fetchLockSucc) {
                MetaDbLogUtil.META_DB_LOG.warn(
                    String.format("Failed to fetch the write lock of HaParams of dn[%s] at 1st time, and now do retry",
                        haContext.getStorageInstId()));
                /**
                 * Failed to fetch the write lock of dn HaContext, and do retry
                 */
                for (int i = 0; i < StorageInstHaContext.HA_WRITE_LOCK_RETRY_TIME; i++) {
                    try {
                        Thread.sleep(StorageInstHaContext.SLEEP_TIME_AFTER_FETCH_TIMEOUT);
                    } catch (Throwable e) {
                        // ignore
                    }
                    fetchLockSucc =
                        haContext.getHaLock().writeLock()
                            .tryLock(StorageInstHaContext.HA_WRITE_LOCK_WAIT_TIMEOUT, TimeUnit.SECONDS);
                    if (fetchLockSucc) {
                        break;
                    }
                    MetaDbLogUtil.META_DB_LOG.warn(String.format(
                        "Failed to fetch the write lock of HaParams of dn[%s] at (%d)th time, and now do retry",
                        haContext.getStorageInstId(), i + 1));
                }
            }
            isFetchWriteLock.set(fetchLockSucc);
            return;
        }

        private void changeHaStatus(StorageInstHaContext haContext, boolean success) {
            synchronized (haContext) {
                if (haContext.haStatus == StorageInstHaContext.StorageHaStatus.SWITCHING) {
                    if (success) {
                        haContext.currAvailableNodeAddr = this.newAvailableAddr;
                        haContext.currIsVip = this.newIsVip;
                        haContext.currXport = this.newXport;
                        haContext.user = this.newAvailableAddrUser;
                        haContext.encPasswd = this.newAvailableAddrEncPasswd;
                    } else {
                        if (!StringUtils.isEmpty(newAvailableAddr) && this.newAvailableAddr.equalsIgnoreCase(
                            UNAVAILABLE_ACCESS_FOR_LEARNER)) {
                            //learner is unavaliable, thus refresh the haContext
                            haContext.currAvailableNodeAddr = this.newAvailableAddr;
                            haContext.currIsVip = this.newIsVip;
                            haContext.currXport = this.newXport;
                            haContext.user = this.newAvailableAddrUser;
                            haContext.encPasswd = this.newAvailableAddrEncPasswd;
                        }
                    }

                    // change the status to NORMAL to wait for next switch task to retry if success is false
                    haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
                }

//                if (success) {
//                    // all group switch tasks are successful
//                    if (haContext.haStatus == StorageInstHaContext.StorageHaStatus.SWITCHING) {
//                        haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
//                        haContext.currAvailableNodeAddr = this.newAvailableAddr;
//                    }
//                } else {
//                    // change the status to NORMAL to wait for next switch task to retry
//                    haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
//                }
            }
        }

        protected boolean submitGroupHaSwitchTasks(StorageInstHaContext haContext,
                                                   Map<String, StorageNodeHaInfo> addrWithRoleMap,
                                                   StorageHaManager storageHaManager) {
            // available node addr has changed, so need to do ha switch
            HaSwitchParams haSwitchParams = new HaSwitchParams();
            haSwitchParams.storageInstId = haContext.storageInstId;
            haSwitchParams.storageConnPoolConfig = getConnPoolConfigFromManager();
            haSwitchParams.storageHaInfoMap = addrWithRoleMap;

            haSwitchParams.curAvailableAddr = this.newAvailableAddr;
            haSwitchParams.userName = this.newAvailableAddrUser;
            haSwitchParams.passwdEnc = this.newAvailableAddrEncPasswd;

            haSwitchParams.xport = newXport =
                getAndCheckXport(this.newAvailableAddr, this.newIsVip, haContext,
                    addrWithRoleMap.get(this.newAvailableAddr));
            haSwitchParams.storageKind = haContext.storageKind;
            haSwitchParams.instId = haContext.instId;
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
                    // haContext.haStatus = StorageInstHaContext.StorageHaStatus.NORMAL;
                    changeHaStatus(haContext, taskEx == null);
                    return false;
                }
            }

            if (ConfigDataMode.isMasterMode() &&
                storageInstKind == StorageInfoRecord.INST_KIND_SLAVE) {
                if (haSwitchParams.instId != null &&
                    !ServerInstIdManager.getInstance().getAllHTAPReadOnlyInstIdSet().contains(haSwitchParams.instId)) {
                    //the learner storageId ha, but it not belong to the HTAP inst. Here ignore building datasource.
                    logger.warn(
                        storageInstId + "is ha, but it not belong to the HTAP inst. Here ignore building datasource!");
                    return true;
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
                storageHaManager.groupHaTaskExecutor.submit(() -> {
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
                                          String oldAddr, String newAddr, boolean newIsVip, int Xport) {
            String storageInstHaLog = "";

            String allGrpListStr = String.join(",", allGrpListToBeSwitched);
            String allRoleInfoStr = buildStorageNodeHaInfoLogMsg(this.newStorageNodeHaInfoMap);
            if (isSucc) {
                storageInstHaLog =
                    String.format(
                        "StorageInst[%s] do HA switch successfully, SwitchInfo is [timeCost(ms)=%s, groupCnt=%s, taskCnt=%s, beginTime=%s, endTime=%s, newAddr=%s, newIsVip=%s, oldAddr=%s, newXport=%s, allGrpList=[%s], roleInfos=[%s]]",
                        storageInstId, endTs - beginTs, groupCnt, taskCnt, beginTs, endTs, newAddr,
                        newIsVip ? "true" : "false", oldAddr,
                        Xport, allGrpListStr, allRoleInfoStr);
            } else {
                storageInstHaLog =
                    String.format(
                        "StorageInst[%s] do HA switch failed, SwitchInfo is [timeCost(ms)=%s, groupCnt=%s, taskCnt=%s, beginTime=%s, endTime=%s, newAddr=%s, newIsVip=%s, oldAddr=%s, newXport=%s, allGrpList=[%s], roleInfos=[%s]]",
                        storageInstId, endTs - beginTs, groupCnt, taskCnt, beginTs, endTs, newAddr,
                        newIsVip ? "true" : "false", oldAddr,
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
            try {
                doCheckRoleForStorageNodes();
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }

        protected void doCheckRoleForStorageNodes() {

            int storageType = haContext.storageType;
            int storageKind = haContext.storageKind;

            String user = haContext.user;
            String encPasswd = haContext.encPasswd;
            String vipAddr = haContext.storageVipAddr;
            if (haContext.storageVipInfo != null) {
                /**
                 * If dn has vip info, then use its vip user & vip passwd
                 */

                user = haContext.storageVipInfo.user;
                if (!StringUtils.isEmpty(haContext.getStorageVipUser()) && !haContext.getStorageVipUser()
                    .equals(user)) {
                    /**
                     * Maybe the value of vip_user has been refresh by 'altery system refresh storage' cmd
                     */
                    user = haContext.getStorageVipUser();
                }
                encPasswd = haContext.storageVipInfo.passwdEnc;
                if (!StringUtils.isEmpty(haContext.getStorageVipEncPasswd()) && !haContext.getStorageVipEncPasswd()
                    .equals(encPasswd)) {
                    /**
                     * Maybe the value of vip_encPasswd has been refresh by 'altery system refresh storage' cmd
                     */
                    encPasswd = haContext.getStorageVipEncPasswd();
                }
            }

            List<Pair<String, Boolean>> addrList = haContext.storageNodeInfos.entrySet().stream()
                .map(pair -> new Pair<>(pair.getKey(), pair.getValue().isVip == StorageInfoRecord.IS_VIP_TRUE))
                .collect(Collectors.toList());

            Map<String, StorageNodeHaInfo> addrWithRoleMap = null;
            boolean allowFetchRoleOnlyFromLeader = true;
            if (storageKind == StorageInfoRecord.INST_KIND_SLAVE) {
                allowFetchRoleOnlyFromLeader = false;
            }
            final StorageInfoRecord vipInfo = haContext.getNodeInfoByAddress(
                null == vipAddr ? (addrList.isEmpty() ? null : addrList.get(0).getKey()) : vipAddr);
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
                    if (haInfo.role == StorageRole.LEARNER) {
                        if (haInfo.isHealthy) {
                            availableAddr = addr;
                        } else if (ConfigDataMode.isMasterMode()) {
                            //the learner storage is set by unhealthy access url in master instId.
                            availableAddr = UNAVAILABLE_ACCESS_FOR_LEARNER;
                        }
                        break;
                    }
                }
            }
            checkResults.put(storageInstId, new Pair<>(availableAddr, addrWithRoleMap));
        }
    }

    public List<StorageInstHaContext> getMasterStorageList() {
        return storageHaCtxCache.values().stream().filter(StorageInstHaContext::isDNMaster)
            .collect(Collectors.toList());
    }

    public StorageInstHaContext getMetaDbStorageHaCtx() {
        return metaDbStorageHaCtx;
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

        List<String> targetDnIdList = new ArrayList<>();
        if (instances.size() > 1 && newRole == StorageRole.LEADER) {
            for (String instance : instances) {
                StorageInstHaContext haCtx = storageHaCtxCache.get(instance);
                if (haCtx != null && haCtx.isMasterMode()) {
                    targetDnIdList.add(instance);
                }
            }
            if (targetDnIdList.size() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("no found target rw-storage for node[%s]", address));
            }
        } else {
            targetDnIdList.addAll(instances);
        }

        for (String instance : targetDnIdList) {
            changeRoleOfStorageNode(instance, address, newRole);
        }

        logger.warn(String.format("change replica(%s) to %s", address, newRole));
        MetaDbLogUtil.META_DB_LOG.warn(String.format("change replica(%s) to %s", address, newRole));
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
                replica.storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER &&
                replica.storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
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
            if (!LeaderStatusBridge.getInstance().hasLeadership()) {
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
            if (checkHaRs == null) {
                haStorageInstInfo +=
                    String.format("{dnId=%s,noFoundHaResult}", storageInstId);
                continue;
            }

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
            MetaDbLogUtil.META_DB_LOG.warn(logMsg);
            EventLogger.log(EventType.DN_HA, String.format("Find dn is to do ha, haInfo is [%s]", logMsg));
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
        protected long lastCheckTime = 0;

        public CheckStorageHaTask(StorageHaManager storageHaManager) {
            this.storageHaManager = storageHaManager;
        }

        @Override
        public void run() {
            final long now = System.currentTimeMillis();
            if (now - lastCheckTime >= storageHaManager.checkStorageTaskPeriod) {
                lastCheckTime = now;
                runInner();
            }

            // check and reset interval if no changing leader
            if (DynamicConfig.getInstance().isEnableSmoothSwitchover()) {
                if (storageHaManager.checkStorageTaskPeriod != DynamicConfig.getInstance().getStorageHaTaskPeriod()) {
                    // fast check, switch to normal check when on node in changing
                    if (!XConnectionManager.getInstance().isAnyOneChangingLeader()) {
                        storageHaManager.adjustStorageHaTaskPeriod(
                            DynamicConfig.getInstance().getStorageHaTaskPeriod());
                    }
                }
            }
        }

        public void setDoHaAsync(boolean async) {
            this.doHaAsync = async;
        }

        protected void runInner() {
            // check and submit task for all storage insts
            try {
                doCheckAndSubmitTaskIfNeed(this.storageHaManager.storageHaCtxCache);
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
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
                List<CheckStorageRoleInfoTask> checkHaTasks = new ArrayList<>();
                for (Map.Entry<String, StorageInstHaContext> storageInstItem : storageHaCache.entrySet()) {
                    CheckStorageRoleInfoTask checkRoleInfoTask =
                        new CheckStorageRoleInfoTask(storageInstItem.getKey(), storageInstItem.getValue(),
                            storageInstNewRoleInfoMap);
                    checkHaTasks.add(checkRoleInfoTask);
                }
                CountDownLatch countDownLatch = new CountDownLatch(checkHaTasks.size());
                for (int i = 0; i < checkHaTasks.size(); i++) {
                    CheckStorageRoleInfoTask task = checkHaTasks.get(i);
                    this.storageHaManager.checkDnRoleTaskExecutor.submit(() -> {
                        try {
                            task.run();
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
                }

                // wait for all tasks
                ExecutorUtil.awaitCountDownLatch(countDownLatch);

                // Submit HA switch ds tasks
                boolean forceHa =
                    ConfigDataMode.isMasterMode() && DynamicConfig.getInstance().enableFollowReadForPolarDBX()
                        != this.storageHaManager.allowFollowRead;

                Map<String, Throwable> haSubmitExceptionMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                for (String storageInstIdVal : storageInstNewRoleInfoMap.keySet()) {
                    try {
                        String newAvailableAddr = storageInstNewRoleInfoMap.get(storageInstIdVal).getKey();
                        String newAvailableAddrUser = null;
                        String newAvailableAddrEncPasswd = null;

                        Map<String, StorageNodeHaInfo> addrWithRoleMap =
                            storageInstNewRoleInfoMap.get(storageInstIdVal).getValue();
                        StorageInstHaContext haCache = storageHaCache.get(storageInstIdVal);
                        if (StringUtils.isEmpty(newAvailableAddr) || newAvailableAddr.equalsIgnoreCase(
                            UNAVAILABLE_ACCESS_FOR_LEARNER)) {
                            haCache.isCurrAvailableNodeAddrHealthy = false;
                            //Set the user&passwd although the address is unavailable!
                            newAvailableAddrUser = haCache.getUser();
                            newAvailableAddrEncPasswd = haCache.getEncPasswd();
                        } else {
                            StorageNodeHaInfo healthyVal = addrWithRoleMap.get(newAvailableAddr);
                            if (healthyVal == null) {
                                String logMsg = String.format(
                                    "No found the StorageRole Info for new available addr %s, its roleInfoMap size is %d",
                                    newAvailableAddr, addrWithRoleMap.size());
                                MetaDbLogUtil.META_DB_LOG.warn(logMsg);
                                haCache.isCurrAvailableNodeAddrHealthy = false;
                            } else {
                                haCache.isCurrAvailableNodeAddrHealthy = healthyVal != null && healthyVal.isHealthy;
                                newAvailableAddrUser = healthyVal.getUser();
                                newAvailableAddrEncPasswd = healthyVal.getEncPasswd();
                            }
                        }

                        boolean isMasterStorageInst = haCache.getStorageKind() != StorageInfoRecord.INST_KIND_SLAVE;
                        boolean shouldHa = false;
                        if (checkIfAvailableAddrChanged(newAvailableAddr, haCache.currAvailableNodeAddr)) {
                            if (haCache.haStatus == StorageInstHaContext.StorageHaStatus.NORMAL) {
                                shouldHa = true;
                            }
                        }

                        if (!shouldHa && checkIfAvailableAddrUserPasswdChanged(haCache.user, newAvailableAddrUser,
                            haCache.encPasswd, newAvailableAddrEncPasswd)) {
                            if (haCache.haStatus == StorageInstHaContext.StorageHaStatus.NORMAL) {
                                shouldHa = true;
                            }
                        }

                        if (!shouldHa && ConfigDataMode.isMasterMode()
                            && haCache.storageKind == StorageInfoRecord.INST_KIND_MASTER) {
                            //check the new followers
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
                                MetaDbLogUtil.META_DB_LOG.warn(String.format(
                                    "Force HA due to the follows change from old follow storages [%s] to new follow storages [%s]",
                                    oldFollows, newFollows));
                            }
                            shouldHa = shouldHa || forceHa;
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
                            // check newAvailableAddr is vip address or not
                            boolean isVipAddr = true;
                            for (Map.Entry<String, StorageNodeHaInfo> addrRoleInfo : storageInstNewRoleInfoMap.get(
                                storageInstIdVal).getValue().entrySet()) {
                                if (addrRoleInfo.getKey().equals(newAvailableAddr) && !addrRoleInfo.getValue()
                                    .isVip()) {
                                    isVipAddr = false;
                                    break; // found one with same addr but not vip, means vip is set to actual leader addr
                                }
                            }

                            MetaDbLogUtil.META_DB_LOG.info(
                                "XCluster StorageDB submitHaSwitchTask newAvailableAddr=" + newAvailableAddr + " isVip="
                                    + (
                                    isVipAddr ? "true" : "false"));

                            submitHaSwitchTask(newAvailableAddr, isVipAddr, newAvailableAddrUser,
                                newAvailableAddrEncPasswd,
                                addrWithRoleMap, haCache);
                        }
                    } catch (Throwable ex) {
                        haSubmitExceptionMap.put(storageInstIdVal, ex);
                        MetaDbLogUtil.META_DB_LOG.error(String.format(
                            "StorageInst[%s] failed to submit its ha switch task, the error msg is %s",
                            storageInstIdVal, ex.getMessage()), ex);
                        haCheckEx = ex;
                    }
                }
                this.storageHaManager.allowFollowRead = DynamicConfig.getInstance().enableFollowReadForPolarDBX();
                endTs = System.nanoTime();
            } catch (Throwable ex) {
                haCheckEx = ex;
                MetaDbLogUtil.META_DB_LOG.error(ex);
            } finally {
                try {
                    StorageHaManager.logHaTask(endTs - beginTs, storageInstNewRoleInfoMap, shouldHaFlags, haCheckEx);
                } catch (Throwable ex) {
                    MetaDbLogUtil.META_DB_LOG.error(ex);
                }
            }
        }

        private void submitHaSwitchTask(String newAvailableAddr,
                                        boolean newIsVip,
                                        String newAvailableAddrUser,
                                        String newAvailableAddrEncPasswd,
                                        Map<String, StorageNodeHaInfo> addrWithRoleMap,
                                        StorageInstHaContext haCache) {
            StorageHaSwitchTask storageHaSwitchTask = null;
            synchronized (haCache) {
                if (haCache.haStatus == StorageInstHaContext.StorageHaStatus.NORMAL) {
                    haCache.haStatus = StorageInstHaContext.StorageHaStatus.SWITCHING;
                    storageHaSwitchTask =
                        new StorageHaSwitchTask(haCache, storageHaManager,
                            newAvailableAddr, newIsVip, newAvailableAddrUser, newAvailableAddrEncPasswd,
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

        protected boolean checkIfAvailableAddrUserPasswdChanged(String oldUser,
                                                                String newUser,
                                                                String oldPasswdEnc,
                                                                String newPasswdEnc) {

            if (!StringUtils.equals(oldUser, newUser)) {
                return true;
            }

            if (!StringUtils.equals(oldPasswdEnc, newPasswdEnc)) {
                return true;
            }

            return false;
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
                    haContext.storageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER &&
                    haContext.storageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
                    continue;
                }
                if (haContext.storageKind == StorageInfoRecord.INST_KIND_SLAVE) {
                    // ignore slave storage
                    continue;
                }

                List<Pair<String, Boolean>> addrList = haContext.storageNodeInfos.entrySet().stream()
                    .map(pair -> new Pair<>(pair.getKey(), pair.getValue().isVip == StorageInfoRecord.IS_VIP_TRUE))
                    .collect(Collectors.toList());

                String user = haContext.user;
                String passwd = PasswdUtil.decrypt(haContext.encPasswd);
                int storageInstKind = haContext.storageKind;
                final StorageInfoRecord vipInfo = haContext.getNodeInfoByAddress(
                    null == haContext.storageVipAddr ? (addrList.isEmpty() ? null : addrList.get(0).getKey()) :
                        haContext.storageVipAddr);
                Map<String, StorageNodeHaInfo> hostHaInfoMap = StorageHaChecker
                    .checkAndFetchRole(addrList, haContext.storageVipAddr, null == vipInfo ? -1 : vipInfo.xport, user,
                        passwd, haContext.storageType, storageInstKind, true);
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

                    /**
                     * Some storage-instance exists in memory but not in metadb
                     */
                    if (CollectionUtils.isEmpty(storageInfoRecordList)) {
                        continue;
                    }

                    Set<String> addrSetFromMetaDb = new HashSet<>();
                    for (StorageInfoRecord storageInfoRecord : storageInfoRecordList) {
                        if (storageInfoRecord.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                            /**
                             * Ignore the record of vip of dn, because the addr of vip
                             * maybe the same as the addr of the dn leader
                             */
                            continue;
                        }
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
