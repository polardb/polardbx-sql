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

package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.TAtomDsStandard;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.atom.config.gms.TAtomDsGmsConfigHelper;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.HaSwitcher;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageNodeHaInfo;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.INST_KIND_MASTER;
import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.INST_KIND_SLAVE;

/**
 * Not thread safe, due to the OptimizedGroupConfigManager maybe manager multi readHAStorageIds.
 */
public class OptimizedGroupConfigManager extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory
        .getLogger(OptimizedGroupConfigManager.class);

    private final TGroupDataSource groupDataSource;

    private final HaSwitcher groupDsSwithcer;

    private volatile Map<String/* Atom dbIndex */, DataSourceWrapper/* Wrapper过的Atom DS */> dataSourceWrapperMap =
        new HashMap<>();

    private volatile GroupDataSourceHolder groupDataSourceHolder = null;

    private volatile Set<String> registeredHAstorageIds = new HashSet<>();

    private volatile HashSet<String> listenerInstIds = new HashSet<>();

    public OptimizedGroupConfigManager(TGroupDataSource tGroupDataSource) {
        this.groupDataSource = tGroupDataSource;
        this.groupDsSwithcer = new GroupDataSourceSwitcher(groupDataSource);
    }

    /**
     * 从Diamond配置中心提取信息，构造TAtomDataSource、构造有优先级信息的读写DBSelector ---add by
     * mazhidan.pt
     */
    @Override
    public void doInit() {
        if (ConfigDataMode.isFastMock()) {
            // mock weight comma str
            mockDataSourceWrapper();
            return;
        }

        // To be load by MetaDB
        initGroupDataSourceByMetaDb();
    }

    protected void initGroupDataSourceByMetaDb() {
        Set<String> instIds = new HashSet<>();

        if (ConfigDataMode.isMasterMode()) {
            instIds.add(InstIdUtil.getInstId());
            //ignore the buildInDB which needn't the separation of reading and writing!
            if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName())) {
                instIds.addAll(ServerInstIdManager.getInstance().getAllHTAPReadOnlyInstIdSet());
            }
        } else if (ConfigDataMode.isRowSlaveMode()) {
            instIds.add(InstIdUtil.getInstId());
            instIds.add(InstIdUtil.getMasterInstId());
        } else if (ConfigDataMode.isColumnarMode() && SystemDbHelper.isDBBuildInExceptCdc(
            groupDataSource.getSchemaName())) {
            //here still need create information_schema group datasource for columnar mode.
            instIds.add(InstIdUtil.getInstId());
        }

        if (instIds.isEmpty()) {
            return;
        }

        this.listenerInstIds.clear();
        this.listenerInstIds.addAll(instIds);

        Throwable ex = null;
        try {
            loadGroupDataSourceByMetaDb(instIds);
            registerGroupInfoListener(instIds);
        } catch (Throwable e) {
            ex = e;
            throw e;
        } finally {
            if (ex != null) {
                unregisterHaSwitcher();
            }
        }
    }

    public void loadGroupDataSourceByMetaDb(Set<String> instIds) {
        // (storageId -> (index -> DataSourceWrapper))
        List<Pair<HaSwitchParams, List<DataSourceWrapper>>> rets = null;
        List<HaSwitchParams> outputHaSwitchParamsWithReadLock = new ArrayList<>();
        try {
            /**
             * Build new datasource by latest leaderAddr(rw-dn) or learnerAddr(ro-dn),
             * it need acquire read lock by calling storageInstHaContext.getHaLock().readLock()
             */
            rets = buildDataSourceWrapperByGms(instIds, outputHaSwitchParamsWithReadLock);
            synchronized (this) {
                /**
                 *  Acquire sync lock of OptimizedGroupConfigManager to update the master-slave data sources info
                 *  by synchronized
                 */
                List<DataSourceWrapper> dswList = new ArrayList<>();
                for (Pair<HaSwitchParams, List<DataSourceWrapper>> pair : rets) {
                    dswList.addAll(pair.getValue());
                }
                resetByPolarDBXDataSourceWrapper(dswList);

                String dbName = groupDataSource.getSchemaName();
                String groupName = groupDataSource.getDbGroupKey();
                updateListenerStorageInstId(dbName, groupName);
            }
        } finally {
            /**
             * Because fetch all readLocks of cared dn in calling buildDataSourceWrapperByGms(),
             * so here need unlock all read locks
             */
            try {
                for (int i = 0; i < outputHaSwitchParamsWithReadLock.size(); i++) {
                    HaSwitchParams haParams = outputHaSwitchParamsWithReadLock.get(i);
                    if (!haParams.autoUnlock && haParams.haLock != null) {
                        try {
                            haParams.haLock.readLock().unlock();
                        } catch (Throwable ex) {
                            MetaDbLogUtil.META_DB_LOG.warn(
                                String.format("Failed to release read lock of dn[%s], err is %s",
                                    haParams.storageInstId, ex.getMessage()), ex);
                        }
                    }
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.warn(ex);
            }
        }

    }

    /**
     * load group datasource by mock
     */
    private void mockDataSourceWrapper() {
        TAtomDsConfDO atomDsConf = new TAtomDsConfDO();
        TAtomDataSource atomDs = new TAtomDataSource(TAtomDataSource.AtomSourceFrom.MASTER_DB, "");
        String dsLeaderKey = GroupInfoUtil.buildAtomKey(groupDataSource.getDbGroupKey(), "", null, null);
        atomDs.init(groupDataSource.getAppName(), groupDataSource.getDbGroupKey(), dsLeaderKey, "", atomDsConf);
        this.groupDataSourceHolder = new MasterSlaveGroupDataSourceHolder(atomDs, Collections.emptyList());
    }

    private synchronized void registerStorageHaTasks(Set<String> needCareHaStorageIds) {

        //calculate the need care storageIds
        Set<String> storageIds = dataSourceWrapperMap.entrySet().stream().map(
            t -> t.getValue().getStorageId()).collect(Collectors.toSet());
        storageIds.addAll(needCareHaStorageIds);

        //remove the old switchers!
        unregisterHaSwitcher();

        Set<String> oldStorageSets = new HashSet<>();
        oldStorageSets.addAll(this.registeredHAstorageIds);
        this.registeredHAstorageIds.clear();
        this.registeredHAstorageIds = storageIds;

        //register the new switchers!
        String dbName = groupDataSource.getSchemaName();
        String groupName = groupDataSource.getDbGroupKey();
        for (String storageInstId : storageIds) {
            StorageHaManager.getInstance().registerHaSwitcher(storageInstId, dbName, groupName, groupDsSwithcer);
        }

        LoggerInit.TDDL_DYNAMIC_CONFIG.info(String
            .format(
                "[GroupStorageChangeSucceed] Group storageInstId change from [%s] to [%s], and HaSwitcher has been updated.",
                oldStorageSets, storageIds));
    }

    private synchronized void registerGroupInfoListener(Set<String> instIds) {
        String dbName = groupDataSource.getSchemaName();
        String groupName = groupDataSource.getDbGroupKey();
        for (String instId : instIds) {
            if (instId.equalsIgnoreCase(InstIdUtil.getInstId())) {
                //register and bind
                MetaDbConfigManager.getInstance()
                    .register(MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, groupName), null);
                MetaDbConfigManager.getInstance()
                    .bindListener(MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, groupName),
                        new GroupDetailInfoListener(this, instId, dbName));
            } else {
                //only bind
                String dataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, groupName);
                MetaDbConfigManager.getInstance()
                    .bindListener(dataId, -1, new GroupDetailInfoListener(this, instId, dbName));
            }
        }
    }

    protected synchronized void updateListenerStorageInstId(String dbName, String group) {

        //listen the new instIds & storage instIds
        Set<String> careInstIds = new HashSet<>();
        Set<String> careStorageInstIds = new HashSet<>();

        if (ConfigDataMode.isMasterMode()) {
            Set<String> htapIds = ServerInstIdManager.getInstance().getAllHTAPReadOnlyInstIdSet();
            ServerInstIdManager.getInstance().getInstId2StorageIds().entrySet().stream().forEach(t -> {
                    String inst = t.getKey();
                    if (inst.equalsIgnoreCase(InstIdUtil.getInstId()) || htapIds.contains(inst)) {
                        careInstIds.add(t.getKey());
                        careStorageInstIds.addAll(t.getValue());
                    }
                }
            );
        } else if (ConfigDataMode.isRowSlaveMode()) {
            careInstIds.add(ServerInstIdManager.getInstance().getMasterInstId());
            careInstIds.add(ServerInstIdManager.getInstance().getInstId());
            ServerInstIdManager.getInstance().getInstId2StorageIds().entrySet().stream().forEach(t -> {
                if (careInstIds.contains(t.getKey())) {
                    careStorageInstIds.addAll(t.getValue());
                }
            });
        }

        if (ConfigDataMode.isMasterMode()) {
            //ignore the buildInDB which needn't the separation of reading and writing!
            if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName())) {
                Set<String> newInstIdSet = Sets.difference(
                    careInstIds, listenerInstIds);

                Set<String> removeInstIdSet = Sets.difference(
                    listenerInstIds, careInstIds);

                //1. build the new bind for instID.
                for (String instId : newInstIdSet) {
                    //The master instId only listener the learner's dataId which maybe not write in time.
                    // Here the init version is -1, thus the listener can be invoke once the learner instId write the dataId.
                    MetaDbConfigManager.getInstance().bindListener(
                        MetaDbDataIdBuilder.getGroupConfigDataId(
                            instId, dbName, group), -1, new GroupDetailInfoListener(this, instId, dbName));
                }

                //2. make sure clean up the useless bind for instID.
                for (String instId : removeInstIdSet) {
                    MetaDbConfigManager.getInstance().unbindListener(MetaDbDataIdBuilder.getGroupConfigDataId(
                        instId, dbName, group));
                }

                if (newInstIdSet.size() > 0 || removeInstIdSet.size() > 0) {
                    listenerInstIds.clear();
                    listenerInstIds.addAll(careInstIds);
                    LoggerInit.TDDL_DYNAMIC_CONFIG.info(String
                        .format("register the group info for [%s], add the [%s], remove the [%s]", listenerInstIds,
                            newInstIdSet, removeInstIdSet));
                }
            }
        }

        HashSet<String> buildStorageInstIds = new HashSet<>();
        for (DataSourceWrapper dataSourceWrapper : dataSourceWrapperMap.values()) {
            buildStorageInstIds.add(dataSourceWrapper.getStorageId());
        }

        //get the removed storageIds.
        Set<String> removeStorageInstIds = new HashSet<>();
        if (!SystemDbHelper.isDBBuildIn(dbName)) {
            //ignore the BuildDB
            removeStorageInstIds.addAll(Sets.difference(
                buildStorageInstIds, careStorageInstIds));
        }

        Set<String> storageIdsInstIdListOfExistGroup = new HashSet<>();
        Set<String> needCareHaStorageId = new HashSet<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            // Find all storage inst_id (included polardbx master inst and polardbx slave inst)
            storageIdsInstIdListOfExistGroup =
                groupDetailInfoAccessor.getStorageInstIdListByStorageIdAndDbNameAndGroupName(
                    buildStorageInstIds, dbName, group);
            //get the ha cared-storages, some storage maybe not in dataSourceWrapperMap.
            needCareHaStorageId =
                groupDetailInfoAccessor.getStorageInstIdListByStorageIdAndDbNameAndGroupName(
                    careStorageInstIds, dbName, group);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        //get the useless storageIds.
        for (String buildStorageId : buildStorageInstIds) {
            if (!storageIdsInstIdListOfExistGroup.contains(buildStorageId)) {
                removeStorageInstIds.add(buildStorageId);
            }
        }
        //3. make sure clean up the useless connection pool
        if (removeStorageInstIds.size() > 0) {
            cleanUselessSourceWrapper(removeStorageInstIds);
        }
        //only dataSourceWrapperMap changed, here update the HA tasks;
        registerStorageHaTasks(needCareHaStorageId);
    }

    /**
     * the Listener to handle all the change of system 'group_detail_info'
     */
    protected static class GroupDetailInfoListener implements ConfigListener {

        protected final OptimizedGroupConfigManager groupConfigManager;
        protected final String instId;
        protected final String dbName;

        public GroupDetailInfoListener(OptimizedGroupConfigManager groupConfigManager, String instId, String dbName) {
            this.groupConfigManager = groupConfigManager;
            this.instId = instId;
            this.dbName = dbName;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            HashSet<String> careInstIds = Sets.newHashSet();
            if (ConfigDataMode.isMasterMode()) {
                careInstIds.add(instId);
                //ignore the buildInDB which needn't the separation of reading and writing!
                if (!SystemDbHelper.isDBBuildIn(dbName)) {
                    ServerInstIdManager.getInstance().getAllHTAPReadOnlyInstIdSet().stream().forEach(t -> {
                        careInstIds.add(t);
                    });
                }
                this.groupConfigManager.loadGroupDataSourceByMetaDb(careInstIds);
            } else if (ConfigDataMode.isRowSlaveMode()) {
                careInstIds.add(instId);
                careInstIds.add(ServerInstIdManager.getInstance().getMasterInstId());
                careInstIds.add(ServerInstIdManager.getInstance().getInstId());
                this.groupConfigManager.loadGroupDataSourceByMetaDb(careInstIds);
            } else if (ConfigDataMode.isColumnarMode() && SystemDbHelper.isDBBuildInExceptCdc(dbName)) {
                //here still need create information_schema group datasource for columnar mode.
                careInstIds.add(instId);
                this.groupConfigManager.loadGroupDataSourceByMetaDb(careInstIds);
            }
        }
    }

    protected synchronized void unregisterHaSwitcher() {
        String groupName = groupDataSource.getDbGroupKey();
        for (String storageInstId : registeredHAstorageIds) {
            StorageHaManager.getInstance().unregisterHaSwitcher(storageInstId, groupName, groupDsSwithcer);
        }
    }

    protected void unbindGroupConfigListener() {
        String dbName = groupDataSource.getSchemaName();
        String groupName = groupDataSource.getDbGroupKey();

        MetaDbConfigManager.getInstance()
            .unbindListener(MetaDbDataIdBuilder.getGroupConfigDataId(InstIdUtil.getInstId(), dbName, groupName));
        for (String instId : listenerInstIds) {
            String dataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, groupName);
            MetaDbConfigManager.getInstance()
                .unbindListener(dataId);
        }
    }

    protected synchronized List<DataSourceWrapper> switchGroupDs(HaSwitchParams haSwitchParams) {

        String userName = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String phyDbName = haSwitchParams.phyDbName;
        ConnPoolConfig storageInstConfig = haSwitchParams.storageConnPoolConfig;
        String curAvailableAddr = haSwitchParams.curAvailableAddr;

        List<DataSourceWrapper> dswList = new ArrayList<>();
        try {
            String schemaName = groupDataSource.getSchemaName();
            String appName = groupDataSource.getAppName();
            String groupName = groupDataSource.getDbGroupKey();
            String availableNodeAddr = curAvailableAddr;
            boolean needDoSwitch = false;
            if (availableNodeAddr != null) {
                //只有leader是可利用的，才支持刷新GroupDs
                Map<String, DataSourceWrapper> curDsWrappers = this.getDataSourceWrapperMap();
                //leader
                String weightStr;
                TAtomDataSource.AtomSourceFrom flag = TAtomDataSource.AtomSourceFrom.MASTER_DB;
                if (ServerInstIdManager.getInstance().isMasterInstId(haSwitchParams.instId)) {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 10);
                } else {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 0);
                    flag = TAtomDataSource.AtomSourceFrom.LEARNER_DB;
                }
                String leaderDsKey = GroupInfoUtil
                    .buildAtomKey(groupName, haSwitchParams.storageInstId, availableNodeAddr,
                        haSwitchParams.phyDbName);
                DataSourceWrapper newLeaderVal = curDsWrappers.get(leaderDsKey);
                if (curDsWrappers.containsKey(leaderDsKey) && newLeaderVal != null &&
                    newLeaderVal.getWeightStr().equalsIgnoreCase(weightStr)) {
                    dswList.add(curDsWrappers.get(leaderDsKey));
                } else {
                    TAtomDsConfDO atomDsConf = TAtomDsGmsConfigHelper
                        .buildAtomDsConfByGms(availableNodeAddr, haSwitchParams.xport, userName, passwdEnc,
                            phyDbName,
                            storageInstConfig, schemaName);
                    TAtomDataSource atomDs = new TAtomDataSource(flag, haSwitchParams.storageInstId);

                    atomDs.init(appName, groupName, leaderDsKey, "", atomDsConf);
                    DataSourceWrapper dsw =
                        new DataSourceWrapper(
                            haSwitchParams.storageInstId, leaderDsKey, weightStr, atomDs, 0);
                    dswList.add(dsw);
                    needDoSwitch = true;
                }
                if (ConfigDataMode.isMasterMode() && haSwitchParams.storageKind == INST_KIND_MASTER) {
                    //follower node
                    if (haSwitchParams.storageHaInfoMap != null) {
                        String slaveWeightStr = GroupInfoUtil.buildWeightStr(10, 0);
                        int dataSourceIndex = 1;
                        for (StorageNodeHaInfo haInfo : haSwitchParams.storageHaInfoMap.values()) {
                            if (haInfo.getRole() == StorageRole.FOLLOWER) {
                                String slaveKey =
                                    GroupInfoUtil
                                        .buildAtomKey(groupName, haSwitchParams.storageInstId, haInfo.getAddr(),
                                            haSwitchParams.phyDbName);
                                DataSourceWrapper newSlaveVal = curDsWrappers.get(slaveKey);
                                if (newSlaveVal != null && newSlaveVal.getWeightStr()
                                    .equalsIgnoreCase(slaveWeightStr)) {
                                    if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName())
                                        && DynamicConfig.getInstance().enableFollowReadForPolarDBX()) {
                                        dswList.add(curDsWrappers.get(slaveKey));
                                    } else {
                                        //need remove slave datasources;
                                        needDoSwitch = true;
                                    }
                                } else {
                                    //只有leader节点开启xport后, slave节点才开启
                                    if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName())
                                        && DynamicConfig.getInstance().enableFollowReadForPolarDBX()) {
                                        int xport = -1;
                                        if (haSwitchParams.xport > 0) {
                                            xport = haInfo.getXPort();
                                        }
                                        TAtomDsConfDO slaveAtomDsConf = TAtomDsGmsConfigHelper
                                            .buildAtomDsConfByGms(haInfo.getAddr(), xport,
                                                haSwitchParams.userName,
                                                haSwitchParams.passwdEnc, haSwitchParams.phyDbName,
                                                haSwitchParams.storageConnPoolConfig, phyDbName);
                                        //the follower datasource
                                        TAtomDataSource slaveAtomDs = new TAtomDataSource(
                                            TAtomDataSource.AtomSourceFrom.FOLLOWER_DB, haSwitchParams.storageInstId);

                                        slaveAtomDs.init(appName, groupName, slaveKey, "", slaveAtomDsConf);

                                        DataSourceWrapper slave =
                                            new DataSourceWrapper(
                                                haSwitchParams.storageInstId, slaveKey, slaveWeightStr, slaveAtomDs,
                                                dataSourceIndex++);
                                        dswList.add(slave);
                                        needDoSwitch = true;
                                    }

                                }
                            }

                        }
                    }
                }
            }

            if (!dswList.isEmpty() && needDoSwitch) {
                resetByPolarDBXDataSourceWrapper(dswList);
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        return dswList;
    }

    protected List<Pair<HaSwitchParams, List<DataSourceWrapper>>> buildDataSource(
        String appName, String unitName, Set<String> instIds, String dbName, String groupName,
        List<HaSwitchParams> outputHaSwitchParamsWithReadLock, boolean ignoreSlaveException) {
        StorageHaManager.getInstance()
            .getStorageHaSwitchParamsForInitGroupDs(instIds, dbName, groupName, outputHaSwitchParamsWithReadLock);
        List<HaSwitchParams> haSwitchParamsList = outputHaSwitchParamsWithReadLock;
        if (haSwitchParamsList.size() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("instId[%s] is NOT available", instIds));
        }

        List<Pair<HaSwitchParams, List<DataSourceWrapper>>> rets = new ArrayList<>();
        for (HaSwitchParams haSwitchParams : haSwitchParamsList) {
            String availableAddr = haSwitchParams.curAvailableAddr;
            if (availableAddr == null) {
                if (haSwitchParams.instId == InstIdUtil.getInstId()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("storageInst[%s] is NOT available", haSwitchParams.storageInstId));
                } else {
                    //ignore the haSwitchParam if the instId is not InstIdUtil.getInstId().
                    logger.warn(String.format("storageInst[%s] is NOT available", haSwitchParams.storageInstId));
                    continue;
                }
            }
            try {
                String dsLeaderKey =
                    GroupInfoUtil.buildAtomKey(groupName, haSwitchParams.storageInstId, availableAddr,
                        haSwitchParams.phyDbName);

                DataSourceWrapper newDsw = null;
                String weightStr;
                TAtomDataSource.AtomSourceFrom flag = TAtomDataSource.AtomSourceFrom.MASTER_DB;
                if (ServerInstIdManager.getInstance().isMasterInstId(haSwitchParams.instId)) {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 10);
                } else {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 0);
                    flag = TAtomDataSource.AtomSourceFrom.LEARNER_DB;
                }

                if (dataSourceWrapperMap.get(dsLeaderKey) != null && !DynamicConfig.getInstance()
                    .forceCreateGroupDataSource()) {
                    logger.warn("Needn't create the new DataSourceWrapper for " + dsLeaderKey);
                    newDsw = dataSourceWrapperMap.get(dsLeaderKey);
                } else {
                    TAtomDsConfDO atomDsConf = TAtomDsGmsConfigHelper
                        .buildAtomDsConfByGms(availableAddr, haSwitchParams.xport, haSwitchParams.userName,
                            haSwitchParams.passwdEnc, haSwitchParams.phyDbName, haSwitchParams.storageConnPoolConfig,
                            dbName);
                    TAtomDataSource atomDs = new TAtomDataSource(flag, haSwitchParams.storageInstId);
                    try {
                        atomDs.init(appName, groupName, dsLeaderKey, unitName, atomDsConf);
                    } catch (Throwable t) {
                        if (ConfigDataMode.isMasterMode() && !ServerInstIdManager.getInstance()
                            .isMasterInstId(haSwitchParams.instId)) {
                            //catch the Exception in order to avoid effect the master connections.
                            logger.error("init the datasource failed for " + dsLeaderKey, t);
                            continue;
                        }
                        throw new RuntimeException(t);
                    }
                    newDsw = new DataSourceWrapper(
                        haSwitchParams.storageInstId, dsLeaderKey, weightStr, atomDs, 0);
                }
                rets.add(new Pair<>(haSwitchParams, Lists.newArrayList(newDsw)));
            } catch (Throwable t) {
                if (haSwitchParams != null && haSwitchParams.storageKind == INST_KIND_SLAVE && ignoreSlaveException) {
                    MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.warn(
                        String.format("storageInst[%s] init connection failed! ", haSwitchParams.storageInstId), t);
                } else {
                    throw new RuntimeException(t);
                }
            }
        }

        return rets;
    }

    /**
     * If the connection pool fails to load, it may result in startup failure.
     * However, for the primary instance, if the connection pool of the read-only instance is loaded,
     * even if it fails, we should ignore the error message.
     */
    protected List<Pair<HaSwitchParams, List<DataSourceWrapper>>> buildDataSourceWrapperByGms(Set<String> instIds,
                                                                                              List<HaSwitchParams> outputHaSwitchParamsWithReadLock) {
        String unitName = "";
        List<Pair<HaSwitchParams, List<DataSourceWrapper>>> dataSourceWrapperLists = null;
        try {
            String dbName = this.groupDataSource.getSchemaName();
            String appName = this.groupDataSource.getAppName();
            String groupName = this.groupDataSource.getDbGroupKey();

            // build DatasourceWrapper for the instIds
            dataSourceWrapperLists =
                buildDataSource(appName, unitName, instIds, dbName, groupName, outputHaSwitchParamsWithReadLock,
                    ConfigDataMode.isMasterMode());
            if (dataSourceWrapperLists.size() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("instId[%s] is NOT available", instIds));
            }

            if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName()) && DynamicConfig.getInstance()
                .enableFollowReadForPolarDBX() && ConfigDataMode.isMasterMode()) {
                for (Pair<HaSwitchParams, List<DataSourceWrapper>> dataSourceWrapperPair : dataSourceWrapperLists) {
                    HaSwitchParams haSwitchParams = dataSourceWrapperPair.getKey();
                    if (haSwitchParams.storageKind == INST_KIND_MASTER) {
                        //在PolarDb-X下如果curAvailableAddr 节点为非空，且HaSwitchParams表名该地址是主库地址，则允许备库一致性读
                        if (haSwitchParams.storageHaInfoMap != null) {
                            //build DatasourceWrapper for follower
                            for (StorageNodeHaInfo haInfo : haSwitchParams.storageHaInfoMap.values()) {
                                if (haInfo.getRole() == StorageRole.FOLLOWER) {
                                    String slaveKey =
                                        GroupInfoUtil
                                            .buildAtomKey(groupName, haSwitchParams.storageInstId, haInfo.getAddr(),
                                                haSwitchParams.phyDbName);
                                    //只有leader节点开启xport后, slave节点才开启
                                    int xport = -1;
                                    if (haSwitchParams.xport > 0) {
                                        xport = haInfo.getXPort();
                                    }
                                    TAtomDsConfDO slaveAtomDsConf = TAtomDsGmsConfigHelper
                                        .buildAtomDsConfByGms(haInfo.getAddr(), xport, haSwitchParams.userName,
                                            haSwitchParams.passwdEnc, haSwitchParams.phyDbName,
                                            haSwitchParams.storageConnPoolConfig, dbName);
                                    String slaveWeightStr = GroupInfoUtil.buildWeightStr(10, 0);
                                    //the follower datasource.
                                    TAtomDataSource slaveAtomDs = new TAtomDataSource(
                                        TAtomDataSource.AtomSourceFrom.FOLLOWER_DB, haSwitchParams.storageInstId);

                                    slaveAtomDs.init(appName, groupName, slaveKey, unitName, slaveAtomDsConf);
                                    DataSourceWrapper follower = new DataSourceWrapper(
                                        haSwitchParams.storageInstId, slaveKey, slaveWeightStr, slaveAtomDs, 1);
                                    dataSourceWrapperPair.getValue().add(follower);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        return dataSourceWrapperLists;
    }

    private synchronized void resetByPolarDBXDataSourceWrapper(List<DataSourceWrapper> changeDswList) {

        Map<String, DataSourceWrapper> uselessSourceWrapper = new HashMap<>();
        Map<String, DataSourceWrapper> newDataSourceWrapperMap =
            new HashMap<String, DataSourceWrapper>(changeDswList.size());

        Set<String> newInstIdOfDataSourceWrapper = new HashSet<>();
        for (DataSourceWrapper dataSourceWrapper : changeDswList) {
            newInstIdOfDataSourceWrapper.add(dataSourceWrapper.getStorageId());
            newDataSourceWrapperMap.put(dataSourceWrapper.getDataSourceKey(), dataSourceWrapper);
        }

        for (Map.Entry<String, DataSourceWrapper> entry : dataSourceWrapperMap.entrySet()) {
            if (newInstIdOfDataSourceWrapper.contains(entry.getValue().getStorageId())) {
                uselessSourceWrapper.put(entry.getKey(), entry.getValue());
            } else {
                newDataSourceWrapperMap.put(entry.getKey(), entry.getValue());
            }
        }

        Iterator<Map.Entry<String, DataSourceWrapper>> iterator = newDataSourceWrapperMap.entrySet().iterator();
        boolean failedSlave = false;
        while (iterator.hasNext()) {
            DataSourceWrapper dsw = iterator.next().getValue();
            if (dsw.getDataSourceKey().contains(StorageHaManager.UNAVAILABLE_URL_FOR_LEARNER)) {
                iterator.remove();
                //主动剔除确认无效的连接池
                destroy(dsw);
                failedSlave = true;
            }
        }

        this.dataSourceWrapperMap = newDataSourceWrapperMap;

        resetPolarDBXSourceHolder(failedSlave);
        // 需要考虑关闭老的DataSource对象
        cleanUselessSourceWrapper(uselessSourceWrapper, true);
    }

    private synchronized void resetPolarDBXSourceHolder(boolean failedSlave) {
        if (dataSourceWrapperMap.size() == 1) {
            this.groupDataSourceHolder = failedSlave ?
                new MasterFailedSlaveGroupDataSourceHolder(
                    dataSourceWrapperMap.values().iterator().next().getWrappedDataSource()) :
                new MasterOnlyGroupDataSourceHolder(
                    dataSourceWrapperMap.values().iterator().next().getWrappedDataSource());
        } else {

            TAtomDataSource masterDataSource = null;
            List<TAtomDataSource> slaveDataSources = new ArrayList<TAtomDataSource>();

            for (DataSourceWrapper dataSourceWrapper : dataSourceWrapperMap.values()) {
                if (dataSourceWrapper.hasWriteWeight()) {
                    masterDataSource = dataSourceWrapper.getWrappedDataSource();
                } else {
                    slaveDataSources.add(dataSourceWrapper.getWrappedDataSource());
                }

            }

            if (GeneralUtil.isEmpty(slaveDataSources)) {
                /**
                 * 备库没有任何读权重
                 */
                this.groupDataSourceHolder = failedSlave ?
                    new MasterFailedSlaveGroupDataSourceHolder(masterDataSource) :
                    new MasterOnlyGroupDataSourceHolder(masterDataSource);
            } else {
                this.groupDataSourceHolder = new MasterSlaveGroupDataSourceHolder(masterDataSource, slaveDataSources);
            }

        }
    }

    private synchronized void cleanUselessSourceWrapper(Set<String> uselessStorageIds) {
        Iterator<Map.Entry<String, DataSourceWrapper>> iterator = dataSourceWrapperMap.entrySet().iterator();
        LoggerInit.TDDL_DYNAMIC_CONFIG.info(String.format("unregister %s storageIds", uselessStorageIds));
        while (iterator.hasNext()) {
            DataSourceWrapper dsw = iterator.next().getValue();
            if (uselessStorageIds.contains(dsw.getStorageId())) {
                try {
                    LoggerInit.TDDL_DYNAMIC_CONFIG.info(String.format(
                        "unregister %s storageIds successfully", dsw.getDataSourceKey()));
                    iterator.remove();
                    destroy(dsw);
                } catch (Throwable e) {
                    logger.error("we got exception when close datasource : " + dsw.getDataSourceKey(), e);
                }
            } else {
                logger.warn("continue save the storageId : " + dsw.getDataSourceKey());
            }
        }
        resetPolarDBXSourceHolder(false);
    }

    private synchronized void destroy(DataSourceWrapper dsw) {
        DataSource ds = dsw.getWrappedDataSource();
        if (ds instanceof TAtomDsStandard) {
            try {
                TAtomDsStandard tads = (TAtomDsStandard) ds;
                tads.destroyDataSource();
                MatrixStatistics.removeAtom(groupDataSource.getAppName(),
                    groupDataSource.getDbGroupKey(),
                    dsw.getDataSourceKey());// 清除掉该GROUP下面的该TAOM
            } catch (Throwable t) {
                logger.error("error", t);
            }
        } else {
            logger.error("target datasource is not a TAtom Data Source");
        }
    }

    private synchronized void cleanUselessSourceWrapper(
        Map<String, DataSourceWrapper> uselessSourceWrapper, boolean polarDBX) {
        // 需要考虑关闭老的DataSource对象
        for (String dbKey : uselessSourceWrapper.keySet()) {
            DataSourceWrapper old = uselessSourceWrapper.get(dbKey);
            boolean repeated = polarDBX && (old != dataSourceWrapperMap.get(dbKey));
            if (!dataSourceWrapperMap.containsKey(dbKey) || repeated) {
                // 新的列表中没有dbKey或者两者存储的value不一样，执行一下关闭
                try {
                    destroy(old);
                } catch (Throwable e) {
                    logger.error("we got exception when close datasource : " + old.getDataSourceKey(), e);
                }
                LoggerInit.TDDL_DYNAMIC_CONFIG.info(String.format(
                    "unregister %s storageIds successfully, it exists the same dbKey: %b", dbKey, repeated));
            } else {
                logger.warn("continue save the storageId : " + dbKey);
            }
        }
        uselessSourceWrapper.clear();
    }

    @Override
    protected synchronized void doDestroy() {
        // 关闭下层DataSource
        if (dataSourceWrapperMap != null) {
            for (DataSourceWrapper dsw : dataSourceWrapperMap.values()) {
                try {
                    DataSource ds = dsw.getWrappedDataSource();
                    if (ds instanceof TAtomDsStandard) {
                        TAtomDsStandard tads = (TAtomDsStandard) ds;
                        tads.destroyDataSource();
                    } else {
                        logger.error("target datasource is not a TAtom Data Source");
                        LoggerInit.TDDL_DYNAMIC_CONFIG.error("target datasource is not a TAtom Data Source");

                    }
                } catch (Exception e) {
                    logger.error("we got exception when close datasource : " + dsw.getDataSourceKey(), e);
                    LoggerInit.TDDL_DYNAMIC_CONFIG
                        .error("we got exception when close datasource : " + dsw.getDataSourceKey(), e);

                }
            }
        }

        try {
            unregisterHaSwitcher();
            unbindGroupConfigListener();
            //clean the cache datasource for physical backfill
            PhysicalBackfillUtils.destroyDataSources();
        } catch (Exception e) {
            logger.error("we got exception when close datasource .", e);
        }
    }

    public void destroyDataSource() {
        destroy();
    }

    /**
     * return multi DataSourceWrappers (master&&slave) for the same group
     */
    public Map<String/* Atom dbIndex */, DataSourceWrapper/* Wrapper过的Atom DS */> getDataSourceWrapperMap() {
        return this.dataSourceWrapperMap;
    }

    public TAtomDataSource getDataSource(MasterSlave masterSlave) {
        if (groupDataSourceHolder == null && ConfigDataMode.isColumnarMode()) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA,
                "don't support query the table without columnar index!", new NullPointerException());
        }
        return this.groupDataSourceHolder.getDataSource(masterSlave);
    }

    protected String getServerInstIdForGroupDataSource() {
        return null;
    }

    protected class GroupDataSourceSwitcher implements HaSwitcher {

        TGroupDataSource groupDs = null;

        public GroupDataSourceSwitcher(TGroupDataSource groupDs) {
            this.groupDs = groupDs;
        }

        @Override
        public void doHaSwitch(HaSwitchParams haSwitchParams) {
            String groupName = groupDs.getDbGroupKey();
            String dbName = groupDs.getSchemaName();
            try {
                /**
                 * When this method is calling, that mean at least dn has HA or groupDetailInfo has benn changed.
                 * At this time, the writeLock of dn of StorageHaContext has been acquired by calling
                 *  StorageHaContext.getHaLock().writeLock()
                 */
                switchGroupDs(haSwitchParams);
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_DYNAMIC_CONFIG
                    .error(String.format("Failed to do switch ds for [%s/%s]", groupName, dbName),
                        ex);
                throw GeneralUtil.nestedException(ex);
            }
        }
    }
}
