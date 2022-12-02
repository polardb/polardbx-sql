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
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
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
import com.alibaba.polardbx.config.ConfigDataListener;
import com.alibaba.polardbx.config.ConfigDataMode;
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
import com.alibaba.polardbx.group.jdbc.DataSourceFetcher;
import com.alibaba.polardbx.group.jdbc.DataSourceLazyInitWrapper;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.biv.MockUtils;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.INST_KIND_MASTER;

/**
 * Not thread safe, due to the OptimizedGroupConfigManager maybe manager multi readHAStorageIds.
 */
public class OptimizedGroupConfigManager extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory
        .getLogger(OptimizedGroupConfigManager.class);

    private final ConfigDataListener configReceiver;
    private final TGroupDataSource groupDataSource;
    private final HaSwitcher groupDsSwithcer;

    /**
     * 是否需要GroupConfigManager主动去初始化所有在它之下的atomDataSource
     */
    private boolean createTAtomDataSource = true;

    private volatile Map<String/* Atom dbIndex */, DataSourceWrapper/* Wrapper过的Atom DS */> dataSourceWrapperMap =
        new HashMap<>();

    private volatile GroupDataSourceHolder groupDataSourceHolder = null;

    private volatile Set<String> registeredHAstorageIds = new HashSet<>();

    private volatile HashSet<String> listenerInstIds = new HashSet<>();

    public OptimizedGroupConfigManager(TGroupDataSource tGroupDataSource) {
        this.groupDataSource = tGroupDataSource;
        this.configReceiver = new ConfigReceiver();
        this.groupDsSwithcer = new GroupDataSourceSwitcher(groupDataSource);
        ((ConfigReceiver) this.configReceiver).setConfigManager(this);
    }

    /**
     * @param dsWeightCommaStr : 例如 db0:rwp1q1i0, db1:rwp0q0i1
     */
    public static List<DataSourceWrapper> buildDataSourceWrapper(String dsWeightCommaStr, DataSourceFetcher fetcher) {
        String[] dsWeightArray = dsWeightCommaStr.split(","); // 逗号分隔：db0:rwp1q1i0,
        // db1:rwp0q0i1
        List<DataSourceWrapper> dss = new ArrayList<DataSourceWrapper>(dsWeightArray.length);
        for (int i = 0; i < dsWeightArray.length; i++) {
            String[] dsAndWeight = dsWeightArray[i].split(":"); // 冒号分隔：db0:rwp1q1i0
            String dsKey = dsAndWeight[0].trim();
            String weightStr = dsAndWeight.length == 2 ? dsAndWeight[1] : null;

            try {
                DataSourceWrapper dsw = buildDataSourceWrapper(dsKey, weightStr, i, fetcher);
                dss.add(dsw);
            } catch (Throwable e) {

                LoggerInit.TDDL_DYNAMIC_CONFIG.error(String
                    .format("[buildDataSourceWrapper] failed, dsKey is [%s], weightStr is [%s] ", dsKey, weightStr), e);

                throw GeneralUtil.nestedException(String
                    .format("[buildDataSourceWrapper] failed, dsKey is [%s], weightStr is [%s] ", dsKey, weightStr), e);
            }

        }
        return dss;
    }

    protected static DataSourceWrapper buildDataSourceWrapper(String dsKey, String weightStr, int index,
                                                              DataSourceFetcher fetcher) {

        // 如果多个group复用一个真实dataSource，会造成所有group引用
        // 这个dataSource的配置 会以最后一个dataSource的配置为准
        TAtomDataSource dataSource = fetcher.getDataSource(dsKey);
        DataSourceWrapper dsw = new DataSourceWrapper(dsKey, weightStr, dataSource, index);
        return dsw;
    }

    /**
     * 从Diamond配置中心提取信息，构造TAtomDataSource、构造有优先级信息的读写DBSelector ---add by
     * mazhidan.pt
     */
    @Override
    public void doInit() {
        if (ConfigDataMode.isFastMock()) {
            // mock weight comma str
            parse(MockUtils.mockDsWeightCommaStr(groupDataSource.getDbGroupKey()));
            return;
        }

        // To be load by MetaDB
        initGroupDataSourceByMetaDb();
    }

    protected void initGroupDataSourceByMetaDb() {

        Set<String> instIds = new HashSet<>();
        instIds.add(InstIdUtil.getInstId());
        if (ServerInstIdManager.getInstance().isMasterInst()) {
            //ignore the buildInDB which needn't the separation of reading and writing!
            if (!SystemDbHelper.isDBBuildIn(groupDataSource.getSchemaName())) {
                instIds.addAll(ServerInstIdManager.getInstance().getAllReadOnlyInstIdSet());
            }
        } else {
            instIds.add(InstIdUtil.getMasterInstId());
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
                            MetaDbLogUtil.META_DB_LOG.warn(String.format("Failed to release read lock of dn[%s], err is %s", haParams.storageInstId, ex.getMessage()), ex);
                        }

                    }
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.warn(ex);
            }
        }

    }

    private synchronized void registerStorageHaTasks() {

        Set<String> storageIds = dataSourceWrapperMap.entrySet().stream().map(
            t -> t.getValue().getStorageId()).collect(Collectors.toSet());
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

        if (ServerInstIdManager.getInstance().isMasterInst()) {
            ServerInstIdManager.getInstance().getInstId2StorageIds().entrySet().stream().forEach(t -> {
                    careInstIds.add(t.getKey());
                    careStorageInstIds.addAll(t.getValue());
                }
            );
        } else {
            careInstIds.add(ServerInstIdManager.getInstance().getMasterInstId());
            careInstIds.add(ServerInstIdManager.getInstance().getInstId());
            ServerInstIdManager.getInstance().getInstId2StorageIds().entrySet().stream().forEach(t -> {
                if (careInstIds.contains(t.getKey())) {
                    careStorageInstIds.addAll(t.getValue());
                }
            });
        }

        if (ServerInstIdManager.getInstance().isMasterInst()) {
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
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            // Find all storage inst_id (included polardbx master inst and polardbx slave inst)
            storageIdsInstIdListOfExistGroup =
                groupDetailInfoAccessor.getStorageInstIdListByStorageIdAndDbNameAndGroupName(
                    buildStorageInstIds, dbName, group);
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
        registerStorageHaTasks();
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
            careInstIds.add(instId);
            if (ServerInstIdManager.getInstance().isMasterInst()) {
                //ignore the buildInDB which needn't the separation of reading and writing!
                if (!SystemDbHelper.isDBBuildIn(dbName)) {
                    ServerInstIdManager.getInstance().getInstId2StorageIds().entrySet().stream().forEach(t -> {
                        careInstIds.add(t.getKey());
                    });
                }
            } else {
                careInstIds.add(ServerInstIdManager.getInstance().getMasterInstId());
                careInstIds.add(ServerInstIdManager.getInstance().getInstId());
            }

            this.groupConfigManager.loadGroupDataSourceByMetaDb(careInstIds);
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
                if (ConfigDataMode.enableSlaveReadForPolarDbX()) {
                    //leader
                    String weightStr;
                    if (ServerInstIdManager.getInstance().isMasterInstId(haSwitchParams.instId)) {
                        weightStr = GroupInfoUtil.buildWeightStr(10, 10);
                    } else {
                        weightStr = GroupInfoUtil.buildWeightStr(10, 0);
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
                        TAtomDataSource atomDs = new TAtomDataSource(true);
                        atomDs.setDnId(haSwitchParams.storageInstId);

                        atomDs.init(appName, groupName, leaderDsKey, "", atomDsConf);
                        DataSourceWrapper dsw =
                            new DataSourceWrapper(
                                haSwitchParams.storageInstId, leaderDsKey, weightStr, atomDs, 0);
                        dswList.add(dsw);
                        needDoSwitch = true;
                    }
                    //salves
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
                                if (curDsWrappers.containsKey(slaveKey) && newSlaveVal != null &&
                                    newSlaveVal.getWeightStr().equalsIgnoreCase(slaveWeightStr)) {
                                    dswList.add(curDsWrappers.get(slaveKey));
                                } else {
                                    //只有leader节点开启xport后, slave节点才开启
                                    int xport = -1;
                                    if (haSwitchParams.xport > 0) {
                                        xport = haInfo.getXPort();
                                    }
                                    TAtomDsConfDO slaveAtomDsConf = TAtomDsGmsConfigHelper
                                        .buildAtomDsConfByGms(haInfo.getAddr(), xport,
                                            haSwitchParams.userName,
                                            haSwitchParams.passwdEnc, haSwitchParams.phyDbName,
                                            haSwitchParams.storageConnPoolConfig, phyDbName);
                                    TAtomDataSource slaveAtomDs = new TAtomDataSource(true);

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
                } else {
                    needDoSwitch = true;
                    String leaderDsKey = GroupInfoUtil
                        .buildAtomKey(groupName, haSwitchParams.storageInstId, availableNodeAddr,
                            haSwitchParams.phyDbName);
                    String weightStr;
                    if (ServerInstIdManager.getInstance().isMasterInstId(haSwitchParams.instId)) {
                        weightStr = GroupInfoUtil.buildWeightStr(10, 10);
                    } else {
                        weightStr = GroupInfoUtil.buildWeightStr(10, 0);
                    }
                    if (curDsWrappers.size() == 1 && curDsWrappers.containsKey(leaderDsKey)) {
                        DataSourceWrapper curDsw = curDsWrappers.get(leaderDsKey);
                        if (curDsw.getWeightStr().equalsIgnoreCase(weightStr)) {
                            needDoSwitch = false;
                        }
                    }
                    if (needDoSwitch) {
                        TAtomDsConfDO atomDsConf = TAtomDsGmsConfigHelper
                            .buildAtomDsConfByGms(availableNodeAddr, haSwitchParams.xport, userName, passwdEnc,
                                phyDbName,
                                storageInstConfig, schemaName);
                        TAtomDataSource atomDs = new TAtomDataSource(true);
                        atomDs.init(appName, groupName, leaderDsKey, "", atomDsConf);
                        DataSourceWrapper dsw = new DataSourceWrapper(
                            haSwitchParams.storageInstId, leaderDsKey, weightStr, atomDs, 0);
                        dswList.add(dsw);
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
        String appName, String unitName, Set<String> instIds, String dbName, String groupName, List<HaSwitchParams> outputHaSwitchParamsWithReadLock) {
        StorageHaManager.getInstance().getStorageHaSwitchParamsForInitGroupDs(instIds, dbName, groupName, outputHaSwitchParamsWithReadLock);
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
            String dsLeaderKey =
                GroupInfoUtil.buildAtomKey(groupName, haSwitchParams.storageInstId, availableAddr,
                    haSwitchParams.phyDbName);

            DataSourceWrapper newDsw = null;
            if (dataSourceWrapperMap.get(dsLeaderKey) != null && !DynamicConfig.getInstance()
                .forceCreateGroupDataSource()) {
                logger.warn("Needn't create the new DataSourceWrapper for " + dsLeaderKey);
                newDsw = dataSourceWrapperMap.get(dsLeaderKey);
            } else {
                TAtomDsConfDO atomDsConf = TAtomDsGmsConfigHelper
                    .buildAtomDsConfByGms(availableAddr, haSwitchParams.xport, haSwitchParams.userName,
                        haSwitchParams.passwdEnc, haSwitchParams.phyDbName, haSwitchParams.storageConnPoolConfig,
                        dbName);
                TAtomDataSource atomDs = new TAtomDataSource(true);
                atomDs.setDnId(haSwitchParams.storageInstId);
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

                String weightStr;
                if (ServerInstIdManager.getInstance().isMasterInstId(haSwitchParams.instId)) {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 10);
                } else {
                    weightStr = GroupInfoUtil.buildWeightStr(10, 0);
                }
                newDsw = new DataSourceWrapper(
                    haSwitchParams.storageInstId, dsLeaderKey, weightStr, atomDs, 0);
            }
            rets.add(new Pair<>(haSwitchParams, Lists.newArrayList(newDsw)));
        }

        return rets;
    }

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
                buildDataSource(appName, unitName, instIds, dbName, groupName, outputHaSwitchParamsWithReadLock);
            if (dataSourceWrapperLists.size() == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("instId[%s] is NOT available", instIds));
            }

            if (ConfigDataMode.enableSlaveReadForPolarDbX()) {
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
                                    TAtomDataSource slaveAtomDs = new TAtomDataSource(true);
                                    slaveAtomDs.setDnId(haSwitchParams.storageInstId);

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

    /**
     * 根据普通的DataSource构造读写DBSelector
     */
    public void init(List<DataSourceWrapper> dataSourceWrappers) {
        if ((dataSourceWrappers == null) || dataSourceWrappers.size() < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "dataSourceWrappers is empty");
        }
        createTAtomDataSource = false;
        resetByDataSourceWrapper(dataSourceWrappers);
        isInited = true;
    }

    ;

    private TAtomDataSource initAtomDataSource(String appName, String groupKey, String dsKey, String unitName,
                                               Weight weight) {
        try {
            TAtomDataSource atomDataSource = new TAtomDataSource(weight.w > 0);
            atomDataSource.init(appName, groupKey, dsKey, unitName);
            atomDataSource.setLogWriter(groupDataSource.getLogWriter());
            atomDataSource.setLoginTimeout(groupDataSource.getLoginTimeout());
            return atomDataSource;
        } catch (TddlException e) {
            throw GeneralUtil.nestedException(e);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException("TAtomDataSource init failed: dsKey=" + dsKey, e);
        }
    }

    // configInfo样例: db1:rw, db2:r, db3:r
    private synchronized void parse(String dsWeightCommaStr) {

        // 首先，根据配置信息（样例: db1:rw, db2:r, db3:），获取新的数据源列表
        List<DataSourceWrapper> dswList = parse2DataSourceWrapperList(dsWeightCommaStr);

        // 更新内存的数据源列表
        resetByDataSourceWrapper(dswList);
    }

    /**
     * 警告: 逗号的位置很重要，要是有连续的两个逗号也不要人为的省略掉， 数据库的个数 =
     * 逗号的个数+1，用0、1、2...编号，比如"db1,,db3"，实际上有3个数据库，
     * 业务层通过传一个ThreadLocal进来，ThreadLocal中就是这种索引编号。
     */
    private List<DataSourceWrapper> parse2DataSourceWrapperList(String dsWeightCommaStr) {

        logger.info("[parse2DataSourceWrapperList]dsWeightCommaStr=" + dsWeightCommaStr);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("[parse2DataSourceWrapperList]dsWeightCommaStr=" + dsWeightCommaStr);
        this.groupDataSource.setDsKeyAndWeightCommaArray(dsWeightCommaStr);
        if ((dsWeightCommaStr == null) || (dsWeightCommaStr = dsWeightCommaStr.trim()).length() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_MISS_GROUPKEY,
                groupDataSource.getDbGroupKey(),
                null,
                groupDataSource.getAppName(),
                groupDataSource.getUnitName());
        }
        return buildDataSourceWrapperSequential(dsWeightCommaStr);
    }

    /**
     * 将封装好的AtomDataSource的列表，进一步封装为可以根据权重优先级随机选择模板库的DBSelector ---add by
     * mazhidan.pt
     */
    private synchronized void resetByDataSourceWrapper(List<DataSourceWrapper> dswList) {
        // 删掉已经不存在的DataSourceWrapper
        Map<String, DataSourceWrapper> newDataSourceWrapperMap = new HashMap<String, DataSourceWrapper>(dswList.size());
        for (DataSourceWrapper dsw : dswList) {
            newDataSourceWrapperMap.put(dsw.getDataSourceKey(), dsw);
        }
        Map<String, DataSourceWrapper> old = this.dataSourceWrapperMap;
        this.dataSourceWrapperMap = newDataSourceWrapperMap;
        /**
         * 清除一下atomDelayMap , 但并发情况下不加锁仍然无法完全保证这里面全是新的atom
         */
        if (dswList.size() == 1) {
            /**
             * 只存在主库的情况
             */
            this.groupDataSourceHolder = new MasterOnlyGroupDataSourceHolder(
                dswList.iterator().next().getWrappedDataSource());
        } else {

            TAtomDataSource masterDataSource = null;
            List<TAtomDataSource> slaveDataSources = new ArrayList<TAtomDataSource>();

            List<Pair<Object, Integer>> readWeightsWithMaster = new ArrayList<Pair<Object, Integer>>();
            List<Pair<Object, Integer>> readWeightsSlaveOnly = new ArrayList<Pair<Object, Integer>>();
            List<String> slaveStorageIds = new ArrayList<>();

            for (DataSourceWrapper dataSourceWrapper : dswList) {
                if (dataSourceWrapper.hasWriteWeight()) {
                    masterDataSource = dataSourceWrapper.getWrappedDataSource();
                    if (dataSourceWrapper.hasReadWeight()) {
                        readWeightsWithMaster
                            .add(Pair.of(dataSourceWrapper.getWrappedDataSource(), dataSourceWrapper.getWeight().r));
                    }
                } else {
                    slaveDataSources.add(dataSourceWrapper.getWrappedDataSource());
                    if (dataSourceWrapper.hasReadWeight()) {
                        readWeightsSlaveOnly
                            .add(Pair.of(dataSourceWrapper.getWrappedDataSource(), dataSourceWrapper.getWeight().r));
                        readWeightsWithMaster
                            .add(Pair.of(dataSourceWrapper.getWrappedDataSource(), dataSourceWrapper.getWeight().r));

                    }
                    slaveStorageIds.add(dataSourceWrapper.getStorageId());
                }

            }

            if (GeneralUtil.isEmpty(readWeightsSlaveOnly)) {
                /**
                 * 备库没有任何读权重
                 */
                this.groupDataSourceHolder = new MasterOnlyGroupDataSourceHolder(masterDataSource);
            } else {

                //FIXME PolarDb-X模式下，主实例暂不支持权重，严格按照Hint来区分主备库流量；且目前主实例CN不能访问只读实例DN
                this.groupDataSourceHolder =
                    new MasterSlaveGroupDataSourceHolder(masterDataSource, slaveDataSources, slaveStorageIds);
            }

        }

        cleanUselessSourceWrapper(old, false);
    }

    private synchronized void resetByPolarDBXDataSourceWrapper(List<DataSourceWrapper> dswList) {

        Map<String, DataSourceWrapper> uselessSourceWrapper = new HashMap<>();
        Map<String, DataSourceWrapper> newDataSourceWrapperMap = new HashMap<String, DataSourceWrapper>(dswList.size());

        Set<String> newInstIdOfDataSourceWrapper = new HashSet<>();
        for (DataSourceWrapper dataSourceWrapper : dswList) {
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
            List<String> slaveStorageIds = new ArrayList<>();

            for (DataSourceWrapper dataSourceWrapper : dataSourceWrapperMap.values()) {
                if (dataSourceWrapper.hasWriteWeight()) {
                    masterDataSource = dataSourceWrapper.getWrappedDataSource();
                } else {
                    slaveDataSources.add(dataSourceWrapper.getWrappedDataSource());
                    slaveStorageIds.add(dataSourceWrapper.getStorageId());
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
                this.groupDataSourceHolder = new MasterSlaveGroupDataSourceHolder(
                    masterDataSource, slaveDataSources, slaveStorageIds);
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

    /**
     * 专用于主备更换时获取数据源
     */
    protected DataSourceWrapper buildDataSourceWrapper(String dsKey, String weightStr, int index) {

        DataSourceWrapper dsw = null;

        Weight weight = new Weight(weightStr);

        // fetcher 由原本的大家共用改为各个dsw单独使用
        // 这样各个dsw的dbType不会有并发冲突， by chengbi
        DataSourceFetcher fetcher = null;
        try {

            fetcher = new MyDataSourceFetcher(weight);

            // 如果多个group复用一个真实dataSource，会造成所有group引用
            // 这个dataSource的配置 会以最后一个dataSource的配置为准
            TAtomDataSource dataSource = fetcher.getDataSource(dsKey);

            dsw = new DataSourceWrapper(dsKey, weightStr, dataSource, index);
            return dsw;

        } catch (Throwable e) {

            String msg = String.format(
                "[buildDataSourceWrapper] Failed to initialize atom datasource and changed to use lazyInit mode for atom datasource, dsKey is [%s], weightStr is [%s] ",
                dsKey,
                weightStr);

            Throwable ex = new TddlNestableRuntimeException(msg, e);
            logger.warn(ex);
            LoggerInit.TDDL_DYNAMIC_CONFIG.warn(ex);

            fetcher = new MyDataSourceLazyInitFetcher(weight);

            /**
             * 这里是主备切换即使在新数据失败后也能生效的关键一步：
             *
             * <pre>
             *  因为当新数据源初始化失败时，原来的逻辑直接对上层逻辑报错，导致无法更新内存的主备配置, 进而影响了主备切换结果；
             *  而这里通过将数据源改为LazyInit(就是下一次请求过来时再初始化数据源)，可以让主备切换的
             *  流程继续往下走而不会被中断，进而保证主备切换的操作肯定生效。
             * </pre>
             */

            // 这里要注意：当数据初始化失败后，
            // 改用LazyInit的dsw, 等真正在使用数据源时，再来初始化
            // 之所以这样设计, 是因为当 DBA 做主备切换后，
            // 新的数据库有可能在初始化就出现问题（如需要prefill=true）或出现超时,
            // 因此，这里LazyInitDataSourceWrapper使用
            dsw = new DataSourceLazyInitWrapper(dsKey, weightStr, fetcher, index);
        }

        return dsw;

    }

    public List<DataSourceWrapper> buildDataSourceWrapperSequential(String dsWeightCommaStr) {

        final String[] dsWeightArray = dsWeightCommaStr.split(","); // 逗号分隔：db0:rwp1q1i0,

        // db1:rwp0q0i1
        List<DataSourceWrapper> dss = new ArrayList<DataSourceWrapper>(dsWeightArray.length);

        for (int i = 0; i < dsWeightArray.length; i++) {
            final int j = i;
            final String[] dsAndWeight = dsWeightArray[j].split(":"); // 冒号分隔：db0:rwp1q1i0
            final String dsKey = dsAndWeight[0].trim();
            String weightStr = dsAndWeight.length == 2 ? dsAndWeight[1] : null;
            try {
                DataSourceWrapper newDsw = buildDataSourceWrapper(dsKey, weightStr, j);
                dss.add(newDsw);
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }

        }

        return dss;
    }

    // 仅用于测试
    public void receiveConfigInfo(String configInfo) {
        configReceiver.onDataReceived(null, configInfo);
    }

    // 仅用于测试
    public void resetDbGroup(String configInfo) {
        try {
            parse(configInfo);
        } catch (Throwable t) {
            logger.error("resetDbGroup failed:" + configInfo, t);
        }

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
        return this.groupDataSourceHolder.getDataSource(masterSlave);
    }

    public GroupDataSourceHolder getGroupDataSourceHolder() {
        return groupDataSourceHolder;
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

    protected class MyDataSourceFetcher implements DataSourceFetcher {

        private Weight weight;

        public MyDataSourceFetcher(Weight weight) {
            this.weight = weight;
        }

        @Override
        public TAtomDataSource getDataSource(String dsKey) {
            DataSourceWrapper dsw = dataSourceWrapperMap.get(dsKey);

            if (dsw != null) {
                // 当dsw的值不为null时，则直接返回数据源
                return dsw.getWrappedDataSource();

            } else {

                // 当dsw的值为null时，则重新初始化数据源
                if (createTAtomDataSource) {
                    TAtomDataSource atomDs = initAtomDataSource(groupDataSource.getAppName(),
                        groupDataSource.getDbGroupKey(),
                        dsKey,
                        groupDataSource.getUnitName(),
                        weight);
                    return atomDs;
                } else {
                    throw new IllegalArgumentException(dsKey + " not exist!");
                }
            }
        }
    }

    /**
     * <pre>
     * 专用于atom数据源lazy init 的dataSoureFetcher
     *
     * 目前只用于主备切换过程，出现新库初始化失败后，将失败的新库转化为lazyInit的这一过程
     * </pre>
     *
     * @author chenghui.lch 2017年1月21日 下午5:12:55
     * @since 5.0.0
     */
    protected class MyDataSourceLazyInitFetcher implements DataSourceFetcher {

        private Weight weight;

        public MyDataSourceLazyInitFetcher(Weight weight) {
            this.weight = weight;
        }

        @Override
        public TAtomDataSource getDataSource(String dsKey) {

            TAtomDataSource atomDs = null;
            try {
                // 当dsw的值为null时，则重新初始化数据源
                atomDs = initAtomDataSource(groupDataSource.getAppName(),
                    groupDataSource.getDbGroupKey(),
                    dsKey,
                    groupDataSource.getUnitName(),
                    weight);

                return atomDs;
            } catch (Throwable e) {
                String msg = "Failed to initialize atom datasource in lazyInit mode ! dbKey is " + dsKey;
                throw GeneralUtil.nestedException(msg, e);
            }
        }

    }

    private class ConfigReceiver implements ConfigDataListener {

        private OptimizedGroupConfigManager configManager;

        public void setConfigManager(OptimizedGroupConfigManager configManager) {
            this.configManager = configManager;
        }

        @Override
        public void onDataReceived(String dataId, String data) {
            try {
                String oldData = this.configManager.groupDataSource.getDsKeyAndWeightCommaArray();
                LoggerInit.TDDL_DYNAMIC_CONFIG.info("[Data Recieved] [group datasource] dataId:" + dataId
                    + ", new data:" + data + ", old data:" + oldData);
                parse(data);
            } catch (Throwable t) {
                logger.error("error occurred during parsing group dynamic configs : " + data, t);
                LoggerInit.TDDL_DYNAMIC_CONFIG.error("error occurred during parsing group dynamic configs : " + data,
                    t);

            }
        }
    }
}
