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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.model.RepoInst;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.repo.RepositoryHolder;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.biv.MockUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * group以及其对应的执行器
 *
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.0.0
 */
public class TopologyHandler extends AbstractLifecycle {

    public final static Logger logger = LoggerFactory.getLogger(TopologyHandler.class);

    private final Map<String/* group key of upper case */, IGroupExecutor> executorMap =
        new ConcurrentHashMap<String, IGroupExecutor>();

    private String appName;
    private String unitName;
    private String schemaName;
    private Matrix matrix = new Matrix();
    private Map<String, Object> connProperties;
    private TopologyGmsListener topologyGmsListener;
    transient private List<TopologyHandler> subTopologyHandlers = new ArrayList();
    private RepositoryHolder repositoryHolder;
    private ExecutorContext executorContext;

    /**
     * All the groups that can participate in XA trans
     * <pre>
     *     Notice: This group list may be refreshed because of addGroups/removeGroups from scale out
     * </pre>
     */
    private volatile List<String> allTransGroupList = new ArrayList<>();

    private volatile TopologyChanger topologyChanger;

    public TopologyHandler(String appName, String schemaName, String unitName,
                           ExecutorContext executorContext) {
        this.appName = appName;
        this.schemaName = schemaName;
        this.unitName = unitName;
        this.topologyGmsListener = new TopologyGmsListener(schemaName);
        this.repositoryHolder = new RepositoryHolder();
        this.executorContext = executorContext;
    }

    public TopologyHandler(String appName, String schemaName, String unitName,
                           Map<String, Object> connProperties, ExecutorContext executorContext) {
        this(appName, schemaName, unitName, executorContext);
        this.connProperties = connProperties;
    }

    public interface TopologyChanger {
        void onTopology(TopologyHandler topologyHandler);
    }

    @Override
    protected void doInit() {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TopologyHandler start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        try {
            // refresh the topology for matrix
            refresh();
        } catch (Throwable ex) {
            logger.error("matrix topology init error,file is: appname is: " + this.getAppName(), ex);
            throw GeneralUtil.nestedException(ex);
        }
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getDbTopologyDataId(schemaName), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getDbTopologyDataId(schemaName), topologyGmsListener);
    }

    public Map getCp() {
        return connProperties;
    }

    private void reloadStorageManager() {
        if (executorContext == null) {
            return;
        }
        final StorageInfoManager storageInfoManager = executorContext.getStorageInfoManager();

        if (null != storageInfoManager) {
            storageInfoManager.destroy();
        }
    }

    private Matrix initMatrix() {
        // get mock matrix info
        if (ConfigDataMode.isFastMock()) {
            return MockUtils.getMatrix(schemaName);
        }

        Matrix matrix = initPolarDbXTopology();
        return matrix;
    }

    /**
     * Only destroy the old group executors that is NOT in new group list.
     */
    public void refresh() {
        synchronized (executorMap) {
            Matrix matrix = initMatrix();
            mergeMatrix(matrix);
            reloadStorageManager();
        }
    }

    /**
     * destroy all the group executors and recreate all the group executors again
     */
    public void reload() {
        synchronized (executorMap) {
            clean();
            Matrix matrix = initMatrix();
            mergeMatrix(matrix);
        }
        reloadStorageManager();
    }

    protected void clean() {
        Map<String, IRepository> repos = repositoryHolder.getRepository();
        for (IRepository repo : repos.values()) {
            repo.destroy();
        }

        this.repositoryHolder.clear();
        this.executorMap.clear();
    }

    @Override
    protected void doDestroy() {
        try {
            clean();
        } catch (Exception ex) {
            logger.warn("", ex);
        }

        if (this.subTopologyHandlers != null) {
            for (TopologyHandler sub : this.subTopologyHandlers) {
                try {
                    sub.destroy();
                } catch (Exception ex) {
                    logger.warn("", ex);
                }
            }
        }
    }

    private void mergeMatrix(Matrix matrix) {
        synchronized (executorMap) {
            List<Group> oldGroups = this.matrix.getGroups();
            List<Group> newGroups = matrix.getGroups();
            List<Group> oldScaleOutGroups = this.matrix.getScaleOutGroups();
            List<Group> newScaleOutGroups = matrix.getScaleOutGroups();
            this.matrix.setName(matrix.getName());
            this.matrix.setSchemaName(matrix.getSchemaName());
            this.matrix.setGroups(matrix.getGroups());
            this.matrix.setScaleOutGroups(matrix.getScaleOutGroups());
            this.matrix.setProperties(matrix.getProperties());
            for (Matrix sub : matrix.getSubMatrixs()) {
                if (!matrix.getSubMatrixs().contains(sub)) {
                    matrix.addSubMatrix(sub);
                }
            }
            Set<String> newGrpNameSet = new HashSet<>();
            for (int i = 0; i < newGroups.size(); i++) {
                newGrpNameSet.add(newGroups.get(i).getName().toUpperCase());
            }
            for (int i = 0; i < newScaleOutGroups.size(); i++) {
                newGrpNameSet.add(newScaleOutGroups.get(i).getName().toUpperCase());
            }

            List<Group> grpListToClose = new ArrayList<>();
            for (Group oldGroup : oldGroups) {
                boolean found = newGrpNameSet.contains(oldGroup.getName().toUpperCase());
                if (!found) {
                    // 关闭老的group
                    grpListToClose.add(oldGroup);
                }
            }
            for (Group oldGroup : oldScaleOutGroups) {
                boolean found = newGrpNameSet.contains(oldGroup.getName().toUpperCase());
                if (!found) {
                    // 关闭老的scale out group
                    grpListToClose.add(oldGroup);
                }
            }
            loadAllTransGroupList();
            for (Group oldGroup : grpListToClose) {
                // 关闭老的group
                IGroupExecutor executor = executorMap.remove(oldGroup.getName().toUpperCase());
                IRepository repo =
                    repositoryHolder.getOrCreateRepository(oldGroup, matrix.getProperties(), connProperties);
                try {
                    repo.invalidateGroupExecutor(oldGroup);
                    if (executor != null) {
                        executor.destroy();
                    }
                } catch (Throwable e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * 指定Group配置，创建一个GroupExecutor
     */
    public IGroupExecutor createOne(Group group) {
        synchronized (executorMap) {
            group.setAppName(this.appName);
            group.setSchemaName(this.schemaName);
            group.setUnitName(this.unitName);
            IRepository repo = repositoryHolder.getOrCreateRepository(group, matrix.getProperties(), connProperties);

            IGroupExecutor groupExecutor = repo.getGroupExecutor(group);
            putOne(group.getName(), groupExecutor);
            return groupExecutor;
        }
    }

    /**
     * 添加指定groupKey的GroupExecutor，返回之前已有的
     */
    protected IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor) {
        return putOne(groupKey, groupExecutor, true);
    }

    protected IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor, boolean singleton) {
        if (singleton && executorMap.containsKey(groupKey.toUpperCase())) {
            throw new IllegalArgumentException("group key is already exists . group key : " + groupKey + " . map "
                + executorMap);
        }
        return executorMap.put(groupKey.toUpperCase(), groupExecutor);
    }

    public IGroupExecutor get(String key) {

        String keyUpperCase = key.toUpperCase();
        IGroupExecutor groupExecutor = executorMap.get(keyUpperCase);
        if (groupExecutor == null) {
            for (TopologyHandler sub : this.subTopologyHandlers) {
                groupExecutor = sub.get(key);

                if (groupExecutor != null) {
                    return groupExecutor;
                }
            }
            Group group = matrix.getGroup(keyUpperCase);
            if (group != null) {
                synchronized (executorMap) {
                    // double-check，避免并发创建
                    groupExecutor = executorMap.get(keyUpperCase);
                    if (groupExecutor == null) {
                        return createOne(group);
                    } else {
                        return executorMap.get(keyUpperCase);
                    }
                }
            }
        }
        return groupExecutor;
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

    public class TopologyGmsListener implements ConfigListener {

        protected String schemaName;

        public TopologyGmsListener(String schemaName) {
            this.schemaName = schemaName;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LoggerInit.TDDL_DYNAMIC_CONFIG.info("[Data Recieved] [MATRIX TOPOLOGY] update topology for " + dataId);
            refresh();
        }
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public Matrix getMatrix() {
        return this.matrix;
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

    public Map<String, RepoInst> getRepoInstMaps(int instType) {

        Map<String, RepoInst> repoInstMaps = new HashMap<String, RepoInst>();
        for (Group group : matrix.getGroups()) {
            IGroupExecutor groupExecutor = get(group.getName());

            TGroupDataSource ds = (TGroupDataSource) groupExecutor.getDataSource();

            // 整理atom的权重信息
            Map<TAtomDataSource, Weight> atomDsWeights = ds.getAtomDataSourceWeights();
            for (Map.Entry<TAtomDataSource, Weight> atomWeight : atomDsWeights.entrySet()) {
                TAtomDataSource atomDs = atomWeight.getKey();
                Weight weight = atomWeight.getValue();

                int w = weight.w;
                int r = weight.r;

                RepoInst repoInst = new RepoInst();
                repoInst.setWriteWeight(w);
                repoInst.setReadWeight(r);

                String addr = atomDs.getHost() + ':' + atomDs.getPort();
                repoInst.setAddress(addr);

                if (repoInstMaps.containsKey(addr)) {
                    continue;
                }

                if (instType == RepoInst.REPO_INST_TYPE_MASTER && w > 0) {
                    repoInstMaps.put(addr, repoInst);
                } else if (instType == RepoInst.REPO_INST_TYPE_SLAVE && w == 0 && r > 0) {
                    repoInstMaps.put(addr, repoInst);
                } else {
                    repoInstMaps.put(addr, repoInst);
                }
            }
        }

        return repoInstMaps;
    }

    public Map<String, RepoInst> getGroupRepoInstMapsForzigzig() {
        Map<String, RepoInst> groupRepoInstMaps = new HashMap<String, RepoInst>();

        List<Group> allGroupsInMatrix = new ArrayList<>();
        allGroupsInMatrix.addAll(matrix.getGroups());
        for (Group group : allGroupsInMatrix) {
            IGroupExecutor groupExecutor = get(group.getName());
            TGroupDataSource ds = (TGroupDataSource) groupExecutor.getDataSource();
            String groupName = ds.getDbGroupKey();
            String address = ds.getOneAtomAddress(ConfigDataMode.isMasterMode());
            RepoInst repoInst = new RepoInst();
            repoInst.setAddress(address);
            groupRepoInstMaps.put(groupName, repoInst);
        }
        return groupRepoInstMaps;
    }

    public List<String> getGroupNames() {
        List<String> groupNames = new ArrayList<>(matrix.getGroups().size());
        for (Group group : matrix.getGroups()) {
            IGroupExecutor groupExecutor = get(group.getName());
            Object o = groupExecutor.getDataSource();
            if (o instanceof TGroupDataSource) {
                groupNames.add(group.getName());
            }
        }
        return groupNames;
    }

    protected Matrix initPolarDbXTopology() {
        String appName = this.appName;
        String dbName = this.schemaName;

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            dbGroupInfoAccessor.setConnection(conn);
            Matrix matrix = new Matrix();
            matrix.setSchemaName(dbName);
            matrix.setName(appName);

            List<String> normalGroupNameList = new ArrayList<>();
            List<String> normalGroupNameListForPartitionTables = new ArrayList<>();
            List<String> scaleOutGroupNameList = new ArrayList<>();
            Map<String, Group> grpNameAndGrpMap = new HashMap<>();
            List<Group> groups = new ArrayList<>();
            List<Group> scaleOutGroups = new ArrayList<>();
            List<Group> partitionTableGroups = new ArrayList<>();
            List<DbGroupInfoRecord> dbGroupInfoRecords = dbGroupInfoAccessor.queryDbGroupByDbName(dbName);
            String groupsContent = "";
            for (int i = 0; i < dbGroupInfoRecords.size(); i++) {
                DbGroupInfoRecord dbGroupInfoRecord = dbGroupInfoRecords.get(i);
                int groupType = dbGroupInfoRecord.groupType;
                Group group = new Group();
                group.setName(dbGroupInfoRecord.groupName);
                group.setSchemaName(dbGroupInfoRecord.dbName);
                group.setAppName(appName);
                String grpNameUpperCase = group.getName().toUpperCase();
                if (groupType == DbGroupInfoRecord.GROUP_TYPE_NORMAL) {
                    normalGroupNameList.add(grpNameUpperCase);
                } else if (groupType == DbGroupInfoRecord.GROUP_TYPE_ADDED
                    || groupType == DbGroupInfoRecord.GROUP_TYPE_SCALEOUT_FINISHED) {
                    scaleOutGroupNameList.add(grpNameUpperCase);
                } else if (groupType == DbGroupInfoRecord.GROUP_TYPE_ADDING
                    || groupType == DbGroupInfoRecord.GROUP_TYPE_REMOVING) {
                    continue;
                }

                if (!groupsContent.isEmpty()) {
                    groupsContent += ",";
                }
                groupsContent += grpNameUpperCase;
                grpNameAndGrpMap.put(grpNameUpperCase, group);
            }
            GroupInfoUtil.sortGroupNames(normalGroupNameList);
            GroupInfoUtil.sortGroupNames(scaleOutGroupNameList);
            for (int i = 0; i < normalGroupNameList.size(); i++) {
                groups.add(grpNameAndGrpMap.get(normalGroupNameList.get(i)));
            }
            for (int i = 0; i < normalGroupNameListForPartitionTables.size(); i++) {
                partitionTableGroups.add(grpNameAndGrpMap.get(normalGroupNameListForPartitionTables.get(i)));
            }
            for (int i = 0; i < scaleOutGroupNameList.size(); i++) {
                scaleOutGroups.add(grpNameAndGrpMap.get(scaleOutGroupNameList.get(i)));
            }
            matrix.setGroups(groups);
            matrix.setScaleOutGroups(scaleOutGroups);
            matrix.setSubMatrixs(new ArrayList<>());
            LoggerInit.TDDL_DYNAMIC_CONFIG
                .info(String.format(" newGrpList of appName[%s] is [%s]", appName, groupsContent));
            return matrix;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

    }

    public List<String> getAllTransGroupList() {
        return allTransGroupList;
    }

    protected void loadAllTransGroupList() {
        List<Group> groups = matrix.getGroups();
        List<Group> scaleGroups = matrix.getScaleOutGroups();
        List<String> groupNames = new ArrayList<>(groups.size() + scaleGroups.size());
        for (Group group : groups) {
            if (group.getType().isMysql()) {
                groupNames.add(group.getName());
            } else {
                String logMsg = "Ignored non-mysql group: " + group.getName();
                logger.warn(logMsg);
                LoggerInit.TDDL_DYNAMIC_CONFIG.warn(logMsg);
            }
        }
        for (Group group : scaleGroups) {
            if (group.getType().isMysql()) {
                groupNames.add(group.getName());
            } else {
                String logMsg = "Ignored non-mysql group: " + group.getName();
                logger.warn(logMsg);
                LoggerInit.TDDL_DYNAMIC_CONFIG.warn(logMsg);
            }
        }

        Collections.sort(groupNames);

        String logMsg = "All Trans Groups: " + StringUtils.join(groupNames, ", ");
        if (logger.isInfoEnabled()) {
            logger.info(logMsg);
        }
        LoggerInit.TDDL_DYNAMIC_CONFIG.info(logMsg);

        this.allTransGroupList = groupNames;

        if (topologyChanger != null) {
            topologyChanger.onTopology(this);
        }

    }

    public void setTopologyChanger(TopologyChanger topologyChanger) {
        this.topologyChanger = topologyChanger;
    }

}
