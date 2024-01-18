package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.junit.Assert;

import javax.xml.crypto.Data;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StoragePoolTestCaseValidator {
    private static final Logger logger = LoggerFactory.getLogger(StoragePoolTestCaseValidator.class);

    public static abstract class Checker {
        public boolean check(StoragePoolTestUtils.LocalityBean localityBean) {
            return true;
        }

        public void apply(
            StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
        }

        @Override
        public String toString() {
            return "";
        }
    }

    public static class Selector {
        public boolean where(StoragePoolTestUtils.LocalityBean localityBean) {
            return true;
        }

        public void apply(
            StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
        }

        @Override
        public String toString() {
            return "";
        }
    }

    public static class SelectByObjectTypeAndName extends Selector {
        String type;
        String name;

        SelectByObjectTypeAndName(String type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public void apply(
            StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.name = substitueAction.substitue(this.name);
        }

        @Override
        public boolean where(StoragePoolTestUtils.LocalityBean localityBean) {
            return this.type.equals(localityBean.objectType) && this.name.equals(localityBean.objectName);
        }

        @Override
        public String toString() {
            return String.format("Selector{type='%s', name='%s'}", this.type, this.name);
        }
    }

    public static class SelectByObjectTypeAndGroupElement extends Selector {
        String type;
        List<String> groupElements;

        SelectByObjectTypeAndGroupElement(String objectType, List<String> groupElements) {
            this.type = objectType;
            this.groupElements = groupElements;
        }

        @Override
        public boolean where(StoragePoolTestUtils.LocalityBean localityBean) {
            if (type.equals(localityBean.objectType)) {
                List<String> groupElements =
                    Arrays.stream(localityBean.groupElement.split(",")).collect(Collectors.toList());
                return groupElements.containsAll(this.groupElements);
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("Selector{type='%s', groupElement='%s'}", this.type,
                String.join(",", this.groupElements));
        }
    }

    public static class CheckItemValue extends Checker {
        String localitySql;

        CheckItemValue(String sql) {
            this.localitySql = sql;
        }

        @Override
        public void apply(
            StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.localitySql = substitueAction.substitue(this.localitySql);
        }

        @Override
        public boolean check(StoragePoolTestUtils.LocalityBean localityBean) {
            LocalityDesc localityDesc = LocalityDesc.parse(localityBean.locality);
            LocalityDesc compareLocalityDesc = LocalityDesc.parse(localitySql);
//            return localityDesc.compactiableWith(compareLocalityDesc) && compareLocalityDesc.compactiableWith( localityDesc);
            return localityDesc.toString().equalsIgnoreCase(compareLocalityDesc.toString());
        }
    }

    public static class CheckItemValueAndCollectName extends Checker {
        String localitySql;
        String groupName;
        Map.Entry<String, String> collectedName;

        CheckItemValueAndCollectName(String sql, String groupName) {
            this.groupName = groupName;
            this.localitySql = sql;
        }

        @Override
        public boolean check(StoragePoolTestUtils.LocalityBean localityBean) {
            LocalityDesc localityDesc = LocalityDesc.parse(localityBean.locality);
            LocalityDesc compareLocalityDesc = LocalityDesc.parse("dn=" + localitySql);
            if (localityDesc.compactiableWith(compareLocalityDesc) && compareLocalityDesc.compactiableWith(
                localityDesc)) {
                this.collectedName = new AbstractMap.SimpleEntry<>(this.groupName, localityBean.objectName);
                return true;
            }
            return false;
        }

        @Override
        public void apply(
            StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.localitySql = substitueAction.substitue(localitySql);
        }

        @Override
        public String toString() {
            return String.format("Checker{groupName='%s', locality='%s'}", this.groupName, this.localitySql);
        }
    }

    public static class StoragePoolValueCheckAction {
        public String storagePool;
        public List<String> dnIds;

        public String undeletableDn;

        public StoragePoolValueCheckAction() {
        }

        public StoragePoolValueCheckAction(String storagePool, List<String> dnIds, String undeletableDnId) {
            this.storagePool = storagePool;
            this.dnIds = dnIds;
            this.undeletableDn = undeletableDnId;
        }

        public void apply(StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.dnIds = this.dnIds.stream().map(o -> substitueAction.substitue(o)).collect(Collectors.toList());
            this.undeletableDn = substitueAction.substitue(this.undeletableDn);
        }

        boolean check(StoragePoolTestUtils.StoragePoolBean spBean) {
            if (this.dnIds.containsAll(spBean.dnIds) && spBean.dnIds.containsAll(this.dnIds)
                && this.undeletableDn.equalsIgnoreCase(spBean.undeletableDn)) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("Checker{storagePool='%s', dnIds='%s'}", this.storagePool, this.dnIds);
        }
    }

    public static class DatasourceCheckAction {
        public String database;
        public List<String> dnIds;

        public DatasourceCheckAction() {
        }

        public DatasourceCheckAction(String database, List<String> dnIds) {
            this.database = database;
            this.dnIds = dnIds;
        }

        public void apply(StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.dnIds = this.dnIds.stream().map(o -> substitueAction.substitue(o)).collect(Collectors.toList());
        }

        boolean check(StoragePoolTestUtils.DsBean dsBean) {
            if (this.dnIds.isEmpty()) {
                return true;
            } else if (this.dnIds.contains(dsBean.dnId)) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("Checker{database='%s', dnIds='%s'}", this.database, this.dnIds);
        }
    }

    public static class TableTopologyCheckAction {
        public String tableName;
        public String tableTopology;
        public Map<String, String> partitionTopology = new HashMap<>();

        public TableTopologyCheckAction() {
        }

        public void apply(StoragePoolUnitTaskCaseTask.SubstitueAction substitueAction) {
            this.tableTopology = substitueAction.substitue(this.tableTopology);
            for (Map.Entry<String, String> partitionTopologyItem : partitionTopology.entrySet()) {
                String partition = partitionTopologyItem.getKey();
                String topology = substitueAction.substitue(partitionTopologyItem.getValue());
                partitionTopology.put(partition, topology);
            }
        }

        public TableTopologyCheckAction(String tableName, String tableTopology) {
            this.tableName = tableName;
            this.tableTopology = tableTopology;
        }

        boolean check(StoragePoolTestUtils.TopologyBean topologyBean) {
            LocalityDesc localityDesc = new LocalityDesc();
            if (partitionTopology.containsKey(topologyBean.partitionName)) {
                localityDesc = LocalityDesc.parse("dn=" + partitionTopology.get(topologyBean.partitionName));
            }
            if (localityDesc.holdEmptyDnList()) {
                localityDesc = LocalityDesc.parse("dn=" + tableTopology);
            }
            return localityDesc.matchStorageInstance(topologyBean.storageId);
        }

        @Override
        public String toString() {
            return String.format("Checker{tableName='%s', tableTopology='%s'}", this.tableName, this.tableTopology);
        }
    }

    public static class TableTopologyResult {
        Map<String, List<StoragePoolTestUtils.TopologyBean>> tableTopology;
    }

    public static Map<String, List<StoragePoolTestUtils.DsBean>> getDsInfo(Connection tddlConnection) {
        final String showDsSql = String.format("show ds");
        Map<String, List<StoragePoolTestUtils.DsBean>> results = new HashMap<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showDsSql)) {
            while (result.next()) {
                String dnId = result.getString("STORAGE_INST_ID");
                String dbName = result.getString("DB");
                String phyDbName = result.getString("PHY_DB");
                if (results.containsKey(dbName)) {
                    results.get(dbName).add(new StoragePoolTestUtils.DsBean(dnId, phyDbName, dbName));
                } else {
                    results.put(dbName, Lists.newArrayList(new StoragePoolTestUtils.DsBean(dnId, phyDbName, dbName)));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    public static Map<String, StoragePoolTestUtils.StoragePoolBean> getStoragePoolInfo(Connection tddlConnection) {
        final String showStoragePoolSql = String.format("select * from information_schema.storage_pool_info");
        Map<String, StoragePoolTestUtils.StoragePoolBean> results = new HashMap<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showStoragePoolSql)) {
            while (result.next()) {
                String storagePool = result.getString("NAME");
                String dnIds = result.getString("DN_ID_LIST");
                String undeletableDnId = result.getString("UNDELETABLE_DN_ID");
                List<String> dnIdList = Arrays.stream(dnIds.split(",")).collect(Collectors.toList());
                if (StringUtils.isEmpty(dnIds)) {
                    dnIdList = new ArrayList<>();
                }
                results.put(storagePool,
                    new StoragePoolTestUtils.StoragePoolBean(dnIdList, storagePool, undeletableDnId));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    public static Map<String, List<StoragePoolTestUtils.TopologyBean>> getTopologyInfo(Connection tddlConnection,
                                                                                       String dbName,
                                                                                       List<String> tableNameList) {
        final String showTopologySql = "SHOW TOPOLOGY ";
        Map<String, List<StoragePoolTestUtils.TopologyBean>> res = new HashMap<>();
        for (String tableName : tableNameList) {
            List<StoragePoolTestUtils.TopologyBean> topologyBeans = new ArrayList<>();
            try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showTopologySql + tableName)) {
                while (result.next()) {
                    String partition = result.getString("PARTITION_NAME");
                    String group = result.getString("GROUP_NAME");
                    String storageInstId = result.getString("DN_ID");
                    StoragePoolTestUtils.TopologyBean topologyBean =
                        new StoragePoolTestUtils.TopologyBean(partition, group, storageInstId);
                    topologyBeans.add(topologyBean);
                }
                res.put(tableName, topologyBeans);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
//        LOG.info("getStorageInfo: " + res);
        }
        return res;
    }

    public static List<StoragePoolTestUtils.LocalityBean> getLocalityInfo(Connection tddlConnection, String dbName) {
        final String showLocalitySql = "show locality";
        final String useSql = "use " + dbName;

        List<StoragePoolTestUtils.LocalityBean> res = new ArrayList<>();
        JdbcUtil.executeQuery(useSql, tddlConnection);
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showLocalitySql)) {
            while (result.next()) {
                String objectType = result.getString("OBJECT_TYPE");
                String objectName = result.getString("OBJECT_NAME");
                String locality = result.getString("LOCALITY");
                String groupElement = result.getString("OBJECT_GROUP_ELEMENT");
                if (objectType.equalsIgnoreCase("table")) {
                    objectName = dbName + "." + objectName;
                }
                if (!StringUtils.isEmpty(groupElement)) {
                    List<String> tableNames =
                        Arrays.stream(groupElement.split(",")).map(tableName -> dbName + "." + tableName)
                            .collect(Collectors.toList());
                    groupElement = StringUtil.join(",", tableNames).toString();
                }
                StoragePoolTestUtils.LocalityBean
                    bean = new StoragePoolTestUtils.LocalityBean(objectType, objectName, locality, groupElement);
                res.add(bean);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public static void checkLocalityInfo(Map<Selector, Checker> selectForCheckMap,
                                         List<StoragePoolTestUtils.LocalityBean> localityBeanItems) {
        Map<Selector, Boolean> selectSucceed = new HashMap<>();
        Map<Selector, Boolean> selectFound = new HashMap<>();
        Map<Selector, StoragePoolTestUtils.LocalityBean> failedValue = new HashMap<>();
        for (Selector selector : selectForCheckMap.keySet()) {
            selectSucceed.put(selector, false);
            selectFound.put(selector, false);
        }
        if (localityBeanItems == null || localityBeanItems.isEmpty()) {
            return;
        }
        for (StoragePoolTestUtils.LocalityBean localityBean : localityBeanItems) {
            for (Selector selector : selectForCheckMap.keySet()) {
                if (selector.where(localityBean)) {
                    Checker checkItemAction = selectForCheckMap.get(selector);
                    Assert.assertEquals(selectFound.get(selector), false);
                    if (checkItemAction.check(localityBean)) {
                        selectSucceed.put(selector, true);
                    } else {
                        selectSucceed.put(selector, false);
                        failedValue.put(selector, localityBean);
                    }
                    selectFound.put(selector, true);
                }
            }
        }
        List<Selector> failedSelector =
            selectSucceed.keySet().stream().filter(o -> !selectSucceed.get(o)).collect(Collectors.toList());
        Set<Selector> ignoreSelector = new HashSet<>();
        for (Selector selector : failedSelector) {
            if (selectForCheckMap.get(selector) instanceof CheckItemValue) {
                String localitySql = ((CheckItemValue) selectForCheckMap.get(selector)).localitySql;
                if (StringUtils.isEmpty(localitySql) && !selectFound.get(selector)) {
                    ignoreSelector.add(selector);
                }
            }
        }
        failedSelector = failedSelector.stream().filter(o -> !ignoreSelector.contains(o)).collect(Collectors.toList());
        if (failedSelector.size() > 0) {
            for (Selector selector : failedSelector) {
                logger.error(String.format("check item %s locality", selector.toString()));
                logger.error(String.format("expected %s", selectForCheckMap.get(selector).toString()));
                if (failedValue.get(selector) != null) {
                    logger.error(String.format("the value is %s",
                        failedValue.get(selector).toString()));
                }
            }
        }
        Assert.assertEquals(0, failedSelector.size());
    }

    public static void checkDsInfo(Map<String, DatasourceCheckAction> datasourceCheckActionMap,
                                   Map<String, List<StoragePoolTestUtils.DsBean>> showDsResult) {
        Map<String, Boolean> succeedDb = new HashMap<>();
        datasourceCheckActionMap.keySet().forEach(tableName -> succeedDb.put(tableName, false));
        Map<String, StoragePoolTestUtils.DsBean> failedValue = new HashMap<>();
        for (Map.Entry<String, DatasourceCheckAction> datasourceCheckActionEntry : datasourceCheckActionMap.entrySet()) {
            String dbName = datasourceCheckActionEntry.getKey();
            DatasourceCheckAction datasourceCheckAction = datasourceCheckActionEntry.getValue();
            List<StoragePoolTestUtils.DsBean> dsBeans = showDsResult.get(dbName);
            succeedDb.put(dbName, true);
            for (StoragePoolTestUtils.DsBean dsBean : dsBeans) {
                if (!datasourceCheckAction.check(dsBean)) {
                    succeedDb.put(dbName, false);
                    failedValue.put(dbName, dsBean);
                }
            }
        }
        List<String> failedDb = succeedDb.entrySet().stream().filter(o -> !o.getValue()).map(Map.Entry::getKey)
            .collect(Collectors.toList());
        if (failedDb.size() > 0) {
            for (String dbName : failedDb) {
                logger.error(String.format("check item %s ds", dbName));
                logger.error(String.format("expected %s", datasourceCheckActionMap.get(dbName).dnIds));
                logger.error(String.format("the value is %s", failedValue.get(dbName).toString()));
            }
        }
        Assert.assertEquals(0, failedDb.size());
    }

    public static void checkStoragePoolInfo(Map<String, StoragePoolValueCheckAction> storagePoolValueCheckActionMap,
                                            Map<String, StoragePoolTestUtils.StoragePoolBean> showStoragePoolResults) {
        Map<String, Boolean> succeedStoragePool = new HashMap<>();
        storagePoolValueCheckActionMap.keySet().forEach(tableName -> succeedStoragePool.put(tableName, false));
        Map<String, StoragePoolTestUtils.StoragePoolBean> failedValue = new HashMap<>();
        for (Map.Entry<String, StoragePoolValueCheckAction> storagePoolValueCheckActionEntry : storagePoolValueCheckActionMap.entrySet()) {
            String storagePoolName = storagePoolValueCheckActionEntry.getKey();
            StoragePoolValueCheckAction storagePoolValueCheckAction = storagePoolValueCheckActionEntry.getValue();
            StoragePoolTestUtils.StoragePoolBean storagePoolBean = showStoragePoolResults.get(storagePoolName);
            succeedStoragePool.put(storagePoolName, true);
            if (!storagePoolValueCheckAction.check(storagePoolBean)) {
                succeedStoragePool.put(storagePoolName, false);
                failedValue.put(storagePoolName, storagePoolBean);
            }
        }
        List<String> failedStoragePool =
            succeedStoragePool.entrySet().stream().filter(o -> !o.getValue()).map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (failedStoragePool.size() > 0) {
            for (String storagePool : failedStoragePool) {
                logger.error(String.format("check item %s storage pool", storagePool));
                logger.error(String.format("expected %s %s, ", storagePoolValueCheckActionMap.get(storagePool).dnIds,
                    storagePoolValueCheckActionMap.get(storagePool).undeletableDn));
                logger.error(String.format("the value is %s", failedValue.get(storagePool).toString()));
            }
        }
        Assert.assertEquals(0, failedStoragePool.size());
    }

    public static void checkTopologyInfo(Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap,
                                         Map<String, List<StoragePoolTestUtils.TopologyBean>> showTopologyResult) {
        Map<String, Boolean> succeedTable = new HashMap<>();
        tableTopologyCheckActionMap.keySet().forEach(tableName -> succeedTable.put(tableName, false));
        Map<String, StoragePoolTestUtils.TopologyBean> failedValue = new HashMap<>();
        for (Map.Entry<String, TableTopologyCheckAction> tableTopologyCheckActionEntry : tableTopologyCheckActionMap.entrySet()) {
            String tableName = tableTopologyCheckActionEntry.getKey();
            TableTopologyCheckAction tableTopologyCheckAction = tableTopologyCheckActionEntry.getValue();
            List<StoragePoolTestUtils.TopologyBean> topologyBeans = showTopologyResult.get(tableName);
            succeedTable.put(tableName, true);
            for (StoragePoolTestUtils.TopologyBean topologyBean : topologyBeans) {
                if (!tableTopologyCheckAction.check(topologyBean)) {
                    succeedTable.put(tableName, false);
                    failedValue.put(tableName, topologyBean);
                }
            }

        }
        List<String> failedTable = succeedTable.entrySet().stream().filter(o -> !o.getValue()).map(Map.Entry::getKey)
            .collect(Collectors.toList());
        if (failedTable.size() > 0) {
            for (String table : failedTable) {
                logger.error(String.format("check item %s topology", table));
                logger.error(String.format("expected %s", tableTopologyCheckActionMap.get(table).toString()));
                logger.error(String.format("the value is %s", failedValue.get(table).toString()));
            }
        }
        Assert.assertEquals(0, failedTable.size());
    }

}
