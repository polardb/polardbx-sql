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

package com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class LocalityTestCaseValidator {
    private static final Logger logger = LoggerFactory.getLogger(LocalityTestCaseValidator.class);

    public static abstract class Checker {
        public boolean check(LocalityTestUtils.LocalityBean localityBean) {
            return true;
        }

        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
        }

        @Override
        public String toString() {
            return "";
        }
    }

    public static class Selector {
        public boolean where(LocalityTestUtils.LocalityBean localityBean) {
            return true;
        }

        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
        }

        @Override
        public String toString() {
            return "";
        }
    }

    public static class SelectByObjectTypeAndName extends LocalityTestCaseValidator.Selector {
        String type;
        String name;

        SelectByObjectTypeAndName(String type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
            this.name = substitueAction.substitue(this.name);
        }

        @Override
        public boolean where(LocalityTestUtils.LocalityBean localityBean) {
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
        public boolean where(LocalityTestUtils.LocalityBean localityBean) {
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
        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
            this.localitySql = substitueAction.substitue(this.localitySql);
        }

        @Override
        public boolean check(LocalityTestUtils.LocalityBean localityBean) {
            LocalityDesc localityDesc = LocalityDesc.parse(localityBean.locality);
            LocalityDesc compareLocalityDesc = LocalityDesc.parse("dn=" + localitySql);
            if (localitySql.startsWith(LocalityDesc.BALANCE_PREFIX)) {
                compareLocalityDesc = LocalityDesc.parse(localitySql);
            }
            if (localityDesc.getBalanceSingleTable()) {
                return compareLocalityDesc.getBalanceSingleTable();
            }
            if (localityDesc.holdEmptyDnList() && compareLocalityDesc.holdEmptyDnList()) {
                return true;
            } else {
                return localityDesc.toString().equalsIgnoreCase(compareLocalityDesc.toString());
            }
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
        public boolean check(LocalityTestUtils.LocalityBean localityBean) {
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
        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
            this.localitySql = substitueAction.substitue(localitySql);
        }

        @Override
        public String toString() {
            return String.format("Checker{groupName='%s', locality='%s'}", this.groupName, this.localitySql);
        }
    }

    public static class TableTopologyCheckAction {
        public String tableName;
        public String tableTopology;
        public Map<String, String> partitionTopology = new HashMap<>();

        public TableTopologyCheckAction() {
        }

        public void apply(LocalitySingleTaskCaseTask.SubstitueAction substitueAction) {
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

        boolean check(LocalityTestUtils.TopologyBean topologyBean) {
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
        Map<String, List<LocalityTestUtils.TopologyBean>> tableTopology;
    }

    public static Map<String, List<LocalityTestUtils.TopologyBean>> getTopologyInfo(Connection tddlConnection,
                                                                                    String dbName,
                                                                                    List<String> tableNameList) {
        List<String> storageIds = LocalityTestUtils.getDatanodes(tddlConnection);
        final String showGroupInfoSql =
            String.format("SELECT * FROM metadb.group_detail_info where db_name = \"%s\"", dbName);
        Map<String, String> groupStorageIdMap = new HashMap<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showGroupInfoSql)) {
            while (result.next()) {
                String group = result.getString(6);
                String storageId = result.getString(7);
                if (storageIds.contains(storageId)) {
                    groupStorageIdMap.put(group, storageId);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        final String showTopologySql = "SHOW TOPOLOGY ";
        Map<String, List<LocalityTestUtils.TopologyBean>> res = new HashMap<>();
        for (String tableName : tableNameList) {
            List<LocalityTestUtils.TopologyBean> topologyBeans = new ArrayList<>();
            try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showTopologySql + tableName)) {
                while (result.next()) {
                    String partition = result.getString(4);
                    String group = result.getString(2);
                    LocalityTestUtils.TopologyBean topologyBean =
                        new LocalityTestUtils.TopologyBean(partition, group, groupStorageIdMap.get(group));
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

    public static List<LocalityTestUtils.LocalityBean> getLocalityInfo(Connection tddlConnection) {
        final String sql = "/*TDDL:CMD_EXTRA(SHOW_FULL_LOCALITY=true)*/ show locality";

        List<LocalityTestUtils.LocalityBean> res = new ArrayList<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String objectType = result.getString("OBJECT_TYPE");
                String objectName = result.getString("OBJECT_NAME");
                String locality = result.getString("LOCALITY");
                String groupElement = result.getString("OBJECT_GROUP_ELEMENT");
                LocalityTestUtils.LocalityBean
                    bean = new LocalityTestUtils.LocalityBean(objectType, objectName, locality, groupElement);
                res.add(bean);
            }
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkLocalityInfo(Map<Selector, Checker> selectForCheckMap,
                                         List<LocalityTestUtils.LocalityBean> localityBeanItems) {
        Map<Selector, Boolean> selectSucceed = new HashMap<>();
        Map<Selector, Boolean> selectFound = new HashMap<>();
        Map<Selector, LocalityTestUtils.LocalityBean> failedValue = new HashMap<>();
        for (Selector selector : selectForCheckMap.keySet()) {
            selectSucceed.put(selector, false);
            selectFound.put(selector, false);
        }
        for (LocalityTestUtils.LocalityBean localityBean : localityBeanItems) {
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
                logger.error(String.format("the value is %s", failedValue.get(selector).toString()));
            }
        }
        Assert.assertEquals(0, failedSelector.size());
    }

    public static void checkTopologyInfo(Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap,
                                         Map<String, List<LocalityTestUtils.TopologyBean>> showTopologyResult) {
        Map<String, Boolean> succeedTable = new HashMap<>();
        tableTopologyCheckActionMap.keySet().forEach(tableName -> succeedTable.put(tableName, false));
        Map<String, LocalityTestUtils.TopologyBean> failedValue = new HashMap<>();
        for (Map.Entry<String, TableTopologyCheckAction> tableTopologyCheckActionEntry : tableTopologyCheckActionMap.entrySet()) {
            String tableName = tableTopologyCheckActionEntry.getKey();
            TableTopologyCheckAction tableTopologyCheckAction = tableTopologyCheckActionEntry.getValue();
            List<LocalityTestUtils.TopologyBean> topologyBeans = showTopologyResult.get(tableName);
            succeedTable.put(tableName, true);
            for (LocalityTestUtils.TopologyBean topologyBean : topologyBeans) {
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
