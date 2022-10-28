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

package com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestCaseValidator.*;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.schema.Table;

import static com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestCaseValidator.*;
import static java.lang.Thread.sleep;

public class LocalitySingleTaskCaseTask {

    public static Logger logger = LoggerFactory.getLogger(LocalitySingleTaskCaseTask.class);

    static private String trimString(String original) {
        original = original.trim();
        if (original.startsWith("\"") && original.endsWith("\"")) {
            original = original.substring(1, original.length() - 1);
        }
        return original;
    }

    static public LocalitySingleTaskCaseTask loadFrom(LocalityTestCaseBean.SingleTestCase singleTestCase) {
        Map<Selector, Checker> localityValueAndTableGroupMatchCheckerMap = new HashMap<>();
        LocalityTestCaseBean.CheckActions checkActions = singleTestCase.checkActions;
        localityValueAndTableGroupMatchCheckerMap.putAll(
            LocalitySingleTaskCaseTask.fromLocalityValueCheck(checkActions.localityValueCheck));
        localityValueAndTableGroupMatchCheckerMap.putAll(fromTableGroupMatchCheck(checkActions.tableGroupMatchCheck));

        Map<Selector, Checker> partitionLocalityCheckerMap = new HashMap<>();
        if (checkActions.partitionLocalityCheck != null) {
            partitionLocalityCheckerMap.putAll(
                LocalitySingleTaskCaseTask.fromPartitionLocalityCheck(checkActions.partitionLocalityCheck));
        }

        Map<String, TableTopologyCheckAction> tableTopologyCheckActions =
            LocalitySingleTaskCaseTask.fromTopologyCheck(checkActions);

        Map<String, Integer> checkTriggers = LocalitySingleTaskCaseTask.fromCheckTriggers(singleTestCase.checkTriggers);
        Map<String, String> rejectDDls = singleTestCase.getRejectDDls().stream().collect(
            Collectors.toMap(LocalityTestCaseBean.RejectDdl::getDdl, LocalityTestCaseBean.RejectDdl::getMessage));
        return new LocalitySingleTaskCaseTask(singleTestCase.prepareDDls, rejectDDls, singleTestCase.cleanupDDls,
            checkTriggers, localityValueAndTableGroupMatchCheckerMap, partitionLocalityCheckerMap,
            tableTopologyCheckActions);
    }

    static public Map<Selector, Checker> fromLocalityValueCheck(List<String> localityValueCheck) {
        Map<Selector, Checker> selectorCheckerMap = new HashMap<>();
        for (String localityItem : localityValueCheck) {
            String[] localityItemList = localityItem.split(" ");
            String objectName = localityItemList[0];
            String objectType = localityItemList[1];
            String locality = trimString(localityItemList[2]);
            Selector selector = new SelectByObjectTypeAndName(objectType, objectName);
            Checker checker = new CheckItemValue(locality);
            selectorCheckerMap.put(selector, checker);
        }
        return selectorCheckerMap;
    }

    static public Map<String, Integer> fromCheckTriggers(List<String> checkTriggers) {
        Map<String, Integer> checkTriggerMap = new HashMap<>();
        if (checkTriggers != null) {
            for (String checkTrigger : checkTriggers) {
                String[] checkTriggerElement = checkTrigger.split("###");
                String sql = checkTriggerElement[0];
                Integer expected = Integer.valueOf(checkTriggerElement[1]);
                checkTriggerMap.put(sql, expected);
            }
        }
        return checkTriggerMap;
    }

    static public Map<String, TableTopologyCheckAction> fromTopologyCheck(
        LocalityTestCaseBean.CheckActions checkActions) {
        List<String> localityValueCheck =
            (checkActions.localityValueCheck == null) ? new ArrayList<>() : checkActions.localityValueCheck;
        List<String> tableGroupMatchCheck =
            (checkActions.tableGroupMatchCheck == null) ? new ArrayList<>() : checkActions.tableGroupMatchCheck;
        List<String> topologyCheck =
            (checkActions.topologyCheck == null) ? new ArrayList<>() : checkActions.topologyCheck;
        List<String> partitionLocalityCheck =
            (checkActions.partitionLocalityCheck == null) ? new ArrayList<>() : checkActions.partitionLocalityCheck;
        Map<String, TableTopologyCheckAction> tableTopologyCheckActions = new HashMap<>();
        //auto generated from locality value check
        for (String localityItem : localityValueCheck) {
            String[] localityItemList = localityItem.split(" ");
            String objectName = localityItemList[0];
            String objectType = localityItemList[1];
            String locality = trimString(localityItemList[2]);
            if (objectType.equals("table")) {
                tableTopologyCheckActions.put(objectName, new TableTopologyCheckAction(objectName, locality));
            }
        }
        Map<String, List<String>> tableGroup2Tables = new HashMap<>();
        for (String tableGroupMatchCheckItem : tableGroupMatchCheck) {
            String[] tableGroupMatchCheckItemList = tableGroupMatchCheckItem.split(" ");
            String objectName = tableGroupMatchCheckItemList[0];
            String groupName = tableGroupMatchCheckItemList[1];
            String[] tableNames = objectName.split(",");
            tableGroup2Tables.put(groupName, Arrays.stream(tableNames).collect(Collectors.toList()));
        }

        //auto generated from partition locality check
        for (String partitionGroupLocalityCheckItem : partitionLocalityCheck) {
            String[] localityItemList = partitionGroupLocalityCheckItem.split(" ");
            String tableGroupAndPartitionGroupName = localityItemList[0];
            String locality = trimString(localityItemList[1]);
            String[] groupNames = tableGroupAndPartitionGroupName.split("\\.");
            String tableGroupName = groupNames[0];
            String partitionName = groupNames[1];
            List<String> tableNames = tableGroup2Tables.get(tableGroupName);
            for (String tableName : tableNames) {
                tableTopologyCheckActions.get(tableName).partitionTopology.put(partitionName, locality);
            }
        }

        //override by self-config topology item
        for (String topologyItem : topologyCheck) {
            String[] topologyItemList = topologyItem.split(" ");
            String objectName = topologyItemList[0];
            String objectType = topologyItemList[1];
            String locality = trimString(topologyItemList[2]);
            if (objectType.equals("partition")) {
                String[] tableNameAndPartition = objectName.split("\\.");
                String partitionName = tableNameAndPartition[1];
                String[] tableNames = tableNameAndPartition[0].split(",");
                for (String tableName : tableNames) {
                    tableTopologyCheckActions.get(tableName).partitionTopology.put(partitionName, locality);
                }
            } else if (objectType.equals("table")) {
                if (tableTopologyCheckActions.containsKey(objectName)) {
                    tableTopologyCheckActions.get(objectName).tableTopology = locality;
                } else {
                    tableTopologyCheckActions.put(objectName, new TableTopologyCheckAction(objectName, locality));
                }
            }
        }
        return tableTopologyCheckActions;
    }

    static public Map<Selector, Checker> fromTableGroupMatchCheck(List<String> tableGroupMatchCheck) {
        Map<Selector, Checker> selectorCheckerMap = new HashMap<>();
        String objectType = "tablegroup";
        for (String localityItem : tableGroupMatchCheck) {
            String[] localityItemList = localityItem.split(" ");
            String tableNames = localityItemList[0];
            String tableGroupName = localityItemList[1];
            String locality = trimString(localityItemList[2]);
            List<String> tableNameList = Arrays.stream(tableNames.split(",")).collect(Collectors.toList());
            Selector selector = new SelectByObjectTypeAndGroupElement(objectType, tableNameList);
            Checker checker = new CheckItemValueAndCollectName(locality, tableGroupName);
            selectorCheckerMap.put(selector, checker);
        }
        return selectorCheckerMap;
    }

    static public Map<Selector, Checker> fromPartitionLocalityCheck(List<String> partitionLocalityCheck) {
        Map<Selector, Checker> selectorCheckerMap = new HashMap<>();
        String objectType = "partitiongroup";
        for (String localityItem : partitionLocalityCheck) {
            String[] localityItemList = localityItem.split(" ");
            String partitionGroupName = localityItemList[0];
            String locality = trimString(localityItemList[1]);
            Selector selector = new SelectByObjectTypeAndName(objectType, partitionGroupName);
            Checker checker = new CheckItemValue(locality);
            selectorCheckerMap.put(selector, checker);
        }
        return selectorCheckerMap;
    }

    public List<String> prepareDDls;
    public List<String> cleanupDDls;
    public Map<String, String> rejectDDls;
    public String dbName;
    Map<String, Integer> checkTriggers;
    Map<Selector, Checker> firstPassSelectCheckMap;
    Map<Selector, Checker> secondPassSelectCheckMap;
    Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap;
    Map<String, String> groupNameMap;

    LocalitySingleTaskCaseTask(List<String> prepareDDls, Map<String, String> rejectDDls, List<String> cleanupDDls,
                               Map<String, Integer> checkTriggers, Map<Selector, Checker> firstPassSelectCheckMap,
                               Map<Selector, Checker> secondPassSelectorCheckerMap,
                               Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap) {
        this.prepareDDls = prepareDDls;
        this.rejectDDls = rejectDDls;
        this.cleanupDDls = cleanupDDls;
        this.firstPassSelectCheckMap = firstPassSelectCheckMap;
        this.secondPassSelectCheckMap = secondPassSelectorCheckerMap;
        this.tableTopologyCheckActionMap = tableTopologyCheckActionMap;
        this.checkTriggers = checkTriggers;
    }

    private String applySubstitute(String original, Map<String, String> stringMap) {
        for (Map.Entry<String, String> stringMapEntry : stringMap.entrySet()) {
            original = original.replaceAll(Pattern.quote(stringMapEntry.getKey()), stringMapEntry.getValue());
        }
        return original;

    }

    public static class SubstitueAction {
        String substitue(String original) {
            return original;
        }
    }

    public void visit(SubstitueAction substitueAction) {
        this.prepareDDls = this.prepareDDls.stream().map(substitueAction::substitue).collect(Collectors.toList());
        HashMap<String, String> rejectDDls = new HashMap<>();
        this.rejectDDls.forEach((key, value) -> {
            rejectDDls.put(substitueAction.substitue(key), substitueAction.substitue(value));
        });
        this.rejectDDls = rejectDDls;
        this.firstPassSelectCheckMap.forEach((key, value) -> {
            key.apply(substitueAction);
            value.apply(substitueAction);
        });
        this.secondPassSelectCheckMap.forEach((key, value) -> {
            key.apply(substitueAction);
            value.apply(substitueAction);
        });
        this.tableTopologyCheckActionMap.forEach((key, value) -> value.apply(substitueAction));
    }

    public void applyNodeMap(Map<String, String> nodeMap) {
        if (nodeMap.size() == 0) {
            return;
        }
        visit(new SubstitueAction() {
            @Override
            String substitue(String original) {
                return applySubstitute(original, nodeMap);
            }
        });
    }

    public void applyTableGroupMap(Map<String, String> tableGroupMap) {
        if (tableGroupMap.size() == 0) {
            return;
        }
        visit(new SubstitueAction() {
            @Override
            String substitue(String original) {
                return applySubstitute(original, tableGroupMap);
            }
        });
    }

    public void execute(Connection tddlConnection) {
        try {
            executePrepareDdls(tddlConnection);
            waitTillCheckTriggerOn(tddlConnection);
            List<LocalityTestUtils.LocalityBean> showLocalityResult = getLocalityInfo(tddlConnection);
            for (LocalityTestUtils.LocalityBean item : showLocalityResult) {
                logger.info("show locality item: " + item.toString());
            }
            checkLocalityInfo(this.firstPassSelectCheckMap, showLocalityResult);
            Map<String, String> groupNameMap = collectGroupNameMap(this.firstPassSelectCheckMap);
            logger.info(String.format("groupNameMap is %s", groupNameMap.toString()));
            this.groupNameMap = groupNameMap;
            applyTableGroupMap(groupNameMap);
            checkLocalityInfo(this.secondPassSelectCheckMap, showLocalityResult);
            for (LocalityTestUtils.LocalityBean item : showLocalityResult) {
                logger.info("show locality item: %s" + item.toString());
            }
            List<String> tableNameList = new ArrayList<>(this.tableTopologyCheckActionMap.keySet());
            Map<String, List<LocalityTestUtils.TopologyBean>> showTopologyResult =
                getTopologyInfo(tddlConnection, this.dbName, tableNameList);
            checkTopologyInfo(this.tableTopologyCheckActionMap, showTopologyResult);
            executeRejectDdls(tddlConnection);
        } catch (Exception e) {
            executeCleanupDdls(tddlConnection);
            throw e;
        }
    }

    public void waitTillCheckTriggerOn(Connection tddlConnection) {
        Boolean triggerOn = false;
        while (!triggerOn) {
            triggerOn = true;
            for (Map.Entry<String, Integer> checkTrigger : checkTriggers.entrySet()) {
                Integer value = -1;
                try {
                    ResultSet resultSet = JdbcUtil.executeQuery(checkTrigger.getKey(), tddlConnection);
                    while (resultSet.next()) {
                        value = resultSet.getInt(1);
                    }
                } catch (SQLException e) {
                    triggerOn = false;
                    continue;
                }
                if (!value.equals(checkTrigger.getValue())) {
                    triggerOn = false;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void executePrepareDdls(Connection tddlConnection) {
        for (String prepareDDl : prepareDDls) {
            logger.info(String.format("execute prepare ddl: %s ", prepareDDl));
            JdbcUtil.executeUpdate(tddlConnection, prepareDDl);
        }
    }

    public void executeRejectDdls(Connection tddlConnection) {
        for (Map.Entry<String, String> rejectDDl : rejectDDls.entrySet()) {
            logger.info(String.format("execute reject ddl: %s ", rejectDDl.getKey()));
            JdbcUtil.executeUpdateFailed(tddlConnection, rejectDDl.getKey(), rejectDDl.getValue());
        }
    }

    public void executeCleanupDdls(Connection tddlConnection) {
        for (String cleanupDDl : cleanupDDls) {
            JdbcUtil.executeUpdate(tddlConnection, cleanupDDl);
        }
    }

    public Map<String, String> collectGroupNameMap(Map<Selector, Checker> selectCheckMap) {
        return selectCheckMap.values().stream().filter(checker -> checker instanceof CheckItemValueAndCollectName)
            .map(checker -> ((CheckItemValueAndCollectName) checker).collectedName)
            .collect(Collectors.toMap(Map.Entry<String, String>::getKey, Map.Entry<String, String>::getValue));
    }

}
