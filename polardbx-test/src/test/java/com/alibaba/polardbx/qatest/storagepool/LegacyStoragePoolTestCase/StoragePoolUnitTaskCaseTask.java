package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.storagepool.StoragePoolTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase.StoragePoolTestCaseValidator.*;

public class StoragePoolUnitTaskCaseTask {

    public static Logger logger = LoggerFactory.getLogger(StoragePoolUnitTaskCaseTask.class);

    static private String trimString(String original) {
        original = original.trim();
        if (original.startsWith("\"") && original.endsWith("\"")) {
            original = original.substring(1, original.length() - 1);
        }
        return original;
    }

    static public StoragePoolUnitTaskCaseTask loadFrom(StoragePoolTestCaseBean.SingleTestCase singleTestCase) {
        Map<StoragePoolTestCaseValidator.Selector, StoragePoolTestCaseValidator.Checker>
            localityValueAndTableGroupMatchCheckerMap = new HashMap<>();
        StoragePoolTestCaseBean.CheckActions checkActions = singleTestCase.checkActions;
        localityValueAndTableGroupMatchCheckerMap.putAll(
            StoragePoolUnitTaskCaseTask.fromLocalityValueCheck(checkActions.localityValueCheck));
        localityValueAndTableGroupMatchCheckerMap.putAll(fromTableGroupMatchCheck(checkActions.tableGroupMatchCheck));

        Map<StoragePoolTestCaseValidator.Selector, StoragePoolTestCaseValidator.Checker> partitionLocalityCheckerMap =
            new HashMap<>();
        if (checkActions.partitionLocalityCheck != null) {
            partitionLocalityCheckerMap.putAll(
                StoragePoolUnitTaskCaseTask.fromPartitionLocalityCheck(checkActions.partitionLocalityCheck));
        }

        Map<String, StoragePoolTestCaseValidator.TableTopologyCheckAction> tableTopologyCheckActions =
            StoragePoolUnitTaskCaseTask.fromTopologyCheck(checkActions);
        Map<String, StoragePoolTestCaseValidator.DatasourceCheckAction> datasourceCheckActions =
            StoragePoolUnitTaskCaseTask.fromDatasourcesCheck(checkActions);
        Map<String, StoragePoolTestCaseValidator.StoragePoolValueCheckAction> storagePoolValueCheckActions =
            StoragePoolUnitTaskCaseTask.fromStoragePoolValueCheck(checkActions);

        Map<String, Integer> checkTriggers =
            StoragePoolUnitTaskCaseTask.fromCheckTriggers(singleTestCase.checkTriggers);
        Map<String, String> rejectDDls = new HashMap<>();
        if (singleTestCase.getRejectDDls() != null) {
            rejectDDls = singleTestCase.getRejectDDls().stream().collect(
                Collectors.toMap(StoragePoolTestCaseBean.RejectDdl::getDdl,
                    StoragePoolTestCaseBean.RejectDdl::getMessage));
        }

        Map<String, String> expectedSQLs = new HashMap<>();
        if (singleTestCase.getExpectedSQLs() != null) {
            singleTestCase.getExpectedSQLs().stream().collect(
                Collectors.toMap(StoragePoolTestCaseBean.ExpectedSQL::getSql,
                    StoragePoolTestCaseBean.ExpectedSQL::getResult));
        }
        return new StoragePoolUnitTaskCaseTask(singleTestCase.prepareDDls, rejectDDls, expectedSQLs,
            singleTestCase.cleanupDDls,
            checkTriggers, localityValueAndTableGroupMatchCheckerMap, partitionLocalityCheckerMap,
            storagePoolValueCheckActions,
            datasourceCheckActions,
            tableTopologyCheckActions);
    }

    static public Map<StoragePoolTestCaseValidator.Selector, StoragePoolTestCaseValidator.Checker> fromLocalityValueCheck(
        List<String> localityValueCheck) {
        Map<StoragePoolTestCaseValidator.Selector, StoragePoolTestCaseValidator.Checker> selectorCheckerMap =
            new HashMap<>();
        if (localityValueCheck != null) {
            for (String localityItem : localityValueCheck) {
                String[] localityItemList = localityItem.split(" ");
                String objectName = localityItemList[0];
                String objectType = localityItemList[1];
                String locality = trimString(localityItemList[2]);
                StoragePoolTestCaseValidator.Selector selector =
                    new StoragePoolTestCaseValidator.SelectByObjectTypeAndName(objectType, objectName);
                StoragePoolTestCaseValidator.Checker checker =
                    new StoragePoolTestCaseValidator.CheckItemValue(locality);
                selectorCheckerMap.put(selector, checker);
            }
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

    static public Map<String, StoragePoolTestCaseValidator.DatasourceCheckAction> fromDatasourcesCheck(
        StoragePoolTestCaseBean.CheckActions checkActions) {

        List<String> datasourceCheck = Optional.ofNullable(checkActions.datasourceCheck).orElse(new ArrayList<>());
        Map<String, DatasourceCheckAction> results = new HashMap<>();
        for (String datasourceItem : datasourceCheck) {
            String[] datasourceItemList = datasourceItem.split(" ");
            String dbName = datasourceItemList[0];
            String[] dnIds = trimString(datasourceItemList[1]).split(",");
            DatasourceCheckAction datasourceCheckAction =
                new DatasourceCheckAction(dbName, Arrays.stream(dnIds).collect(
                    Collectors.toList()));
            results.put(dbName, datasourceCheckAction);
        }
        return results;
    }

    static public Map<String, StoragePoolTestCaseValidator.StoragePoolValueCheckAction> fromStoragePoolValueCheck(
        StoragePoolTestCaseBean.CheckActions checkActions) {

        List<String> storagePoolCheck =
            Optional.ofNullable(checkActions.storagePoolValueCheck).orElse(new ArrayList<>());
        Map<String, StoragePoolValueCheckAction> results = new HashMap<>();
        for (String storagePoolCheckItem : storagePoolCheck) {
            String[] storagePoolCheckItemList = storagePoolCheckItem.split(" ");
            String storagePool = storagePoolCheckItemList[0];
            String[] dnIds = trimString(storagePoolCheckItemList[1]).split(",");
            String undeletableDn = trimString(storagePoolCheckItemList[2]);
            List<String> dnIdList =
                Arrays.stream(dnIds).filter(o -> !StringUtils.isEmpty(o)).collect(Collectors.toList());
            StoragePoolValueCheckAction storagePoolValueCheckAction =
                new StoragePoolValueCheckAction(storagePool, dnIdList,
                    undeletableDn);
            results.put(storagePool, storagePoolValueCheckAction);
        }
        return results;
    }

    static public Map<String, StoragePoolTestCaseValidator.TableTopologyCheckAction> fromTopologyCheck(
        StoragePoolTestCaseBean.CheckActions checkActions) {
        List<String> localityValueCheck =
            (checkActions.localityValueCheck == null) ? new ArrayList<>() : checkActions.localityValueCheck;
        List<String> tableGroupMatchCheck =
            (checkActions.tableGroupMatchCheck == null) ? new ArrayList<>() : checkActions.tableGroupMatchCheck;
        List<String> topologyCheck =
            (checkActions.topologyCheck == null) ? new ArrayList<>() : checkActions.topologyCheck;
        List<String> partitionLocalityCheck =
            (checkActions.partitionLocalityCheck == null) ? new ArrayList<>() : checkActions.partitionLocalityCheck;
        Map<String, StoragePoolTestCaseValidator.TableTopologyCheckAction> tableTopologyCheckActions = new HashMap<>();
        //auto generated from locality value check
        for (String localityItem : localityValueCheck) {
            String[] localityItemList = localityItem.split(" ");
            String objectName = localityItemList[0];
            String objectType = localityItemList[1];
            String locality = trimString(localityItemList[2]);
            if (objectType.equals("table")) {
                tableTopologyCheckActions.put(objectName,
                    new StoragePoolTestCaseValidator.TableTopologyCheckAction(objectName, locality));
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
                int lastDotIndex = objectName.lastIndexOf(".");
                String partitionName = objectName.substring(lastDotIndex + 1);
                String[] tableNames = objectName.substring(0, lastDotIndex).split(",");
                for (String tableName : tableNames) {
                    tableTopologyCheckActions.get(tableName).partitionTopology.put(partitionName, locality);
                }
            } else if (objectType.equals("table")) {
                if (tableTopologyCheckActions.containsKey(objectName)) {
                    tableTopologyCheckActions.get(objectName).tableTopology = locality;
                } else {
                    tableTopologyCheckActions.put(objectName,
                        new StoragePoolTestCaseValidator.TableTopologyCheckAction(objectName, locality));
                }
            }
        }
        return tableTopologyCheckActions;
    }

    static public Map<Selector, Checker> fromTableGroupMatchCheck(List<String> tableGroupMatchCheck) {
        Map<Selector, Checker> selectorCheckerMap = new HashMap<>();
        String objectType = "tablegroup";
        if (tableGroupMatchCheck != null) {
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
    public Map<String, String> exepctedSQLs;
    public List<String> dbNames;
    Map<String, Integer> checkTriggers;
    Map<Selector, Checker> firstPassSelectCheckMap;
    Map<Selector, Checker> secondPassSelectCheckMap;
    Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap;

    Map<String, DatasourceCheckAction> datasourceCheckActionMap;

    Map<String, StoragePoolValueCheckAction> storagePoolValueCheckActionMap;
    Map<String, String> groupNameMap;

    StoragePoolUnitTaskCaseTask(List<String> prepareDDls, Map<String, String> rejectDDls,
                                Map<String, String> expectedSQLs,
                                List<String> cleanupDDls,
                                Map<String, Integer> checkTriggers, Map<Selector, Checker> firstPassSelectCheckMap,
                                Map<Selector, Checker> secondPassSelectorCheckerMap,
                                Map<String, StoragePoolValueCheckAction> storagePoolValueCheckActionMap,
                                Map<String, DatasourceCheckAction> datasourceCheckActionMap,
                                Map<String, TableTopologyCheckAction> tableTopologyCheckActionMap) {
        this.prepareDDls = prepareDDls;
        this.rejectDDls = rejectDDls;
        this.cleanupDDls = cleanupDDls;
        this.exepctedSQLs = expectedSQLs;
        this.firstPassSelectCheckMap = firstPassSelectCheckMap;
        this.secondPassSelectCheckMap = secondPassSelectorCheckerMap;
        this.datasourceCheckActionMap = datasourceCheckActionMap;
        this.storagePoolValueCheckActionMap = storagePoolValueCheckActionMap;
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
        HashMap<String, String> expectedSQLs = new HashMap<>();
        this.exepctedSQLs.forEach((key, value) -> {
            expectedSQLs.put(substitueAction.substitue(key), substitueAction.substitue(value));
        });
        this.exepctedSQLs = expectedSQLs;
        this.firstPassSelectCheckMap.forEach((key, value) -> {
            key.apply(substitueAction);
            value.apply(substitueAction);
        });
        this.secondPassSelectCheckMap.forEach((key, value) -> {
            key.apply(substitueAction);
            value.apply(substitueAction);
        });
        this.datasourceCheckActionMap.forEach((key, value) -> value.apply(substitueAction));
        this.storagePoolValueCheckActionMap.forEach((key, value) -> value.apply(substitueAction));
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
            List<StoragePoolTestUtils.LocalityBean> showLocalityResult = new ArrayList<>();
            for (String dbName : dbNames) {
                showLocalityResult.addAll(getLocalityInfo(tddlConnection, dbName));
            }
            for (StoragePoolTestUtils.LocalityBean item : showLocalityResult) {
                logger.info("show locality item: " + item.toString());
            }
            checkLocalityInfo(this.firstPassSelectCheckMap, showLocalityResult);
            Map<String, String> groupNameMap = collectGroupNameMap(this.firstPassSelectCheckMap);
            logger.info(String.format("groupNameMap is %s", groupNameMap.toString()));
            this.groupNameMap = groupNameMap;
            applyTableGroupMap(groupNameMap);
            checkLocalityInfo(this.secondPassSelectCheckMap, showLocalityResult);
            for (StoragePoolTestUtils.LocalityBean item : showLocalityResult) {
                logger.info("show locality item: %s" + item.toString());
            }
            Map<String, List<StoragePoolTestUtils.DsBean>> showDsResult =
                getDsInfo(tddlConnection);
            checkDsInfo(this.datasourceCheckActionMap, showDsResult);
            Map<String, StoragePoolTestUtils.StoragePoolBean> showStoragePoolResult =
                getStoragePoolInfo(tddlConnection);
            checkStoragePoolInfo(this.storagePoolValueCheckActionMap, showStoragePoolResult);
            List<String> tableNameList = new ArrayList<>(this.tableTopologyCheckActionMap.keySet());
            Map<String, List<StoragePoolTestUtils.TopologyBean>> showTopologyResult = new HashMap<>();
            for (String dbName : this.dbNames) {
                showTopologyResult.putAll(getTopologyInfo(tddlConnection, dbName, tableNameList));
            }
            checkTopologyInfo(this.tableTopologyCheckActionMap, showTopologyResult);
            executeExpectedSQLs(tddlConnection);
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

    public void executeExpectedSQLs(Connection tddlConnection) {
        for (Map.Entry<String, String> excpetedSQL : exepctedSQLs.entrySet()) {
            logger.info(String.format("execute expected sql: %s ", excpetedSQL.getKey()));
            ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, excpetedSQL.getKey());
            List<List<Object>> results = JdbcUtil.getAllResult(resultSet);
            List<String> lineResult = results.stream().map(o -> StringUtils.join(o, "#")).collect(Collectors.toList());
            String finalResult = StringUtils.join(lineResult, "##");
            if (!finalResult.toLowerCase().contains(excpetedSQL.getValue().toLowerCase())) {
                String message = String.format("check expected SQL {} failed! expected {}, but get {}",
                    excpetedSQL.getKey(), excpetedSQL.getValue().toLowerCase(), finalResult);
                throw new RuntimeException(message);
            }
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
