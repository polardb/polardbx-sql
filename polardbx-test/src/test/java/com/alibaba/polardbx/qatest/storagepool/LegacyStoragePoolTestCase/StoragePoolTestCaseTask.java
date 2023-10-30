package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoragePoolTestCaseTask {

    public StoragePoolTestCaseBean storagePoolTestCaseBean;
    public Map<String, String> nodeMap;
    public Map<String, String> tableGroupMap;

    public StoragePoolTestCaseTask(String fileDir) throws FileNotFoundException {
        this.storagePoolTestCaseBean = new StoragePoolTestCaseReader(fileDir).readTestCase();
        this.nodeMap = new HashMap<>();
        this.tableGroupMap = new HashMap<>();
    }

    public void executeCleanupDdls(Connection tddlConnection, List<String> cleanupDDls) {
        for (String cleanupDDl : cleanupDDls) {
            JdbcUtil.executeUpdate(tddlConnection, cleanupDDl);
        }
    }

    public void execute(Connection tddlConnection) throws InterruptedException {
        List<String> storageList = StoragePoolTestUtils.getDatanodes(tddlConnection);
        if (storageList.size() < this.storagePoolTestCaseBean.getStorageList().size()) {
            throw new RuntimeException("The storage inst size is less than the test case wanted!");
        }
        this.nodeMap = StoragePoolTestUtils.generateNodeMap(storageList, this.storagePoolTestCaseBean.getStorageList());
        for (StoragePoolTestCaseBean.SingleTestCase singleTestCase : storagePoolTestCaseBean.testCases) {
            StoragePoolUnitTaskCaseTask
                unitTestCaseTask = StoragePoolUnitTaskCaseTask.loadFrom(singleTestCase);
            unitTestCaseTask.dbNames = singleTestCase.getDbNames();
            unitTestCaseTask.applyNodeMap(nodeMap);
            unitTestCaseTask.applyTableGroupMap(tableGroupMap);
            unitTestCaseTask.execute(tddlConnection);
            tableGroupMap.putAll(unitTestCaseTask.groupNameMap);
        }
        Thread.sleep(2 * 1000);
        executeCleanupDdls(tddlConnection, this.storagePoolTestCaseBean.getCleanupDDls());
    }
}
