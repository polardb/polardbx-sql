package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import java.util.ArrayList;
import java.util.List;

public class StoragePoolTestCaseBean {
    public static class CheckActions {
        public List<String> getLocalityValueCheck() {
            return localityValueCheck;
        }

        public void setLocalityValueCheck(List<String> localityValueCheck) {
            this.localityValueCheck = localityValueCheck;
        }

        public List<String> getTableGroupMatchCheck() {
            return tableGroupMatchCheck;
        }

        public void setTableGroupMatchCheck(List<String> tableGroupMatchCheck) {
            this.tableGroupMatchCheck = tableGroupMatchCheck;
        }

        public List<String> getPartitionLocalityCheck() {
            return partitionLocalityCheck;
        }

        public void setPartitionLocalityCheck(List<String> partitionLocalityCheck) {
            this.partitionLocalityCheck = partitionLocalityCheck;
        }

        public List<String> getTopologyCheck() {
            return topologyCheck;
        }

        public void setTopologyCheck(List<String> topologyCheck) {
            this.topologyCheck = topologyCheck;
        }

        public List<String> localityValueCheck = new ArrayList<>();
        public List<String> tableGroupMatchCheck = new ArrayList<>();
        public List<String> partitionLocalityCheck = new ArrayList<>();
        public List<String> topologyCheck = new ArrayList<>();

        public List<String> getDatasourceCheck() {
            return datasourceCheck;
        }

        public void setDatasourceCheck(List<String> datasourceCheck) {
            this.datasourceCheck = datasourceCheck;
        }

        public List<String> getStoragePoolValueCheck() {
            return storagePoolValueCheck;
        }

        public void setStoragePoolValueCheck(List<String> storagePoolValueCheck) {
            this.storagePoolValueCheck = storagePoolValueCheck;
        }

        public List<String> datasourceCheck = new ArrayList<>();
        public List<String> storagePoolValueCheck = new ArrayList<>();
    }

    public static class RejectDdl {
        public String getDdl() {
            return ddl;
        }

        public void setDdl(String ddl) {
            this.ddl = ddl;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String ddl;
        public String message;
    }

    public static class ExpectedSQL {
        public String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public String result;
    }

    public static class SingleTestCase {
        public SingleTestCase() {

        }

        public List<String> getPrepareDDls() {
            return prepareDDls;
        }

        public void setPrepareDDls(List<String> prepareDDls) {
            this.prepareDDls = prepareDDls;
        }

        public List<RejectDdl> getRejectDDls() {
            return rejectDDls;
        }

        public void setRejectDDls(List<RejectDdl> rejectDDls) {
            this.rejectDDls = rejectDDls;
        }

        public List<String> getCheckTriggers() {
            return checkTriggers;
        }

        public void setCheckTriggers(List<String> checkTriggers) {
            this.checkTriggers = checkTriggers;
        }

        public CheckActions getCheckActions() {
            return checkActions;
        }

        public void setCheckActions(CheckActions checkActions) {
            this.checkActions = checkActions;
        }

        public List<String> getCleanupDDls() {
            return cleanupDDls;
        }

        public void setCleanupDDls(List<String> cleanupDDls) {
            this.cleanupDDls = cleanupDDls;
        }

        public List<String> prepareDDls = new ArrayList<>();
        public List<String> checkTriggers = new ArrayList<>();
        public List<String> cleanupDDls = new ArrayList<>();
        public List<RejectDdl> rejectDDls = new ArrayList<>();
        public CheckActions checkActions = new CheckActions();
        public List<String> dbNames = new ArrayList<>();

        public List<String> getDbNames() {
            return dbNames;
        }

        public void setDbNames(List<String> dbNames) {
            this.dbNames = dbNames;
        }

        public List<ExpectedSQL> getExpectedSQLs() {
            return expectedSQLs;
        }

        public void setExpectedSQLs(
            List<ExpectedSQL> expectedSQLs) {
            this.expectedSQLs = expectedSQLs;
        }

        public List<ExpectedSQL> expectedSQLs = new ArrayList<>();
    }

    public List<SingleTestCase> getTestCases() {
        return testCases;
    }

    public void setTestCases(List<SingleTestCase> testCases) {
        this.testCases = testCases;
    }

    public List<String> getStorageList() {
        return storageList;
    }

    public void setStorageList(List<String> storageList) {
        this.storageList = storageList;
    }

    public List<String> getCleanupDDls() {
        return cleanupDDls;
    }

    public void setCleanupDDls(List<String> cleanupDDls) {
        this.cleanupDDls = cleanupDDls;
    }

    public List<SingleTestCase> testCases = new ArrayList<>();
    public List<String> storageList = new ArrayList<>();
    public List<String> cleanupDDls = new ArrayList<>();
}
