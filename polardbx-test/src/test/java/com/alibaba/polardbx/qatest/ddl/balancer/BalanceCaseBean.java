package com.alibaba.polardbx.qatest.ddl.balancer;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class BalanceCaseBean {

    public static BalanceCaseBean readTestCase(String fileDir) throws IOException {
        Yaml yaml = new Yaml();
        InputStream inputStream = Files.newInputStream(Paths.get(fileDir));
        return yaml.loadAs(inputStream, BalanceCaseBean.class);
    }

    public List<SingleBalanceCaseBean> getSingleBalanceCaseBeans() {
        return singleBalanceCaseBeans;
    }

    public void setSingleBalanceCaseBeans(
        List<SingleBalanceCaseBean> singleBalanceCaseBeans) {
        this.singleBalanceCaseBeans = singleBalanceCaseBeans;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public int getDnNum() {
        return dnNum;
    }

    public void setDnNum(int dnNum) {
        this.dnNum = dnNum;
    }

    List<SingleBalanceCaseBean> singleBalanceCaseBeans;
    String schemaName;
    int dnNum;

    public static class SingleBalanceCaseBean {
        List<CreateTableAction> createTableActions;
        List<ManipulateAction> manipulateActions;

        public List<CreateTableAction> getCreateTableActions() {
            return createTableActions;
        }

        public void setCreateTableActions(
            List<CreateTableAction> createTableActions) {
            this.createTableActions = createTableActions;
        }

        public List<ManipulateAction> getManipulateActions() {
            return manipulateActions;
        }

        public void setManipulateActions(
            List<ManipulateAction> manipulateActions) {
            this.manipulateActions = manipulateActions;
        }

        public List<DataDistributionCheckAction> getDataDistributionCheckActions() {
            return dataDistributionCheckActions;
        }

        public void setDataDistributionCheckActions(
            List<DataDistributionCheckAction> dataDistributionCheckActions) {
            this.dataDistributionCheckActions = dataDistributionCheckActions;
        }

        List<DataDistributionCheckAction> dataDistributionCheckActions;
    }

    public static class CreateTableAction {
        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getCreateTableStmt() {
            return createTableStmt;
        }

        public void setCreateTableStmt(String createTableStmt) {
            this.createTableStmt = createTableStmt;
        }

        public String getKeyDistribution() {
            return keyDistribution;
        }

        public void setKeyDistribution(String keyDistribution) {
            this.keyDistribution = keyDistribution;
        }

        public List<String> getDistributionParameter() {
            return distributionParameter;
        }

        public void setDistributionParameter(List<String> distributionParameter) {
            this.distributionParameter = distributionParameter;
        }

        public int getRowNum() {
            return rowNum;
        }

        public void setRowNum(int rowNum) {
            this.rowNum = rowNum;
        }

        String tableName;
        String createTableStmt;
        String keyDistribution;
        List<String> distributionParameter;
        int rowNum;
    }

    public static class ManipulateAction {
        String conditionStmt;

        public List<String> getExpectedConditionResult() {
            return expectedConditionResult;
        }

        public List<Integer> getExpectedConditionColumns() {
            return expectedConditionColumns;
        }

        public void setExpectedConditionColumns(List<Integer> expectedConditionColumns) {
            this.expectedConditionColumns = expectedConditionColumns;
        }

        public List<Integer> expectedConditionColumns;

        public void setExpectedConditionResult(List<String> expectedConditionResult) {
            this.expectedConditionResult = expectedConditionResult;
        }

        List<String> expectedConditionResult;
        String manipulateStmt;

        public String getException() {
            return exception;
        }

        public void setException(String exception) {
            this.exception = exception;
        }

        String exception;

        public String getConditionStmt() {
            return conditionStmt;
        }

        public void setConditionStmt(String conditionStmt) {
            this.conditionStmt = conditionStmt;
        }

        public String getManipulateStmt() {
            return manipulateStmt;
        }

        public void setManipulateStmt(String manipulateStmt) {
            this.manipulateStmt = manipulateStmt;
        }

        public List<String> getExpectedManipulateResult() {
            return expectedManipulateResult;
        }

        public void setExpectedManipulateResult(List<String> expectedManipulateResult) {
            this.expectedManipulateResult = expectedManipulateResult;
        }

        List<String> expectedManipulateResult;

        public List<Integer> getExpectedManipulateColumns() {
            return expectedManipulateColumns;
        }

        public void setExpectedManipulateColumns(List<Integer> expectedManipulateColumns) {
            this.expectedManipulateColumns = expectedManipulateColumns;
        }

        List<Integer> expectedManipulateColumns;
    }

    public static class DataDistributionCheckAction {
        public String getConditionStmt() {
            return conditionStmt;
        }

        public void setConditionStmt(String conditionStmt) {
            this.conditionStmt = conditionStmt;
        }

        String conditionStmt;

        public List<String> getExpectedConditionResult() {
            return expectedConditionResult;
        }

        public void setExpectedConditionResult(List<String> expectedConditionResult) {
            this.expectedConditionResult = expectedConditionResult;
        }

        public String getObjectName() {
            return objectName;
        }

        public void setObjectName(String objectName) {
            this.objectName = objectName;
        }

        public String getObjectType() {
            return objectType;
        }

        public void setObjectType(String objectType) {
            this.objectType = objectType;
        }

        List<String> expectedConditionResult;
        String objectName;
        String objectType;

        public double getExpectedMu() {
            return expectedMu;
        }

        public void setExpectedMu(double expectedMu) {
            this.expectedMu = expectedMu;
        }

        double expectedMu;
    }

}
