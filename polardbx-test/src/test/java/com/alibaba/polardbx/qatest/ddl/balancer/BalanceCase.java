package com.alibaba.polardbx.qatest.ddl.balancer;

import com.alibaba.polardbx.common.statementsummary.model.ExecInfo;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataGenerator;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.ManualHotSpotDataGenerator;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.NormalDistributionDataGenerator;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.UniformDistributionDataGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BalanceCase extends BaseTestCase {
    public static final Logger LOG = LoggerFactory.getLogger(BalanceCase.class);
    BalanceCaseBean balanceCaseBean;

    Connection tddlConnection;

    BalanceCase(String resourceFile) throws IOException {
        String resourceDir = "partition/env/RebalanceTest/" + resourceFile;
        String fileDir = getClass().getClassLoader().getResource(resourceDir).getPath();
        balanceCaseBean = BalanceCaseBean.readTestCase(fileDir);
    }

    void runCase() throws InterruptedException, SQLException {
        tddlConnection = getPolardbxConnection();
        runCreateDatabase(balanceCaseBean.schemaName);
        for (BalanceCaseBean.SingleBalanceCaseBean singleBalanceCaseBean : balanceCaseBean.getSingleBalanceCaseBeans()) {
            runCreateTableActions(singleBalanceCaseBean.createTableActions);
            runManipulateActions(singleBalanceCaseBean.manipulateActions);
            runCheckDataDistributionActions(singleBalanceCaseBean.dataDistributionCheckActions);
        }
        Thread.sleep(2 * 1000);
//        runDropDatabase(balanceCaseBean.schemaName);

    }

    DataGenerator fromDistribution(String distributionType, List<String> distributionParams) {
        //TODO
        DataGenerator dataGenerator = null;
        if ("normal".equals(distributionType)) {
            dataGenerator = new NormalDistributionDataGenerator();
        } else if ("uniform".equals(distributionType)) {
            dataGenerator = new UniformDistributionDataGenerator();
        } else {
            dataGenerator = new ManualHotSpotDataGenerator();
        }
        return dataGenerator;
    }

    void runCreateDatabase(String schemaName) {
        String createDbSql = String.format("create database if not exists `%s` mode = 'auto'", schemaName);
        String useDbSql = String.format("use `%s`", schemaName);
        runDropDatabase(schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDbSql);

    }

    void runDropDatabase(String schemaName) {
        String useDbSql = "use polardbx";
        String dropDbSql = String.format("drop database if exists `%s`", schemaName);
        try {
            JdbcUtil.executeUpdate(tddlConnection, useDbSql);
            LOG.info("execute " + dropDbSql);
            JdbcUtil.executeUpdate(tddlConnection, dropDbSql);
        } catch (Exception e) {

        }
    }

    void runCreateTableActions(List<BalanceCaseBean.CreateTableAction> createTableActions) {
        if (createTableActions == null) {
            return;
        }
        for (int i = 0; i < createTableActions.size(); i++) {
            BalanceCaseBean.CreateTableAction createTableAction = createTableActions.get(i);
            LOG.info("execute " + createTableAction.createTableStmt);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTableAction.createTableStmt);
            String tableName = createTableAction.tableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, "truncate table " + tableName);
            DataGenerator dataGenerator =
                fromDistribution(createTableAction.keyDistribution, createTableAction.distributionParameter);
            DataLoader dataLoader = DataLoader.create(tddlConnection, tableName, dataGenerator);
            dataLoader.batchInsert(createTableAction.getRowNum());
        }
    }

    Boolean compareResult(List<List<Object>> results, List<String> expectedResult, List<Integer> columns) {
        if (results.size() == expectedResult.size()) {
            for (int i = 0; i < results.size(); i++) {
                List<Object> row = results.get(i);
                String[] expectedRow = expectedResult.get(i).split("#");
                for (int j = 0; j < expectedRow.length; j++) {
                    int index;
                    if (columns == null) {
                        index = j;
                    } else {
                        index = columns.get(j);
                    }
                    if (index >= row.size() || !row.get(index).toString().equals(expectedRow[j])) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    void waitTillSatisfiy(String conditionStmt, List<String> expectedConditionStmt, List<Integer> columns)
        throws InterruptedException {
        Boolean flag = false;
        Long waitInterval = 2 * 1000L;
        if (conditionStmt == null) {
            return;
        }
        while (!flag) {
            Thread.sleep(waitInterval);
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuery(conditionStmt, tddlConnection));
            flag = compareResult(results, expectedConditionStmt, columns);
        }
    }

    void runManipulateActions(List<BalanceCaseBean.ManipulateAction> manipulateActions) throws InterruptedException {
        for (int i = 0; i < manipulateActions.size(); i++) {
            BalanceCaseBean.ManipulateAction manipulateAction = manipulateActions.get(i);
            waitTillSatisfiy(manipulateAction.conditionStmt, manipulateAction.expectedConditionResult,
                manipulateAction.expectedConditionColumns);
            LOG.info("execute " + manipulateAction.manipulateStmt);
            List<List<Object>> results = new ArrayList<>();
            if (manipulateAction.getException() == null) {
                if (manipulateAction.expectedManipulateResult == null
                    || manipulateAction.expectedManipulateResult.isEmpty()) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, manipulateAction.manipulateStmt);
                } else {
                    ResultSet resultSet = JdbcUtil.executeQuery(manipulateAction.manipulateStmt, tddlConnection);
                    results = JdbcUtil.getAllResult(resultSet);
                    String assertTips =
                        String.format("expected %s, while get %s", manipulateAction.expectedManipulateResult, results);
                    Assert.assertTrue(compareResult(results, manipulateAction.expectedManipulateResult,
                        manipulateAction.expectedManipulateColumns), assertTips);

                }
            } else {
                try {
                    JdbcUtil.executeQuery(manipulateAction.manipulateStmt, tddlConnection);
                } catch (AssertionError exception) {
                    if (!exception.getMessage().contains(manipulateAction.getException())) {
                        throw exception;
                    }
                }
            }

        }
    }

    Boolean checkDataDistribution(String objectName, String objectType, double mu) throws SQLException {
        //TODO
        String sql;
        String queryDataDistributionStmt = "select  sum(table_rows) as table_rows,\n"
            + "            sum(1) as part_num, storage_inst_id from information_schema.table_detail\n"
            + "        where table_schema='" + balanceCaseBean.schemaName
            + "' and %s group by storage_inst_id;";
        switch (objectType) {
        case "table_group":
            sql = String.format(queryDataDistributionStmt, "table_group_name='" + objectName + "'");
            break;
        case "table":
            sql = String.format(queryDataDistributionStmt, "table_name='" + objectName + "'");
            break;
        case "database":
            sql = String.format(queryDataDistributionStmt, "1=1");
            break;
        default:
            sql = "";
            break;
        }
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
        LOG.info("data distribution of " + objectName + " :" + results);
        List<Long> rowNums = results.stream().map(o -> ((JdbcUtil.MyNumber) o.get(0)).getNumber().longValue())
            .collect(Collectors.toList());
        List<Long> partNums = results.stream().map(o -> ((JdbcUtil.MyNumber) o.get(1)).getNumber().longValue())
            .collect(Collectors.toList());
        Double avgRows = rowNums.stream().mapToLong(o -> o).average().getAsDouble();
        Boolean nonBadRowNums = rowNums.stream().noneMatch(o -> o < avgRows * (1 - mu) || o > avgRows * (1 + mu));
        Long sumPart = partNums.stream().mapToLong(o -> o).sum();
        Long lowPart = Math.floorDiv(sumPart, partNums.size());
        Long highPart = -Math.floorDiv(-sumPart, partNums.size());
        Boolean nonBadPartNums = partNums.stream().noneMatch(o -> o < lowPart || o > highPart);
        return nonBadPartNums && nonBadRowNums;
    }

    void runCheckDataDistributionActions(List<BalanceCaseBean.DataDistributionCheckAction> dataDistributionCheckActions)
        throws InterruptedException, SQLException {
        for (int i = 0; i < dataDistributionCheckActions.size(); i++) {
            BalanceCaseBean.DataDistributionCheckAction dataDistributionCheckAction =
                dataDistributionCheckActions.get(i);
            waitTillSatisfiy(dataDistributionCheckAction.conditionStmt,
                dataDistributionCheckAction.expectedConditionResult, null);
        }
        List<Pair<String, String>> errorObjects = new ArrayList<>();
        for (int i = 0; i < dataDistributionCheckActions.size(); i++) {
            BalanceCaseBean.DataDistributionCheckAction dataDistributionCheckAction =
                dataDistributionCheckActions.get(i);
            LOG.info("check data distribution " + dataDistributionCheckAction.objectName);
            Boolean dataDistributionResult =
                checkDataDistribution(dataDistributionCheckAction.objectName, dataDistributionCheckAction.objectType,
                    dataDistributionCheckAction.expectedMu);
            if (!dataDistributionResult) {
                errorObjects.add(
                    Pair.of(dataDistributionCheckAction.objectName, dataDistributionCheckAction.objectType));
            }
        }
        List<String> assertTips =
            errorObjects.stream().map(o -> String.format("bad %s: %s", o.getValue(), o.getKey())).collect(
                Collectors.toList());
        Assert.assertTrue(assertTips.isEmpty(), StringUtils.join(assertTips, ","));
    }
}
