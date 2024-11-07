package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

public class FullTypeTest extends ColumnarReadBaseTestCase {
    Logger logger = LoggerFactory.getLogger(FullTypeTest.class);
    public static String PRIMARY_TABLE_NAME = "full_type_create_test";
    public static String COLUMNAR_INDEX_NAME = "full_type_index";

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
                ExecuteTableSelect.DEFAULT_NEW_PARTITIONING_DEFINITION));
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
    }

    @Test
    public void createIndexThenInsert() throws InterruptedException {
        ColumnarUtils.createColumnarIndex(tddlConnection,
            COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 4);
        List<String> insertSql = GsiConstant.buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME)
            .values().stream().flatMap(List::stream).collect(Collectors.toList());

        for (String sql : insertSql) {
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        waitForSync(tddlConnection);

        waitForRowCountEquals(tddlConnection, PRIMARY_TABLE_NAME, COLUMNAR_INDEX_NAME);

        checkColumnOneByOne();
        String columnarSql = "select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void insertThenCreateIndex() throws InterruptedException {
        List<String> insertSql = GsiConstant.buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME)
            .values().stream().flatMap(List::stream).collect(Collectors.toList());

        for (String sql : insertSql) {
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        ColumnarUtils.createColumnarIndex(tddlConnection,
            COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 4);

        checkColumnOneByOne();
        String columnarSql = "select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ")";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    public static void waitForRowCountEquals(Connection polardbxConnection, String primaryTableName,
                                             String columnarIndexName) throws InterruptedException {

        String columnarSql =
            "select count(*) from " + primaryTableName + " force index (" + columnarIndexName + ")";
        String primarySql = "select count(*) from " + primaryTableName + " force index (primary)";
        try (Statement statement = polardbxConnection.createStatement()) {
            int waitTime = 0;
            while (waitTime < 60) {
                ResultSet rs1 = statement.executeQuery(primarySql);
                rs1.next();
                int innodbCount = rs1.getInt(1);
                ResultSet rs2 = statement.executeQuery(columnarSql);
                rs2.next();
                int columnarCount = rs2.getInt(1);
                if (innodbCount == columnarCount) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                    waitTime++;
                } catch (InterruptedException e) {
                    return;
                }
            }
            if (waitTime >= 60) {
                throw new AssertionError("Columnar delay is beyond 1 minute, abort test");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, polardbxConnection,
            polardbxConnection,
            false, false, false);
    }

    public static void waitForSync(Connection polardbxConnection) throws InterruptedException {
        //cdc延迟基本在1s以内，有小概率超过1s，导致polardbx的位点不包含已执行的语句
        Thread.sleep(10000);

        List<List<String>> result =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("SHOW COLUMNAR OFFSET", polardbxConnection), false, null);
        List<String> polardbxResult = result.stream().filter(list -> list.get(0).equalsIgnoreCase("CDC"))
            .collect(Collectors.toList()).get(0);
        String[] currentBinlogFileSplits = polardbxResult.get(1).split("\\.");
        int currentBinlogFileId = Integer.parseInt(currentBinlogFileSplits[currentBinlogFileSplits.length - 1]);
        long currentBinlogPosition = Long.parseLong(polardbxResult.get(2));

        while (true) {
            List<List<String>> offset =
                JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("SHOW COLUMNAR OFFSET", polardbxConnection), false,
                    null);
            List<String> columnarResult =
                offset.stream().filter(list -> list.get(0).equalsIgnoreCase("CN_MIN_LATENCY"))
                    .collect(Collectors.toList()).get(0);

            String[] columnarBinlogFileSplits = columnarResult.get(1).split("\\.");
            int columnarBinlogFileId = Integer.parseInt(columnarBinlogFileSplits[columnarBinlogFileSplits.length - 1]);
            long columnarBinlogPosition = Long.parseLong(columnarResult.get(2));
            if ((currentBinlogFileId == columnarBinlogFileId ?
                Long.compare(columnarBinlogPosition, currentBinlogPosition) :
                Integer.compare(columnarBinlogFileId, currentBinlogFileId)) >= 0) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }

        }
    }

    private void checkColumnOneByOne() {
        String columnarSqlFormat =
            "select %s from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ")";
        String primarySqlFormat = "select %s from " + PRIMARY_TABLE_NAME + " force index (primary)";
        List<String> checkFailedList = new ArrayList<>();
        for (String columnName : TableConstant.FULL_TYPE_TABLE_COLUMNS) {
            boolean checkOk = true;
            try {
                DataValidator.selectContentSameAssertWithDiffSql(
                    String.format(columnarSqlFormat, columnName),
                    String.format(primarySqlFormat, columnName),
                    null, tddlConnection, tddlConnection, false, false, false);
            } catch (Throwable t) {
                logger.error(String.format("%s check failed: %s", columnName, t.getMessage()));
                checkOk = false;
            }

            if (!checkOk) {
                checkFailedList.add(columnName);
            }
        }
        if (!checkFailedList.isEmpty()) {
            assertWithMessage(checkFailedList.stream().reduce("Check failed column: ", (a, b) -> a + ", " + b)).fail();
        }
    }
}
