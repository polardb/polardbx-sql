package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

public class BatchPreparePolardbxDriverTest extends AutoCrudBasedLockTestCase {

    private final boolean isBroadCast;

    private static final String BATCH_PREPARE_CONN_PROP =
        "directHint=false&serverPrepareBatch=true&useServerPrepStmts=true"
            + "&rewriteBatchedStatements=false&emulateUnsupportedPstmts=false";

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public BatchPreparePolardbxDriverTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
        this.isBroadCast = baseOneTableName.endsWith(ExecuteTableName.BROADCAST_TB_SUFFIX);
    }

    @Before
    public void initData() throws Exception {
        clearData();
    }

    private void clearData() {
        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    private void loadData() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String prepareSql = JdbcUtil.getInsertAllTypePrepareSql(baseOneTableName, columns);

        final int rows = 50;
        try (PreparedStatement polardbxPs = JdbcUtil.preparedStatement(prepareSql, tddlConnection);
            PreparedStatement mysqlPs = JdbcUtil.preparedStatement(prepareSql, mysqlConnection)) {
            for (int i = 0; i < rows; i++) {
                List<Object> params = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, i + 1);

                for (int paramIdx = 0; paramIdx < params.size(); paramIdx++) {
                    mysqlPs.setObject(paramIdx + 1, params.get(paramIdx));
                    polardbxPs.setObject(paramIdx + 1, params.get(paramIdx));
                }
                mysqlPs.addBatch();
                polardbxPs.addBatch();
            }
            mysqlPs.executeBatch();
            polardbxPs.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        DataValidator.selectContentSameAssert(String.format("SELECT * FROM %s", baseOneTableName), null,
            tddlConnection, mysqlConnection);
    }

    @Test
    @Ignore
    public void testBatchPrepareInsert() throws SQLException {
        if (!PropertiesUtil.usePrepare()
            || baseOneTableName.endsWith(ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)
            || baseOneTableName.endsWith(ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX)) {
            return;
        }

        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String prepareSql = JdbcUtil.getInsertAllTypePrepareSql(baseOneTableName, columns);

        try (Connection conn = ConnectionManager.getInstance()
            .newPolarDBXConnectionWithXDriver(polardbxOneDB, BATCH_PREPARE_CONN_PROP);
            PreparedStatement polardbxPs = conn.prepareStatement(prepareSql);
            PreparedStatement mysqlPs = mysqlConnection.prepareStatement(prepareSql)) {
            int pkValue = 1;
            for (int i = 5; i <= 10; i++) {
                for (int j = 1; j <= i; j++) {
                    List<Object> params = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, pkValue++);

                    for (int paramIdx = 0; paramIdx < params.size(); paramIdx++) {
                        mysqlPs.setObject(paramIdx + 1, params.get(paramIdx));
                        polardbxPs.setObject(paramIdx + 1, params.get(paramIdx));
                    }
                    mysqlPs.addBatch();
                    polardbxPs.addBatch();
                }
                mysqlPs.executeBatch();
                polardbxPs.executeBatch();
                mysqlPs.clearBatch();
                polardbxPs.clearBatch();
            }
            DataValidator.selectContentSameAssert(String.format("SELECT * FROM %s", baseOneTableName), null,
                mysqlConnection, conn);
            DataValidator
                .selectContentSameAssert(String.format("SELECT * FROM %s WHERE pk = 10", baseOneTableName), null,
                    mysqlConnection, conn);
        }
    }

    @Test
    public void testBatchPrepareUpdate() throws SQLException {
        if (!PropertiesUtil.usePrepare()) {
            return;
        }
        loadData();
        final int UPDATE_COUNT = 10;
        if (isBroadCast) {
            // broadcast tables do not support batch prepare update
            return;
        }

        String updateSql = String.format("UPDATE %s SET %s = ? WHERE %s = ?", baseOneTableName,
            TableConstant.VARCHAR_TEST_COLUMN, PK_COLUMN_NAME);
        String traceUpdateSql = "TRACE " + updateSql;
        try (Connection connWithXDriver = ConnectionManager.getInstance()
            .newPolarDBXConnectionWithXDriver(polardbxOneDB, BATCH_PREPARE_CONN_PROP);
            PreparedStatement polardbxPsWithXDriver = connWithXDriver.prepareStatement(traceUpdateSql);
            PreparedStatement polardbxPsWithJdbc = tddlConnection.prepareStatement(traceUpdateSql);
            PreparedStatement mysqlPs = mysqlConnection.prepareStatement(updateSql)) {

            for (int i = 0; i < UPDATE_COUNT; i++) {
                String newValue = "test" + i;
                mysqlPs.setString(1, newValue);
                polardbxPsWithXDriver.setString(1, newValue);
                mysqlPs.setInt(2, i * 2 + 1);
                polardbxPsWithXDriver.setInt(2, i * 2 + 1);

                mysqlPs.addBatch();
                polardbxPsWithXDriver.addBatch();
            }
            mysqlPs.executeBatch();
            polardbxPsWithXDriver.executeBatch();

            ResultSet showTraceWithXDriver = JdbcUtil.executeQuery("SHOW TRACE", connWithXDriver);
            int updateRowsWithXDriver = 0;
            while (showTraceWithXDriver.next()) {
                updateRowsWithXDriver++;
            }
            Assert.assertEquals("Expect updated all batchPrepareUpdateRows in one trace",
                UPDATE_COUNT, updateRowsWithXDriver);

            // 验证数据更新成功且分片正确
            DataValidator
                .selectContentSameAssert(String.format("SELECT pk,varchar_test FROM %s", baseOneTableName), null,
                    mysqlConnection, tddlConnection);
            DataValidator
                .selectContentSameAssert(String.format("SELECT * FROM %s WHERE pk = 11", baseOneTableName), null,
                    mysqlConnection, tddlConnection);

            // 验证普通驱动无法做到 batch prepare 更新
            for (int i = 0; i < UPDATE_COUNT; i++) {
                String newValue = "test" + i;
                polardbxPsWithJdbc.setString(1, newValue);
                polardbxPsWithJdbc.setInt(2, i * 2 + 1);

                polardbxPsWithJdbc.addBatch();
            }
            polardbxPsWithJdbc.executeBatch();
            ResultSet showTraceWithJdbc = JdbcUtil.executeQuery("SHOW TRACE", tddlConnection);
            int updateRowsWithJdbc = 0;
            while (showTraceWithJdbc.next()) {
                updateRowsWithJdbc++;
            }
            // JDBC 只能做到一条条更新
            Assert.assertEquals("Expect updated one row in one trace", 1, updateRowsWithJdbc);
        }
    }
}
