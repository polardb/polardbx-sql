package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class LoadDataTraceTest extends BaseLoadDataTest {
    private static String PATH = Thread.currentThread().getContextClassLoader().getResource(".").getPath();

    private static final String FILE_NAME = "load_data_trace";

    private AtomicInteger testSequence = new AtomicInteger();

    public LoadDataTraceTest() {
        this.baseOneTableName = "test_load_data_trace";
    }

    @Before
    public void beforeDmlBaseTestCase() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
    }

    @Test
    public void testPhysicalSqlBatch() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL, primary key(col1_int)) ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&1\n1&1\n1&1\n2&2\n2&2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // check trace result
        sql = "/*+TDDL: ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'" + fileName + "'" + "into table "
            + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        ResultSet traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (traceResult.next()) {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") >= 2,
                    "physical sql should batched, but physical sql is " + phySql);
            } else {
                Assert.fail("trace result is empty");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // check trace result under batch mode
        sql = "/*+TDDL: LOAD_DATA_USE_BATCH_MODE=true ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
            + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (traceResult.next()) {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") == 1, "physical sql should not batched");
            } else {
                Assert.fail("trace result is empty");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void testSingleTablePhySql() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL) ";
        JdbcUtil.executeSuccess(mysqlConnection, create_table_sql);
        JdbcUtil.executeSuccess(tddlConnection, create_table_sql + "SINGLE");
        writeToFile("1&1\n1&1\n1&1\n2&2\n2&2\n2&2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL: ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'" + fileName + "'" + "into table "
            + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        ResultSet traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (traceResult.next()) {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") == 6, "physical sql should batched");
            } else {
                Assert.fail("trace result is empty");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        sql = "/*+TDDL: LOAD_DATA_USE_BATCH_MODE=true ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
            + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (traceResult.next()) {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") == 6, "physical sql should batched");
            } else {
                Assert.fail("trace result is empty");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void testBroadcastTablePhySql() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL) ";
        JdbcUtil.executeSuccess(mysqlConnection, create_table_sql);
        JdbcUtil.executeSuccess(tddlConnection, create_table_sql + "BROADCAST");
        writeToFile("1&1\n1&1\n1&1\n2&2\n2&2\n2&2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL: ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'" + fileName + "'" + "into table "
            + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        ResultSet traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") == 6, "physical sql should batched");
            } while (traceResult.next());
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        sql = "/*+TDDL: LOAD_DATA_USE_BATCH_MODE=true ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
            + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                Assert.assertTrue(StringUtils.countMatches(values, "(") == 6, "physical sql should batched");
            } while (traceResult.next());
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void testTableWithGsiPhySql() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL, key k_col2(col2_int)) ";
        JdbcUtil.executeSuccess(mysqlConnection, create_table_sql);
        JdbcUtil.executeSuccess(tddlConnection, create_table_sql);
        writeToFile("1&1\n1&1\n1&1\n2&2\n2&2\n2&2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        sql = "/*+TDDL: ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'" + fileName + "'" + "into table "
            + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        ResultSet traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            boolean anyBatch = false;
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                anyBatch |= (StringUtils.countMatches(values, "(") > 1);
            } while (traceResult.next());
            if (!anyBatch) {
                Assert.fail("should have batched physical sql");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        sql = "/*+TDDL: LOAD_DATA_USE_BATCH_MODE=true ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
            + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            boolean anyBatch = false;
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                String values = phySql.substring(StringUtils.indexOfIgnoreCase(phySql, "values"));
                anyBatch |= (StringUtils.countMatches(values, "(") > 1);
            } while (traceResult.next());
            if (!anyBatch) {
                Assert.fail("should have batched physical sql");
            }
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void testPureInsertMode() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL primary key,"
            + "`col2_int` int(11) DEFAULT NULL) ";
        JdbcUtil.executeSuccess(mysqlConnection, create_table_sql);
        JdbcUtil.executeSuccess(tddlConnection, create_table_sql);
        writeToFile("1&1\n2&1\n3&1\n4&2\n5&2\n6&2\n", fileName);

        String sql =
            "/*+TDDL: LOAD_DATA_PURE_INSERT_MODE=true ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
                + fileName + "'" + "into table " + tableName
                + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        ResultSet traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                Assert.assertTrue(!StringUtils.containsIgnoreCase(phySql, "ignore"),
                    "physical sql should not contain insert ignore");
            } while (traceResult.next());
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        sql = "/*+TDDL: LOAD_DATA_PURE_INSERT_MODE=false ENABLE_LOAD_DATA_TRACE=true */ load data local infile " + "'"
            + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        traceResult = JdbcUtil.executeQuery("show trace", tddlConnection);
        try {
            if (!traceResult.next()) {
                Assert.fail("trace result is empty");
            }
            do {
                String phySql = traceResult.getString("STATEMENT");
                Assert.assertTrue(StringUtils.containsIgnoreCase(phySql, "ignore"),
                    "physical sql should be insert ignore");
            } while (traceResult.next());
        } catch (SQLException e) {
            Assert.fail("extract trace result failed");
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
