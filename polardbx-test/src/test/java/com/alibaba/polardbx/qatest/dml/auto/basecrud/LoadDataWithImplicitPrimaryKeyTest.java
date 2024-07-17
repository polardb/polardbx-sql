package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class LoadDataWithImplicitPrimaryKeyTest extends BaseLoadDataTest {
    @Parameterized.Parameters(name = "{index}:{0}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            isAutoMode());
    }

    public static Object[] isAutoMode() {
        return new Boolean[] {
            Boolean.TRUE,
            Boolean.FALSE
        };
    }

    boolean isAutoMode;

    private static final String FILE_NAME = "load_data_implicit_Key";

    private AtomicInteger testSequence = new AtomicInteger();

    public LoadDataWithImplicitPrimaryKeyTest(Object isAutoMode) {
        this.isAutoMode = (Boolean) isAutoMode;
        this.baseOneTableName = "test_load_data_implicit_primary_key";
    }

    @Before
    public void beforeDmlBaseTestCase() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
    }

    @Test
    public void normalCase() {
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
            + "`col2_int` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&1\n1&1\n1&1\n2&2\n2&2\n2&2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void loadDataWithColumnList() {
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
            + "`col2_int` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1\n1\n1\n2\n2\n2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n' (col1_int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Test
    public void loadFileHasImplicitColumn() {
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
            + "`col2_int` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&1&1\n1&1&1\n1&1&1\n2&2&2\n2&2&2\n2&2&2\n", fileName);

        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeFailed(tddlConnection, sql, "The column's length is 3, while the value's length is 4");

        tddlConnection = getPolardbxConnection();

        // method 1: use load data mask to hide this implicit column
        sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n' (col1_int, col2_int, @x)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // method 2: disable LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN to load
        sql =
            "/*+TDDL: LOAD_DATA_AUTO_FILL_AUTO_INCREMENT_COLUMN=false*/ load data local infile " + "'" + fileName + "'"
                + "into table " + tableName
                + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    /**
     * Notice: not same with mysql(5.7.38) here, and will not support this ugly design
     * one example is:
     * <p>
     * RDS:
     * <p>
     * CREATE TABLE `t1` (
     * `c1` int(11) DEFAULT NULL,
     * `c2` int(11) DEFAULT NULL
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
     * <p>
     * load file with two empty line to t1, and result is
     * +------+------+
     * | c1   | c2   |
     * +------+------+
     * |    0 | NULL |
     * |    0 | NULL |
     * +------+------+
     * <p>
     * weird, ugly and seems meaningless
     * <p>
     * PolarDB-X:
     * nothing will be insert into this table
     */
    @Test
    public void emptyLine() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("\n\n", fileName);

        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        try (ResultSet rs = JdbcUtil.executeQuery(selectSql, tddlConnection)) {
            if (rs.next()) {
                Assert.fail("should get empty result");
            }
        } catch (SQLException e) {
            Assert.fail("query execute on polarx failed, sql is " + selectSql);
        }

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Override
    public boolean usingNewPartDb() {
        return isAutoMode;
    }
}
