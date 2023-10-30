package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class LoadDataAutoPartitionTest extends BaseLoadDataTest {
    private static final String FILE_NAME = "load_data_primary_key";

    private AtomicInteger testSequence = new AtomicInteger();

    public LoadDataAutoPartitionTest() {
        this.baseOneTableName = "test_load_data_primary_key";
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

        String create_table_sql = "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL primary key,"
            + "`col2_int` int(11) DEFAULT NULL) ENGINE=InnoDB";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&2\n1&3\n1&4\n2&2\n2&2\n2&2\n", fileName);

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
    public void primaryKeyIsAutoInc() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql =
            "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL auto_increment,"
                + "`col2_int` int(11) DEFAULT NULL, primary key(col1_int)) ENGINE=InnoDB";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&2\n1&3\n1&4\n2&2\n2&2\n2&2\n", fileName);

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
    public void primaryKeyWithAutoInc() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql =
            "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
                + "`col2_int` int(11) DEFAULT NULL auto_increment, primary key(col1_int), key(col2_int) ) ENGINE=InnoDB";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        writeToFile("1&2\n144&4\n2&2\n23&2\n22&2\n", fileName);

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
    public void tableWithGsi() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql =
            "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL auto_increment,"
                + "`col2_int` int(11) DEFAULT NULL, `col3_char` varchar(11) DEFAULT NULL, primary key(col1_int), index `g_i_col3`(col3_char) ) ENGINE=InnoDB";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        // one column
        writeToFile("1,2,a\n6,1,b\n7,2,c\n2,1,xx\n3,2,yy\n4,1,zz\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    /**
     * although we can support this, this is not suggest
     */
    @Test
    public void autoFillPrimaryKey() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }

        // prepare data
        int sequence = testSequence.getAndIncrement();
        String tableName = baseOneTableName + sequence;
        String fileName = PATH + FILE_NAME + sequence;
        dropTable(tableName);
        deleteFile(fileName);

        String create_table_sql =
            "CREATE TABLE `" + tableName + "` (" + "`col1_int` bigint(11) NOT NULL auto_increment,"
                + "`col2_int` int(11) DEFAULT NULL, primary key(col1_int)) ENGINE=InnoDB";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        // one column
        writeToFile("1\n1\n1\n2\n2\n2\n", fileName);

        // check load data result
        String sql = "load data local infile " + "'" + fileName + "'" + "into table " + tableName
            + " FIELDS TERMINATED BY '&' LINES TERMINATED BY '\\n' (col2_int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + tableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);

        // clear data
        dropTable(tableName);
        deleteFile(fileName);
    }

    @Before
    public void beforeDmlBaseTestCase() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
