package com.alibaba.polardbx.qatest.dal.mysqltools;

import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteCHNTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.selectBaseOneTable;

@NotThreadSafe
@RunWith(Parameterized.class)
public class MysqlDumpTest extends DirectConnectionBaseTestCase {

    private final String tableName;

    private static final String DUMP_TABLESPACE_SQL1_57_TEMPLATE = "SELECT "
        + "    LOGFILE_GROUP_NAME, "
        + "    FILE_NAME, "
        + "    TOTAL_EXTENTS, "
        + "    INITIAL_SIZE, "
        + "    ENGINE, "
        + "    EXTRA "
        + "FROM "
        + "    INFORMATION_SCHEMA.FILES "
        + "WHERE "
        + "    FILE_TYPE = 'UNDO LOG' "
        + "    AND FILE_NAME IS NOT NULL "
        + "    AND LOGFILE_GROUP_NAME IS NOT NULL "
        + "    AND LOGFILE_GROUP_NAME IN ( "
        + "        SELECT "
        + "            DISTINCT LOGFILE_GROUP_NAME "
        + "        FROM "
        + "            INFORMATION_SCHEMA.FILES "
        + "        WHERE "
        + "            FILE_TYPE = 'DATAFILE' "
        + "            AND TABLESPACE_NAME IN ( "
        + "                SELECT "
        + "                    DISTINCT TABLESPACE_NAME "
        + "                FROM "
        + "                    INFORMATION_SCHEMA.PARTITIONS "
        + "                WHERE "
        + "                    TABLE_SCHEMA = '%s' "
        + "                    AND TABLE_NAME IN (%s) "
        + "            ) "
        + "    ) "
        + "GROUP BY "
        + "    LOGFILE_GROUP_NAME, "
        + "    FILE_NAME, "
        + "    ENGINE, "
        + "    TOTAL_EXTENTS, "
        + "    INITIAL_SIZE "
        + "ORDER BY "
        + "    LOGFILE_GROUP_NAME";

    private static final String DUMP_TABLESPACE_SQL2_57_TEMPLATE = "SELECT "
        + "    DISTINCT TABLESPACE_NAME, "
        + "    FILE_NAME, "
        + "    LOGFILE_GROUP_NAME, "
        + "    EXTENT_SIZE, "
        + "    INITIAL_SIZE, "
        + "    ENGINE "
        + "FROM "
        + "    INFORMATION_SCHEMA.FILES "
        + "WHERE "
        + "    FILE_TYPE = 'DATAFILE' "
        + "    AND TABLESPACE_NAME IN ( "
        + "        SELECT "
        + "            DISTINCT TABLESPACE_NAME "
        + "        FROM "
        + "            INFORMATION_SCHEMA.PARTITIONS "
        + "        WHERE "
        + "            TABLE_SCHEMA = '%s' "
        + "            AND TABLE_NAME IN (%s) "
        + "    ) "
        + "ORDER BY "
        + "    TABLESPACE_NAME, "
        + "    LOGFILE_GROUP_NAME";

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        List<String[]> res = new ArrayList<>();
        res.addAll(Arrays.asList(selectBaseOneTable()));
        res.addAll(Arrays.asList(ExecuteCHNTableName.allBaseTypeOneTable(ExecuteCHNTableName.UPDATE_DELETE_BASE)));
        return res;
    }

    public MysqlDumpTest(String tableName) {
        this.tableName = tableName;
    }

    @BeforeClass
    public static void setupConnection() {
        JdbcUtil.executeSuccess(getPolardbxConnection0(), "SET GLOBAL COMPATIBLE_CHARSET_VARIABLES =true;");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Test
    public void testMySQLDump57() throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("show create table `%s`", tableName), tddlConnection);
        Assert.assertTrue(String.format("Table %s should exist", tableName), resultSet.next());

        String tableNameFromShow = resultSet.getString("Table");
        Assert.assertEquals(this.tableName, tableNameFromShow);

        JdbcUtil.executeSuccess(tddlConnection, "/*!40100 SET @@SQL_MODE='' */");
        JdbcUtil.executeSuccess(tddlConnection, "/*!40103 SET TIME_ZONE='+00:00' */");
        JdbcUtil.executeSuccess(tddlConnection, "SHOW VARIABLES LIKE 'gtid\\_mode'");
        String dumpTablespaceSql1 =
            String.format(DUMP_TABLESPACE_SQL1_57_TEMPLATE, PropertiesUtil.polardbXDBName1(false),
                String.format("'%s'", this.tableName));
        String dumpTablespaceSql2 =
            String.format(DUMP_TABLESPACE_SQL2_57_TEMPLATE, PropertiesUtil.polardbXDBName1(false),
                String.format("'%s'", this.tableName));
        JdbcUtil.executeSuccess(tddlConnection, dumpTablespaceSql1);
        JdbcUtil.executeSuccess(tddlConnection, dumpTablespaceSql2);
        JdbcUtil.executeSuccess(tddlConnection, "/*!40100 SET @@SQL_MODE='' */");
        JdbcUtil.executeSuccess(tddlConnection, "SHOW VARIABLES LIKE 'ndbinfo\\_version'");
        JdbcUtil.executeSuccess(tddlConnection, String.format("SHOW TABLES LIKE '%s'", this.tableName));
        JdbcUtil
            .executeSuccess(tddlConnection, String.format("LOCK TABLES `%s` READ /*!32311 LOCAL */", this.tableName));
        JdbcUtil.executeSuccess(tddlConnection, String.format("show table status like '%s'", this.tableName));
        JdbcUtil.executeSuccess(tddlConnection, "SET SQL_QUOTE_SHOW_CREATE=1");
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION character_set_results = 'binary'");

        /*
         * 验证 binary 结果集返回中英文表名的正确性
         * 与MySQL结果做对比
         */
        resultSet = JdbcUtil.executeQuery(String.format("show create table `%s`", this.tableName), tddlConnection);
        Assert.assertTrue("Table %s should exist", resultSet.next());
        byte[] tableNameFromPolarXDump = resultSet.getBytes("Table");
        JdbcUtil.executeSuccess(mysqlConnection, "SET SESSION character_set_results = 'binary'");
        resultSet = JdbcUtil.executeQuery(String.format("show create table `%s`", this.tableName), mysqlConnection);
        Assert.assertTrue("Table %s should exist", resultSet.next());
        byte[] tableNameFromMysqlDump = resultSet.getBytes("Table");
        Assert.assertArrayEquals(tableNameFromMysqlDump, tableNameFromPolarXDump);

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION character_set_results = 'utf8'");
        JdbcUtil.executeSuccess(tddlConnection, String.format("show fields from `%s`", this.tableName));
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(" SELECT /*!40001 SQL_NO_CACHE */ * FROM `%s`", this.tableName));
        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION character_set_results = 'utf8'");
        JdbcUtil.executeSuccess(tddlConnection, "UNLOCK TABLES");

        JdbcUtil.close(resultSet);
    }

    @After
    public void restoreConnection() {
        try {
            JdbcUtil.executeSuccess(tddlConnection, "SET SESSION character_set_results = 'utf8'");
            JdbcUtil.executeSuccess(tddlConnection, "UNLOCK TABLES");
        } finally {
            JdbcUtil.executeSuccess(mysqlConnection, "SET SESSION character_set_results = 'utf8'");
            JdbcUtil.executeSuccess(mysqlConnection, "UNLOCK TABLES");
        }
    }

    @AfterClass
    public static void afterClass() {
        JdbcUtil.executeSuccess(getPolardbxConnection0(), "SET GLOBAL COMPATIBLE_CHARSET_VARIABLES =false;");
    }

}
