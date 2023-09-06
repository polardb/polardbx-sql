package com.alibaba.polardbx.qatest.oss.dml;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * test load data to oss
 */
public class OssLoadDataTest extends BaseTestCase {
    private static String path = Thread.currentThread().getContextClassLoader().getResource(".").getPath();
    protected Connection mysqlConnection;
    protected Connection polardbXConnection;
    protected String baseOneTableName;

    public OssLoadDataTest() {
        this.baseOneTableName = "test_oss_load_data";
    }

    @After
    public void clearDataOnMysqlAndTddl() {
        String sql = "drop table if exists  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, polardbXConnection, sql, null);
    }

    @Before
    public void initTestDatabase() {
        this.mysqlConnection = getMysqlConnection();
        this.polardbXConnection = getPolardbxConnection();
    }

    @Test
    public void testLoadData() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`pk` bigint(11) NOT NULL,"
            + "`integer_test` int(11) DEFAULT NULL," + "`date_test` date DEFAULT NULL,"
            + "`timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,"
            + "`datetime_test` datetime DEFAULT NULL," + "`varchar_test` varchar(255) DEFAULT NULL,"
            + "`float_test` float DEFAULT NULL," + "PRIMARY KEY (`pk`)" + ") ";

        JdbcUtil.executeUpdate(mysqlConnection, create_table_sql);

        if (usingNewPartDb()) {
            create_table_sql += " SINGLE ";
        }
        create_table_sql += "engine = 'oss'";
        JdbcUtil.executeUpdate(polardbXConnection, create_table_sql);

        FileWriter fw = new FileWriter(path + "localdata.txt");
        for (int i = 0; i < 5; i++) {
            String str = String.format("%d,%d,2012-%02d-%02d,2012-%02d-%02d,2012-%02d-%02d,test%d,0.%d\r\n",
                i,
                i,
                i + 1,
                i + 1,
                i + 1,
                i + 1,
                i + 1,
                i + 1,
                i,
                i);
            fw.write(str);
        }
        fw.close();

        String sql =
            "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
                + " fields terminated by ',' "
                + "lines terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, polardbXConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, polardbXConnection);
    }
}
