package com.alibaba.polardbx.qatest.oss.dml;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlOrTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class OssExportDataTest extends BaseTestCase {
    static String ENABLE_SELECT_INTO_FILE = "/*+TDDL: ENABLE_SELECT_INTO_OUTFILE=true*/ ";
    private static String path = Thread.currentThread().getContextClassLoader().getResource(".").getPath();
    private String rootName;
    private String fileName = "oss_export_test.orc";
    protected Connection polardbXConnection;
    protected String baseOneTableName;

    public OssExportDataTest() {
        this.baseOneTableName = "test_oss_export_data";
    }

    @After
    public void clearDataAndCleanFile() {
        String sql = "drop table if exists  " + baseOneTableName;
        executeOnMysqlOrTddl(polardbXConnection, sql, null);
        FileUtils.deleteQuietly(new File(rootName + fileName));
    }

    @Before
    public void initTestDatabase() {
        this.polardbXConnection = getPolardbxConnection();
        rootName = Paths.get("../../spill/temp/").toAbsolutePath() + "/";
    }

    @Ignore("Not compatible with cdc")
    @Test
    public void testExportData() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`pk` bigint(11) NOT NULL,"
            + "`integer_test` int(11) DEFAULT NULL," + "`date_test` date DEFAULT NULL,"
            + "`timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,"
            + "`datetime_test` datetime DEFAULT NULL," + "`varchar_test` varchar(255) DEFAULT NULL,"
            + "`float_test` float DEFAULT NULL," + "PRIMARY KEY (`pk`)" + ") ";

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

        String loadSql =
            "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
                + " fields terminated by ',' "
                + "lines terminated by '\\r\\n'";
        executeOnMysqlOrTddl(polardbXConnection, loadSql, null);

        String exportSql = ENABLE_SELECT_INTO_FILE +
            "select pk, integer_test, timestamp_test, datetime_test, float_test  into outfile " + "'" + fileName + "' " + "from " + baseOneTableName;

        Statement stmt = polardbXConnection.createStatement();
        stmt.execute(exportSql);
    }
}
