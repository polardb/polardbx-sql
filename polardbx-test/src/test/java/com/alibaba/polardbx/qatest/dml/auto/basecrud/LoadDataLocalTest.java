/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlOrTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Prepare does not support LoadData
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html#packet-COM_STMT_PREPARE">COM_STMT_PREPARE</a>
 */
public class LoadDataLocalTest extends BaseTestCase {
    private static String path = Thread.currentThread().getContextClassLoader().getResource(".").getPath();

    protected Connection mysqlConnection;
    protected Connection tddlConnection;
    protected String baseOneTableName;

    public LoadDataLocalTest() {
        this.baseOneTableName = "test_test_load_data";
    }

    @Before
    public void beforeDmlBaseTestCase() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(mysqlConnection, "set global local_infile = on;");
    }

    @After
    public void clearDataOnMysqlAndTddl() {
        String sql = "drop table if exists  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @After
    public void deleteFile() {
        File file = new File(path + "localdata.txt");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testLoadDataIgnoreLines() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("1&1\r\n2&2\r\n\r\n3&3\r\n4&4");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " FIELDS TERMINATED BY '&' "
            + "LINES TERMINATED BY '\\r\\n'  ignore 3 lines";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataIgnoreNullLines() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("1,1\r\n2,2\r\n\r\n3,3\r\n4,");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " FIELDS TERMINATED BY ',' "
            + "LINES TERMINATED BY '\\r\\n'  ignore 3 lines";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataEnclosedBy() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("\"1\",\"1\"\r\n2,\"2\"\r\n\"3\",\"3\"\r\n4,\"4\"");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' enclosed by '\"' "
            + "lines terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataWithColumnList() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "`col3_char` char(255) DEFAULT NULL," +
            "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("1,1\r\n2,2\r\n3,3\r\n4,4\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' "
            + "lines terminated by '\\r\\n' (col1_int, col2_int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
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
        JdbcUtil.executeUpdate(tddlConnection, create_table_sql);

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
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataLineStartingBy() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql =
            "CREATE TABLE `" + baseOneTableName + "` (" + "`varchar_test` varchar(255) NOT NULL,"
                + "`float_test` float DEFAULT NULL," + "PRIMARY KEY (`varchar_test`)"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        FileWriter fw = new FileWriter(path + "localdata.txt");
        for (int i = 0; i < 5; i++) {
            String str = String.format("测试-test%d,0.%d\r\n", i, i);
            fw.write(str);
        }
        fw.close();

        String sql =
            "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName +
                " fields terminated by ',' " + "lines starting by '测试-' terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataWithCharset() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql =
            "CREATE TABLE `" + baseOneTableName + "` (" + "`varchar_test` varchar(255) NOT NULL,"
                + "`float_test` float DEFAULT NULL," + "PRIMARY KEY (`varchar_test`)"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + "localdata.txt", true), "UTF-8"));
        for (int i = 0; i < 5; i++) {
            String str = String.format("测试-test%d-中文,0.%d\r\n", i, i);
            writer.write(str);
        }
        writer.close();

        String sql =
            "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
                + " character set utf8mb4 " +
                " fields terminated by ',' " + "lines starting by '测试-' terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    // 目前仅支持’\‘作为转义字符
    @Test
    public void testLoadDataEscapedBy() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql =
            "CREATE TABLE `" + baseOneTableName + "` (" + "`pk` bigint(11) NOT NULL,"
                + "`varchar_test` varchar(255) DEFAULT NULL,"
                + "`float_test` float DEFAULT NULL," + "PRIMARY KEY (`pk`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        FileWriter fw = new FileWriter(path + "localdata.txt");
        for (int i = 0; i < 5; i++) {
            String str = String.format("%d,8\ts\\r\\t\\str *\\\\\\\\r\\\\n* \\\\t,0.%d\r\n", i, i, i);
            fw.write(str);
        }
        fw.close();

        String sql =
            "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName +
                " fields terminated by ',' escaped by '\\\\' " + "lines terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataIntoBroadTable() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        JdbcUtil.updateData(mysqlConnection, create_table_sql, null);

        create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=gbk BROADCAST";
        JdbcUtil.updateData(tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            for (int i = 0; i < 20; i++) {
                String str = String.format("%d,%d\r\n", i, i + 1);
                fw.write(str);
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' "
            + "lines terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataIntoMultiDB() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        JdbcUtil.updateData(mysqlConnection, create_table_sql, null);

        create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "PRIMARY KEY (`col1_int`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=gbk ";

        if (usingNewPartDb()) {
            create_table_sql += " partition by hash(col1_int)";
        } else {
            create_table_sql += " dbpartition by hash(col1_int) tbpartition by hash(col1_int) tbpartitions 3";
        }

        JdbcUtil.updateData(tddlConnection, create_table_sql, null);

        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            for (int i = 0; i < 20; i++) {
                String str = String.format("%d,%d\r\n", i, i + 1);
                fw.write(str);
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' "
            + "lines terminated by '\\r\\n'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName + " order by col1_int";
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testTableWithGsi() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL, `col3_int` int(11) DEFAULT NULL, " + "PRIMARY KEY (`col1_int`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        JdbcUtil.updateData(mysqlConnection, create_table_sql, null);

        create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL, `col3_int` int(11) DEFAULT NULL, "
            + "PRIMARY KEY (`col1_int`), ";

        if (usingNewPartDb()) {
            create_table_sql +=
                "GLOBAL INDEX `g_test_test_col2`(`col2_int`) partition by key(`col2_int`) partitions 3"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=gbk partition by key(col1_int) partitions 3";
        } else {
            create_table_sql +=
                "GLOBAL INDEX `g_test_test_col2`(`col2_int`) dbpartition by hash(`col2_int`) tbpartition by hash(`col2_int`) tbpartitions 3"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=gbk dbpartition by hash(col1_int) tbpartition by hash(col1_int) tbpartitions 3";
        }

        JdbcUtil.updateData(tddlConnection, create_table_sql, null);

        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            for (int i = 0; i < 20; i++) {
                String str = String.format("%d,%d,%d\r\n", i, i + 1, i - 1);
                fw.write(str);
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' "
            + "lines terminated by '\\r\\n' (col2_int, col1_int, col3_int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName + " order by col1_int";
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLoadDataWithColumnMask() throws Exception {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        clearDataOnMysqlAndTddl();
        String create_table_sql = "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL,"
            + "`col2_int` int(11) DEFAULT NULL," + "`col3_int` int(11) DEFAULT NULL," +
            "PRIMARY KEY (`col1_int`)" + ") ENGINE=InnoDB DEFAULT CHARSET=gbk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, create_table_sql, null);
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("1,2,3\r\n2,3,4\r\n3,4,5\r\n4,5,6\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sql = "load data local infile " + "'" + path + "localdata.txt' " + "into table " + baseOneTableName
            + " fields terminated by ',' "
            + "lines terminated by '\\r\\n' (col1_int, @col2_int, col3_int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        String selectSql = "select * from " + baseOneTableName;
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void prepareLoadDataFile() {
        try {
            FileWriter fw = new FileWriter(path + "localdata.txt");
            fw.write("1,2\r\n2,3\r\n3,4\r\n4,5\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAutoFill() {
        if (PropertiesUtil.usePrepare()) {
            return;
        }
        prepareLoadDataFile();
        String prefixOfCreateTable =
            "CREATE TABLE `" + baseOneTableName + "` (" + "`col1_int` bigint(11) NOT NULL AUTO_INCREMENT,"
                + "`col2_int` int(11) DEFAULT NULL," + "`col3_int` int(11) DEFAULT NULL," +
                "PRIMARY KEY (`col1_int`)" + ") ";

        String[] suffixOfCreateTables = null;

        if (!usingNewPartDb()) {
            suffixOfCreateTables = new String[] {
                "", "BROADCAST", "dbpartition by hash(col1_int)", "tbpartition by hash(col1_int) tbpartitions 3",
                "dbpartition by hash(col1_int) tbpartition by hash(col1_int) tbpartitions 3"};
        } else {
            suffixOfCreateTables = new String[] {
                "BROADCAST", "partition by key(col1_int) partitions 3"};
        }

        for (String suffixOfCreateTable : suffixOfCreateTables) {
            clearDataOnMysqlAndTddl();
            prepareLoadDataFile();
            String create_table_sql = prefixOfCreateTable + suffixOfCreateTable;
            executeOnMysqlOrTddl(mysqlConnection, prefixOfCreateTable, null);
            executeOnMysqlOrTddl(tddlConnection, create_table_sql, null);
            String sql = "/*+ load_data_auto_fill_auto_increment_column=true */ load data local infile " + "'" + path
                + "localdata.txt' " + "into table " + baseOneTableName
                + " fields terminated by ',' " + "lines terminated by '\\r\\n' (col3_int, col2_int)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
            String selectSql = "select col2_int, col3_int from " + baseOneTableName + " order by col2_int";
            selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
