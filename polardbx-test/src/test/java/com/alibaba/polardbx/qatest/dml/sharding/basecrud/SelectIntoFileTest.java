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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlOrTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@Ignore("现在的测试框架不太好兼容这个case，等后面再搞吧 -> 越寒")
public class SelectIntoFileTest extends ReadBaseTestCase {
    static String ENABLE_SELECT_INTO_FILE = "/*+TDDL: ENABLE_SELECT_INTO_OUTFILE=true*/ ";
    String tableName = "test_select_into_outfile";
    String fileName = "select_into_test.txt";
    String correctFileName = "select_into_correct.txt";
    Statement stmt = null;
    String rootName = null;

    @Before
    public void setUp() throws Exception {
        stmt = tddlConnection.createStatement();
        System.out.println(FileConfig.getInstance().getSpillerTempPath().toAbsolutePath());
//        rootName = Paths.get("../../../spill/temp/").toAbsolutePath() + "/";
        rootName = FileConfig.getInstance().getSpillerTempPath().toAbsolutePath() + "/";
    }

    @After
    public void tearDown()
        throws Exception {
        if (stmt != null) {
            try {
                stmt.execute(String.format("drop table if exists %s;", tableName));
            } finally {
                stmt.close();
            }
        }
        cleanFiles();
    }

    private boolean compareAndDeleteFile() throws IOException {
        FileInputStream fis = new FileInputStream(rootName + fileName);
        String testMd5 = DigestUtils.md5Hex(IOUtils.toByteArray(fis));
        IOUtils.closeQuietly(fis);
        FileInputStream fis2 = new FileInputStream(rootName + correctFileName);
        String correctMd5 = DigestUtils.md5Hex(IOUtils.toByteArray(fis2));
        IOUtils.closeQuietly(fis2);
        FileUtils.deleteQuietly(new File(rootName + fileName));
        return correctMd5.equals(testMd5);
    }

    private void cleanFiles() {
        FileUtils.deleteQuietly(new File(rootName + fileName));
        FileUtils.deleteQuietly(new File(rootName + correctFileName));
    }

    private void writeIntoFile(String fileName, byte[] bytes) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }
        OutputStream outputStream = new FileOutputStream(file, false);
        outputStream.write(bytes);
        outputStream.close();
    }

    @Test
    public void TestCharType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format(
            "CREATE TABLE %s ( `a` char(10) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
            tableName));
        stmt.execute(String.format("INSERT INTO %s VALUES ('a'), ('b'), ('c'), ('d');", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("SELECT * FROM %s INTO OUTFILE '%s';", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "a\nb\nc\nd\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestEnumType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("CREATE TABLE %s (col ENUM ('value1','value2','value3'));", tableName));
        stmt.execute(String.format("INSERT INTO %s values ('value1'), ('value2');", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("SELECT * FROM %s INTO OUTFILE '%s';", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "value1\nvalue2\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestJsonType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s ( v json);", tableName));
        stmt.execute(String.format("insert into %s values "
            + "('{\"id\": 1, \"name\": \"aaa\"}'), "
            + "('{\"id\": 2, \"name\": \"xxx\"}')", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("SELECT * FROM %s INTO OUTFILE '%s'", tableName, fileName));
        writeIntoFile(rootName + correctFileName,
            "{\"id\": 1, \"name\": \"aaa\"}\n{\"id\": 2, \"name\": \"xxx\"}\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestUnsignedTinyInt() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (v tinyint unsigned)", tableName));
        stmt.execute(String.format("insert into %s values (0), (1)", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("SELECT * FROM %s INTO OUTFILE '%s'", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "0\n1\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestFloatType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String
            .format("create table %s (id float(16,2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
                tableName));
        stmt.execute(String.format("insert into %s values (3.4), (1), (10.1), (2.00)", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("SELECT * FROM %s ORDER BY id INTO OUTFILE '%s'", tableName,
                fileName));
        writeIntoFile(rootName + correctFileName, "1.0\n2.0\n3.4\n10.1\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestYearType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (time1 year(4) default '2030')", tableName));
        stmt.execute(String.format("insert into %s values (2010), (2011), (2012);", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n';",
            tableName, fileName));
        writeIntoFile(rootName + correctFileName, "2010\n2011\n2012\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestSelectIntoOutfileFromTable() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format(
            "create table %s (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)",
            tableName));
        stmt.execute(String
            .format("insert into %s values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '[1]')",
                tableName));
        stmt.execute(String
            .format("insert into %s values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '[1,2]')",
                tableName));
        stmt.execute(String
            .format("insert into %s values (null, null, null, null, '2000-03-03', '03:03:03', '[1,2,3]')",
                tableName));
        stmt.execute(
            String.format("insert into %s values (4, 4.4, 0.4, 'd', null, '04:04:04', null)", tableName));

        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("select * from %s into outfile '%s';", tableName, fileName));
        // FIXME prepare模式感觉time里面有时区问题，mysql client无此问题。。。
//        writeIntoFile(rootName + correctFileName,
//            ("1\t1.1\t0.10000\ta\t2000-01-01 00:00:00.0\t2001-01-01 00:00:00.0\t[1]\n"
//                + "2\t2.2\t0.20000\tb\t2000-02-02 00:00:00.0\t2002-02-02 00:00:00.0\t[1, 2]\n"
//                + "\\N\t\\N\t\\N\t\\N\t2000-03-03 00:00:00.0\t2003-03-03 00:00:00.0\t[1, 2, 3]\n"
//                + "4\t4.4\t0.40000\td\t\\N\t2004-04-04 00:00:00.0\t\\N\n").getBytes());
        writeIntoFile(rootName + correctFileName,
            ("1\t1.1\t0.10000\ta\t2000-01-01 00:00:00\t2001-01-01 00:00:00\t[1]\n"
                + "2\t2.2\t0.20000\tb\t2000-02-02 00:00:00\t2002-02-02 00:00:00\t[1, 2]\n"
                + "\\N\t\\N\t\\N\t\\N\t2000-03-03 00:00:00\t2003-03-03 00:00:00\t[1, 2, 3]\n"
                + "4\t4.4\t0.40000\td\t\\N\t2004-04-04 00:00:00\t\\N\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String
            .format(ENABLE_SELECT_INTO_FILE
                    + "select * from %s into outfile '%s' fields terminated by ',' enclosed by '\"' escaped by '#';",
                tableName, fileName));
//        writeIntoFile(rootName + correctFileName,
//            ("\"1\",\"1.1\",\"0.10000\",\"a\",\"2000-01-01 00:00:00.0\",\"2001-01-01 00:00:00.0\",\"[1]\"\n"
//                + "\"2\",\"2.2\",\"0.20000\",\"b\",\"2000-02-02 00:00:00.0\",\"2002-02-02 00:00:00.0\",\"[1, 2]\"\n"
//                + "#N,#N,#N,#N,\"2000-03-03 00:00:00.0\",\"2003-03-03 00:00:00.0\",\"[1, 2, 3]\"\n"
//                + "\"4\",\"4.4\",\"0.40000\",\"d\",#N,\"2004-04-04 00:00:00.0\",#N\n").getBytes());
        writeIntoFile(rootName + correctFileName,
            ("\"1\",\"1.1\",\"0.10000\",\"a\",\"2000-01-01 00:00:00\",\"2001-01-01 00:00:00\",\"[1]\"\n"
                + "\"2\",\"2.2\",\"0.20000\",\"b\",\"2000-02-02 00:00:00\",\"2002-02-02 00:00:00\",\"[1, 2]\"\n"
                + "#N,#N,#N,#N,\"2000-03-03 00:00:00\",\"2003-03-03 00:00:00\",\"[1, 2, 3]\"\n"
                + "\"4\",\"4.4\",\"0.40000\",\"d\",#N,\"2004-04-04 00:00:00\",#N\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' fields terminated by ',' optionally enclosed by '\"' escaped by '#';",
            tableName, fileName));
//        writeIntoFile(rootName + correctFileName,
//            ("1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00.0\",\"2001-01-01 00:00:00.0\",\"[1]\"\n"
//                + "2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00.0\",\"2002-02-02 00:00:00.0\",\"[1, 2]\"\n"
//                + "#N,#N,#N,#N,\"2000-03-03 00:00:00.0\",\"2003-03-03 00:00:00.0\",\"[1, 2, 3]\"\n"
//                + "4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00.0\",#N\n").getBytes());
        writeIntoFile(rootName + correctFileName,
            ("1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00\",\"2001-01-01 00:00:00\",\"[1]\"\n"
                + "2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00\",\"2002-02-02 00:00:00\",\"[1, 2]\"\n"
                + "#N,#N,#N,#N,\"2000-03-03 00:00:00\",\"2003-03-03 00:00:00\",\"[1, 2, 3]\"\n"
                + "4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00\",#N\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' fields terminated by ',' optionally enclosed by '\"' escaped by '#' lines terminated by '<<<\\n';",
            tableName, fileName));
//        writeIntoFile(rootName + correctFileName,
//            ("1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00.0\",\"2001-01-01 00:00:00.0\",\"[1]\"<<<\n"
//                + "2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00.0\",\"2002-02-02 00:00:00.0\",\"[1, 2]\"<<<\n"
//                + "#N,#N,#N,#N,\"2000-03-03 00:00:00.0\",\"2003-03-03 00:00:00.0\",\"[1, 2, 3]\"<<<\n"
//                + "4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00.0\",#N<<<\n").getBytes());
        writeIntoFile(rootName + correctFileName,
            ("1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00\",\"2001-01-01 00:00:00\",\"[1]\"<<<\n"
                + "2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00\",\"2002-02-02 00:00:00\",\"[1, 2]\"<<<\n"
                + "#N,#N,#N,#N,\"2000-03-03 00:00:00\",\"2003-03-03 00:00:00\",\"[1, 2, 3]\"<<<\n"
                + "4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00\",#N<<<\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' fields terminated by ',' optionally enclosed by '\"' escaped by '#' lines starting by '**' terminated by '<<<\\n';",
            tableName, fileName));
//        writeIntoFile(rootName + correctFileName,
//            ("**1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00.0\",\"2001-01-01 00:00:00.0\",\"[1]\"<<<\n"
//                + "**2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00.0\",\"2002-02-02 00:00:00.0\",\"[1, 2]\"<<<\n"
//                + "**#N,#N,#N,#N,\"2000-03-03 00:00:00.0\",\"2003-03-03 00:00:00.0\",\"[1, 2, 3]\"<<<\n"
//                + "**4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00.0\",#N<<<\n").getBytes());
        writeIntoFile(rootName + correctFileName,
            ("**1,1.1,0.10000,\"a\",\"2000-01-01 00:00:00\",\"2001-01-01 00:00:00\",\"[1]\"<<<\n"
                + "**2,2.2,0.20000,\"b\",\"2000-02-02 00:00:00\",\"2002-02-02 00:00:00\",\"[1, 2]\"<<<\n"
                + "**#N,#N,#N,#N,\"2000-03-03 00:00:00\",\"2003-03-03 00:00:00\",\"[1, 2, 3]\"<<<\n"
                + "**4,4.4,0.40000,\"d\",#N,\"2004-04-04 00:00:00\",#N<<<\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestSelectIntoOutfileConstant() throws SQLException, IOException {
        stmt.execute(ENABLE_SELECT_INTO_FILE +
            String.format("select 1, 2, 3, '4', '5', '6', 7.7, 8.8, 9.9, null, '越寒' into outfile '%s'", fileName));
        writeIntoFile(rootName + correctFileName, "1\t2\t3\t4\t5\t6\t7.7\t8.8\t9.9\t\\N\t越寒\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select 1e10, 1e20, 1.234567e8, 0.000123e3, 1.01234567890123456789, 123456789e-10 into outfile '%s'",
            fileName));
        writeIntoFile(rootName + correctFileName,
            "10000000000\t1.0E20\t123456700\t0.123\t1.01234567890123456789\t0.0123456789\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestDeliminators() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("CREATE TABLE %s (`a` varbinary(20) DEFAULT NULL,`b` int DEFAULT NULL)", tableName));
        stmt.execute(String.format("insert into %s values (null, null)", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format("select * from %s into outfile '%s' fields escaped by '';",
            tableName, fileName));
        writeIntoFile(rootName + correctFileName, "NULL\tNULL\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("delete from %s", tableName));
        stmt.execute(String.format("insert into %s values ('d\",\"e\",', 3), ('\\\\', 2)", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'",
            tableName, fileName));
        writeIntoFile(rootName + correctFileName, "\"d\\\",\\\"e\\\",\",\"3\"\n\"\\\\\",\"2\"\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("delete from %s", tableName));
        stmt.execute(String.format("insert into %s values ('a\tb', 1)", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' escaped by '\t' LINES TERMINATED BY '\\n'",
            tableName, fileName));
        writeIntoFile(rootName + correctFileName, "\"a\t\tb\",\"1\"\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("delete from %s", tableName));
        stmt.execute(String.format("insert into %s values ('d\",\"e\",', 1)", tableName));
        stmt.execute(String.format("insert into %s values (unhex(\"00\"), 2)", tableName));
        stmt.execute(String.format("insert into %s values (\"\\r\\n\\b\\Z\\t\", 3)", tableName));
        stmt.execute(String.format("insert into %s values (null, 4)", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'",
            tableName, fileName));
        // 行终止符应当被转义
        writeIntoFile(rootName + correctFileName,
            ("\"d\\\",\\\"e\\\",\",\"1\"\n" + "\"\\0\",\"2\"\n"
                + "\"\r\\\n\b\032\t\",\"3\"\n" + "\\N,\"4\"\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (s char(10), b bit(48), bb blob(6))", tableName));
        stmt.execute(String
            .format("insert into %s values ('\\0\\b\\n\\r\\t\\Z', '\\0\\b\\n\\r\\t\\Z', unhex('00080A0D091A'))",
                tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("select * from %s into outfile '%s'", tableName, fileName));
        writeIntoFile(rootName + correctFileName,
            ("\\0\b\\\n\r\\\t\032\t" + "\\0\b\\\n\r\\\t\032\t"
                + "\\0\b\\\n\r\\\t\032\n").getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (a varchar(10), b varchar(10), c varchar(10))", tableName));
        stmt.execute(String.format("insert into %s values (unhex('00'), '\\0', '\\0')", tableName));
        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("select * from %s into outfile '%s'", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "\\0\t\\0\t\\0\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(
            ENABLE_SELECT_INTO_FILE + String.format("select * from %s into outfile '%s' fields enclosed by '\"'",
                tableName, fileName));
        writeIntoFile(rootName + correctFileName, "\"\\0\"\t\"\\0\"\t\"\\0\"\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (a char(10), b char(10), c char(10))", tableName));
        stmt.execute(String.format("insert into %s values ('abcd', 'abcd', 'abcd')", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s into outfile '%s' fields terminated by 'a-' lines terminated by 'b--'", tableName,
                fileName));
        writeIntoFile(rootName + correctFileName, "\\a\\bcda-\\a\\bcda-\\a\\bcdb--".getBytes());
        Assert.assertTrue(compareAndDeleteFile());

        stmt.execute(ENABLE_SELECT_INTO_FILE + String.format(
            "select * from %s into outfile '%s' fields terminated by 'a-' enclosed by '\"' lines terminated by 'b--'",
            tableName, fileName));
        writeIntoFile(rootName + correctFileName, "\"a\\bcd\"a-\"a\\bcd\"a-\"a\\bcd\"b--".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestEscapeType() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format(
            "create table %s (a int, b double, c varchar(10), d blob, e json, f set('1', '2', '3'), g enum('1', '2', '3'))",
            tableName));
        stmt.execute(
            String.format("insert into %s values (1, 1, \"1\", \"1\", '{\"key\": 1}', \"1\", \"1\")", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s into outfile '%s' fields terminated by ',' escaped by '1'", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "1,1.0,11,11,{\"key\": 11},11,11\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestCharset() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("create table %s (a char(10), b char(10), c char(10)) CHARSET=gbk", tableName));
        stmt.execute(String.format("insert into %s values ('越寒-1', 'chaofan', '123')", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s into outfile '%s' character set gbk fields terminated by ','", tableName,
                fileName));
        writeIntoFile(rootName + correctFileName, "越寒-1,chaofan,123\n".getBytes("gbk"));
        Assert.assertTrue(compareAndDeleteFile());
        stmt.execute(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s into outfile '%s' character set gbk fields terminated by ','", tableName,
                fileName));
        writeIntoFile(rootName + correctFileName, "越寒-1,chaofan,123\n".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(!compareAndDeleteFile());
    }

    @Test
    public void TestShardingTable() throws SQLException, IOException {
        stmt.execute(String.format("drop table if exists %s;", tableName));
        stmt.execute(String.format("CREATE TABLE %s (`id` int(11) NOT NULL, "
                + "`data` int(11) DEFAULT NULL, PRIMARY KEY (`id`) "
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`id`) tbpartition by hash(`id`)",
            tableName));

        stmt.execute(String.format("insert into %s values (1, 1), (2, 2), (3, 3), (4, 4)", tableName));
        stmt.execute(String.format("insert into %s values (5, 5), (6, 6), (7, 7), (8, 8)", tableName));
        stmt.execute(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s order by id into outfile '%s' fields terminated by ','", tableName, fileName));
        writeIntoFile(rootName + correctFileName, "1,1\n2,2\n3,3\n4,4\n5,5\n6,6\n7,7\n8,8\n".getBytes());
        Assert.assertTrue(compareAndDeleteFile());
    }

    @Test
    public void TestSelectIntoOutfileWithLoadData() throws Exception {
        StringBuilder sql = new StringBuilder(String.format("drop table if exists %s;", tableName));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), null);

        sql = new StringBuilder(String.format("CREATE TABLE `%s` ("
            + "`pk` bigint(11) NOT NULL AUTO_INCREMENT,"
            + "`varchar_test` varchar(255) DEFAULT NULL,"
            + "`integer_test` int(11) DEFAULT NULL,"
            + "`char_test` char(255) DEFAULT NULL,"
            + "`tinyint_test` tinyint(4) DEFAULT NULL,"
            + "`tinyint_1bit_test` tinyint(1) DEFAULT NULL,"
            + "`smallint_test` smallint(6) DEFAULT NULL,"
            + "`mediumint_test` mediumint(9) DEFAULT NULL,"
            + "`bigint_test` bigint(20) DEFAULT NULL,"
            + "`double_test` double DEFAULT NULL,"
            + "`decimal_test` decimal(10, 0) DEFAULT NULL,"
            + "`date_test` date DEFAULT NULL,"
            + "`datetime_test` datetime DEFAULT NULL,"
            + "`timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,"
            + "`year_test` year(4) DEFAULT NULL,"
            + "PRIMARY KEY (`pk`)"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 97 DEFAULT CHARSET = utf8", tableName));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql.toString(), null);

        sql = new StringBuilder(String.format(
            "insert into %s (`pk`, `varchar_test`, `integer_test`, `char_test`,"
                + "`tinyint_test`, `tinyint_1bit_test`, `smallint_test`, `mediumint_test`,"
                + "`bigint_test`, `double_test`, `decimal_test`, `date_test`,"
                + "`datetime_test`, `timestamp_test`, `year_test`) values ", tableName));
        List<Object> param = new ArrayList<>();
        final int rowCnt = 12;
        for (int i = 0; i < rowCnt; i++) {
            sql.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            String dateStr = "2021-" + String.format("%02d-%02d", i + 1, i + 1);
            String timeStr = String.format("%02d:%02d:%02d", i, i, i);
            param.add(i + 1);
            param.add("test" + i);
            param.add(i);
            param.add("test" + i);
            param.add(i);
            param.add(i % 2);
            param.add(i);
            param.add(i);
            param.add(i);
            param.add(0.0 + i);
            param.add(0.0 + i);
            param.add(dateStr);
            param.add(dateStr + " " + timeStr);
            param.add(dateStr + " " + timeStr);
            param.add("2021");
            if (i < rowCnt - 1) {
                sql.append(",");
            }
        }
        executeOnMysqlOrTddl(tddlConnection, sql.toString(), param);

        sql = new StringBuilder(ENABLE_SELECT_INTO_FILE + String
            .format("select * from %s into outfile '%s' fields terminated by ','", tableName, fileName));
        stmt.execute(sql.toString());
//        dataOperator.executeOnMysqlOrTddl(tddlConnection, sql.toString(), null);

        sql = new StringBuilder(String
            .format("load data local infile '%s' into table %s fields "
                + "terminated by ',' lines terminated by '\\n'", rootName + fileName, tableName));

        executeOnMysqlOrTddl(mysqlConnection, sql.toString(), null);

        sql = new StringBuilder(String.format("select * from %s", tableName));
        selectContentSameAssert(sql.toString(), null, mysqlConnection, tddlConnection);

        cleanFiles();
    }
}
