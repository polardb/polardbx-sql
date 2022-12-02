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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

public class OSSTest extends BaseTestCase {

    private static String testDataBase = "ossTestDatabase";

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    private Connection getConnectionNoDefaultDb() {
        return getPolardbxConnection();
    }

    @Before
    public void initTestDatabase() {
        try (Connection conn = getConnectionNoDefaultDb()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
            statement.execute(String.format("create database %s mode = 'partitioning'", testDataBase));
            statement.execute(String.format("use %s", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTPCHLoadingIfExists() {
        boolean existTPCHDatabase = false;
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("show databases");
            while (resultSet.next()) {
                if (resultSet.getString(1).equalsIgnoreCase("tpch")) {
                    existTPCHDatabase = true;
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        if (!existTPCHDatabase) {
            return;
        }

        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String[] tpchTableNames = {"nation", "region", "supplier", "part", "customer", "partsupp", "orders", "lineitem"};
            for (String tableName : tpchTableNames) {
                statement.execute(String.format("create table testTPCHLoadingIfExists_%s like tpch.%s engine = 'oss' archive_mode ='loading'", tableName, tableName));
                ResultSet rs1 = statement.executeQuery(String.format("select count(*) from tpch.%s", tableName));
                rs1.next();
                int innodbCount = rs1.getInt(1);
                ResultSet rs2 = statement.executeQuery(String.format("select count(*) from testTPCHLoadingIfExists_%s", tableName));
                rs2.next();
                int ossCount = rs2.getInt(1);
                Assert.assertTrue("table innodbCount == ossCount",innodbCount == ossCount);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testCreateTableWithLoading() {
        try {
            createOssTableWith10000Rows("testCreateTableWithLoadingTable");
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOssRecycleBin() {
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String testTableName = "testOssRecycleBinTable";
            createOssTableWith10000Rows(testTableName);
            statement.execute(String.format("drop table %s", testTableName));
            ResultSet resultSet = statement.executeQuery("show recyclebin;");
            while (resultSet.next()) {
                if (resultSet.getString("ORIGINAL_NAME").equalsIgnoreCase(testTableName)) {
                    String ossBinName = resultSet.getString("NAME");
                    statement.execute(String.format("FLASHBACK TABLE %s TO BEFORE DROP RENAME TO %s", ossBinName, testTableName));
                    break;
                }
            }
            statement.executeQuery(String.format("select count(*) from %s", testTableName));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOssPurgeTable() {
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String testTableName = "testOssPurgeTable";
            createOssTableWith10000Rows(testTableName);
            statement.execute(String.format("drop table %s", testTableName));
            ResultSet resultSet = statement.executeQuery("show recyclebin;");
            String ossBinName = null;
            while (resultSet.next()) {
                if (resultSet.getString("ORIGINAL_NAME").equalsIgnoreCase(testTableName)) {
                    ossBinName = resultSet.getString("NAME");
                    statement.execute(String.format("PURGE TABLE %s", ossBinName));
                    break;
                }
            }
            try {
                statement.executeQuery(String.format("select count(*) from %s", testTableName));
            } catch (Throwable t) {
                Assert.assertTrue(t.getMessage().contains("ERR_TABLE_NOT_EXIST"));
            }
            assert ossBinName != null;
            resultSet = statement.executeQuery("show recyclebin;");
            while (resultSet.next()) {
                if (resultSet.getString("NAME").equalsIgnoreCase(ossBinName)) {
                    Assert.fail("purge table fail");
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOssExpireLocalPartition() {
        try (Connection conn = getConnection()) {
            String ossTableName = "oss_t_local_partition";
            String innodbTableName = "t_local_partition";
            Statement statement = conn.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                    "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                    "    gmt_modified DATETIME NOT NULL,\n" +
                    "    PRIMARY KEY (id, gmt_modified)\n" +
                    ")\n" +
                    "PARTITION BY HASH(id) PARTITIONS 8\n" +
                    "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                    "STARTWITH '2021-01-01'\n" +
                    "INTERVAL 1 MONTH\n" +
                    "EXPIRE AFTER 1\n" +
                    "PRE ALLOCATE 3\n" +
                    "PIVOTDATE NOW();", innodbTableName));

            String[] dates = {"2020-12-01", "2021-01-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            statement.executeUpdate(String.format("create table %s like %s engine = 'oss' archive_mode ='ttl'", ossTableName, innodbTableName));

            long count1 = count(conn, ossTableName);
            statement.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210101");
            long count2 = count(conn, ossTableName);
            statement.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210201");
            long count3 = count(conn, ossTableName);
            statement.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210301");
            long count4 = count(conn, ossTableName);

            Assert.assertTrue(count1 == 0);
            Assert.assertTrue(count1 < count2);
            Assert.assertTrue(count2 < count3);
            Assert.assertTrue(count3 < count4);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testOssAlterTablePurgeBeforeTimestamp() {
        try (Connection conn = getConnection()) {
            String ossTableName = "oss_testOssAlterTablePurgeBeforeTimestampTable";
            String innodbTableName = "testOssAlterTablePurgeBeforeTimestampTable";
            Statement statement = conn.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                    "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                    "    gmt_modified DATETIME NOT NULL,\n" +
                    "    PRIMARY KEY (id, gmt_modified)\n" +
                    ")\n" +
                    "PARTITION BY HASH(id) PARTITIONS 8\n" +
                    "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                    "STARTWITH '2021-01-01'\n" +
                    "INTERVAL 1 MONTH\n" +
                    "EXPIRE AFTER 1\n" +
                    "PRE ALLOCATE 3\n" +
                    "PIVOTDATE NOW();", innodbTableName));

            String[] dates = {"2020-12-01", "2021-01-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            statement.executeUpdate(String.format("create table %s like %s engine = 'oss' archive_mode ='ttl'", ossTableName, innodbTableName));

            List<LocalDateTime> dateTimeList = new ArrayList<>();

            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210101", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());
            Thread.sleep(1000);
            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210201", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());
            Thread.sleep(1000);
            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210301", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());
            Thread.sleep(1000);

            long count1 = count(conn, ossTableName);
            LocalDateTime localDateTime = dateTimeList.get(0);
            statement.executeUpdate(String.format("ALTER TABLE %s PURGE BEFORE TIMESTAMP '%s'", ossTableName, String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond())));

            long count2 = count(conn, ossTableName);
            localDateTime = dateTimeList.get(1);
            statement.executeUpdate(String.format("ALTER TABLE %s PURGE BEFORE TIMESTAMP '%s'", ossTableName, String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond())));

            long count3 = count(conn, ossTableName);
            localDateTime = dateTimeList.get(2);
            statement.executeUpdate(String.format("ALTER TABLE %s PURGE BEFORE TIMESTAMP '%s'", ossTableName, String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond())));

            long count4 = count(conn, ossTableName);

            Assert.assertTrue(count1 > count2);
            Assert.assertTrue(count2 > count3);
            Assert.assertTrue(count3 > count4);
            Assert.assertTrue(count4 == 0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }



    @Test
    public void testFullTypeTable() {
        try (Connection conn = getConnection()) {
            String innodbTableName = "full_type";
            String ossTableName = "oss_full_type";
            FullTypeTestUtil.createInnodbTableWithData(conn, innodbTableName);
            createOssTableLoadingFromInnodbTable(conn, ossTableName, innodbTableName);
            Statement statement = conn.createStatement();
            ResultSet ossRs = statement.executeQuery("select * from " + ossTableName);
            List<List<Object>> ossRsList = JdbcUtil.getAllResult(ossRs, false);

            ResultSet innodbRs = statement.executeQuery("select * from " + innodbTableName);
            List<List<Object>> innodbRsList = JdbcUtil.getAllResult(innodbRs, false);
            assertWithMessage("非顺序情况下：oss 返回结果与innodb 返回结果不一致")
                .that(ossRsList)
                .containsExactlyElementsIn(innodbRsList);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testAlterTableDropFile() {
        try (Connection connection = getConnection()) {
            String tableName = "testDropFileTable";
            createOssTableWith10000Rows(tableName);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + tableName);
            int lastCount = Integer.MAX_VALUE;
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", tableName, fileName));
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount > count);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testAlterTableAsOfTimeStamp() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterTableAsOfTimeStampTable";
            createOssTableWith10000Rows(tableName);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + tableName);
            int lastCount = Integer.MAX_VALUE;
            List<LocalDateTime> dateTimeList = new ArrayList<>();
            dateTimeList.add(LocalDateTime.now());
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", tableName, fileName));
                    Thread.sleep(1000);
                    dateTimeList.add(LocalDateTime.now());
                    Thread.sleep(1000);
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount > count);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            lastCount = Integer.MIN_VALUE;
            for (int i = dateTimeList.size() - 1; i >= 0; i--) {
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    LocalDateTime localDateTime = dateTimeList.get(i);
                    statement2.execute(String.format("alter table %s as of timestamp '%s'", tableName,
                            String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                    localDateTime.getYear(),
                                    localDateTime.getMonthValue(),
                                    localDateTime.getDayOfMonth(),
                                    localDateTime.getHour(),
                                    localDateTime.getMinute(),
                                    localDateTime.getSecond())));
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount < count);
                    lastCount = count;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testAlterFileStorageAsOfTimeStamp() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterFileStorageAsOfTimeStampTable";
            createOssTableWith10000Rows(tableName);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + tableName);
            int lastCount = Integer.MAX_VALUE;
            List<LocalDateTime> dateTimeList = new ArrayList<>();
            dateTimeList.add(LocalDateTime.now());
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", tableName, fileName));
                    Thread.sleep(1000);
                    dateTimeList.add(LocalDateTime.now());
                    Thread.sleep(1000);
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount > count);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            lastCount = Integer.MIN_VALUE;
            for (int i = dateTimeList.size() - 1; i >= 0; i--) {
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    LocalDateTime localDateTime = dateTimeList.get(i);
                    statement2.execute(String.format("alter fileStorage 'oss' as of timestamp '%s'",
                            String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                    localDateTime.getYear(),
                                    localDateTime.getMonthValue(),
                                    localDateTime.getDayOfMonth(),
                                    localDateTime.getHour(),
                                    localDateTime.getMinute(),
                                    localDateTime.getSecond())));
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount < count);
                    lastCount = count;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    @Test
    public void testAlterTableExchange() {
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String innodbTestTableName = "testAlterTableExchangeTable";
            String ossTestTableName = "oss_" + innodbTestTableName;

            statement.execute(String.format("CREATE TABLE %s (\n" +
                    "    pk bigint NOT NULL AUTO_INCREMENT,\n" +
                    "    updated_time TIMESTAMP,\n" +
                    "    primary key (pk, updated_time)\n" +
                    ") ENGINE = 'INNODB'\n" +
                    "PARTITION BY RANGE ( UNIX_TIMESTAMP(updated_time) ) (\n" +
                    "    PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),\n" +
                    "    PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),\n" +
                    "    PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),\n" +
                    "    PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),\n" +
                    "    PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),\n" +
                    "    PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),\n" +
                    "    PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),\n" +
                    "    PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),\n" +
                    "    PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),\n" +
                    "    PARTITION p9 VALUES LESS THAN (MAXVALUE)\n" +
                    ")", innodbTestTableName));

            String[] dates = {"2007-12-01", "2008-01-01", "2008-01-01", "2008-02-01", "2008-03-01", "2008-04-01", "2008-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTestTableName).append(" ('updated_time') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            createOssTableLoadingFromInnodbTable(conn, ossTestTableName, innodbTestTableName);

            statement.execute(String.format("ALTER TABLE %s EXCHANGE PARTITION p0 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count1 = count(conn, ossTestTableName);
            statement.execute(String.format("ALTER TABLE %s EXCHANGE PARTITION p1 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count2 = count(conn, ossTestTableName);
            statement.execute(String.format("ALTER TABLE %s EXCHANGE PARTITION p2 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count3 = count(conn, ossTestTableName);
            Assert.assertTrue(count3 > count2 && count2 > count1 && count1 > 0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testInsertSelectFromOssToInnodb() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testInsertSelectFromOssToInnodb";
            String ossTestTableName = "oss_" + innodbTestTableName;
            FullTypeTestUtil.createInnodbTableWithData(conn, innodbTestTableName);
            createOssTableLoadingFromInnodbTable(conn, ossTestTableName, innodbTestTableName);
            Statement statement = conn.createStatement();
            statement.execute(String.format("truncate table %s", innodbTestTableName));
            Assert.assertTrue(count(conn, innodbTestTableName) == 0);
            statement.execute(String.format("insert into %s select * from %s", innodbTestTableName, ossTestTableName));
            Assert.assertTrue(count(conn, innodbTestTableName) == count(conn, ossTestTableName));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testQueryFlashBack() {
        try (Connection conn = getConnection()) {
            String ossTableName = "oss_testOssAlterTablePurgeBeforeTimestampTable";
            String innodbTableName = "testOssAlterTablePurgeBeforeTimestampTable";
            Statement statement = conn.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                    "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                    "    gmt_modified DATETIME NOT NULL,\n" +
                    "    PRIMARY KEY (id, gmt_modified)\n" +
                    ")\n" +
                    "PARTITION BY HASH(id) PARTITIONS 8\n" +
                    "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                    "STARTWITH '2021-01-01'\n" +
                    "INTERVAL 1 MONTH\n" +
                    "EXPIRE AFTER 1\n" +
                    "PRE ALLOCATE 3\n" +
                    "PIVOTDATE NOW();", innodbTableName));

            String[] dates = {"2020-12-01", "2021-01-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            statement.executeUpdate(String.format("create table %s like %s engine = 'oss' archive_mode ='ttl'", ossTableName, innodbTableName));

            List<LocalDateTime> dateTimeList = new ArrayList<>();

            dateTimeList.add(LocalDateTime.now());

            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210101", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());
            Thread.sleep(1000);
            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210201", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());
            Thread.sleep(1000);
            statement.executeUpdate(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20210301", innodbTableName));
            Thread.sleep(1000);
            dateTimeList.add(LocalDateTime.now());

            LocalDateTime localDateTime = dateTimeList.get(0);
            long count1 = countAsOfTimeStamp(conn, ossTableName, localDateTime);

            localDateTime = dateTimeList.get(1);
            long count2 = countAsOfTimeStamp(conn, ossTableName, localDateTime);

            localDateTime = dateTimeList.get(2);
            long count3 = countAsOfTimeStamp(conn, ossTableName, localDateTime);

            localDateTime = dateTimeList.get(3);
            long count4 = countAsOfTimeStamp(conn, ossTableName, localDateTime);

            Assert.assertTrue(count1 == 0);
            Assert.assertTrue(count1 < count2);
            Assert.assertTrue(count2 < count3);
            Assert.assertTrue(count3 < count4);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void createOssTableWith10000Rows(String tableName) throws SQLException {
        String innodbTableName = tableName + "_innodb";
        createInnodbTableWith10000Rows(innodbTableName);
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("create table %s like %s engine = 'oss' archive_mode = 'loading'", tableName, innodbTableName));
            statement.execute("drop table " + innodbTableName);
        }
    }

    public void createInnodbTableWith10000Rows(String tableName) throws SQLException {
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                    "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                    "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                    "    PRIMARY KEY (id, gmt_modified)\n" +
                    ")\n" +
                    "PARTITION BY HASH(id) PARTITIONS 8\n", tableName));

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(tableName).append("('id') values ");
            for (int i = 0; i < 9999; i++) {
                insert.append("(0)").append(",");
            }
            insert.append("(0)");
            statement.executeUpdate(insert.toString());
            statement.executeUpdate(insert.toString());
        }
    }

    public long count(Connection conn, String tableName) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
        resultSet.next();
        return resultSet.getLong(1);
    }

    public long countAsOfTimeStamp(Connection conn, String tableName, LocalDateTime localDateTime) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(String.format("select count(*) from %s as of TIMESTAMP '%s'", tableName, String.format("%04d-%02d-%02d %02d:%02d:%02d",
                localDateTime.getYear(),
                localDateTime.getMonthValue(),
                localDateTime.getDayOfMonth(),
                localDateTime.getHour(),
                localDateTime.getMinute(),
                localDateTime.getSecond())));
        resultSet.next();
        return resultSet.getLong(1);
    }

    public boolean createOssTableLoadingFromInnodbTable(Connection conn, String ossTableName, String innodbTableName) throws SQLException {
        Statement statement = conn.createStatement();
        return statement.execute(String.format("create table %s like %s engine ='oss' archive_mode = 'loading'", ossTableName, innodbTableName));
    }
}
