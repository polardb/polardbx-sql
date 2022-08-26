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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.gms.engine.CachePolicy;
import com.alibaba.polardbx.gms.engine.DeletePolicy;
import com.alibaba.polardbx.gms.engine.FileStorageInfoAccessor;
import com.alibaba.polardbx.gms.engine.FileStorageInfoRecord;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Baseline of FileStore
 */
public class FileStorageTest extends BaseTestCase {

    private static String testDataBase = "fileStorageTestDatabase";

    private static String testDataBase2 = "fileStorageTestDatabase2";

    private static String testDataBase3 = "fileStorageTestDatabase3";

    private static Engine engine = PropertiesUtil.engine();

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

        try (Connection conn = getConnectionNoDefaultDb()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase2));
            statement.execute(String.format("create database %s mode = 'partitioning'", testDataBase2));
            statement.execute(String.format("use %s", testDataBase2));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        try (Connection conn = getConnectionNoDefaultDb()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase3));
            statement.execute(String.format("create database %s mode = 'partitioning'", testDataBase3));
            statement.execute(String.format("use %s", testDataBase3));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        initLocalDisk();
    }

    /**
     * ERROR 1 (archive_mode must be using with file-store engine): CREATE TABLE target_table LIKE source_table ARCHIVE_MODE = 'xxx';
     * ERROR 2 (source table of create like must not be file-store engine): CREATE TABLE target_table LIKE oss_source_table;
     */
    @Ignore
    public void testCreateTable() {
        String sourceDB = PropertiesUtil.polardbXDBName1(true);
        String sourceTable = "select_base_one_multi_db_multi_tb";

        // ERROR: CREATE TABLE target_table LIKE source_table ARCHIVE_MODE = 'empty';
        Throwable expected = null;
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String ddl = String.format(
                "create table testBaseTable_%s like %s.%s archive_mode ='empty'",
                sourceTable,
                sourceDB,
                sourceTable);
            statement.execute(ddl);
        } catch (Throwable t) {
            expected = t;
        }

        Assert.assertTrue(expected.getMessage(), expected != null && expected.getMessage()
            .endsWith("cannot create table using ARCHIVE_MODE if the engine of target table is INNODB."));

        // ERROR: CREATE TABLE target_table LIKE oss_source_table;
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();

            String ddl = String.format(
                "create table source_oss_%s like %s.%s ENGINE = '%s' ARCHIVE_MODE = 'loading'",
                sourceTable,
                sourceDB,
                sourceTable,
                engine.name());
            statement.execute(ddl);

            ddl = String.format("/*+TDDL:ALLOW_CREATE_TABLE_LIKE_FILE_STORE=true*/ "
                    + "create table testBaseTable_%s like source_oss_%s ENGINE = '%s' ARCHIVE_MODE = 'loading'",
                sourceTable,
                sourceTable,
                engine.name());
            statement.execute(ddl);

            ddl = String.format(
                "create table testBaseTable_%s_1 like source_oss_%s ENGINE = '%s' ARCHIVE_MODE = 'loading'",
                sourceTable,
                sourceTable,
                engine.name());
            statement.execute(ddl);
        } catch (Throwable t) {
            expected = t;
        }

        Assert.assertTrue(expected.getMessage(), expected != null && expected.getMessage()
            .endsWith("cannot create table like an file-store table, the engine of source table must be INNODB."));

    }

    /**
     * CREATE TABLE target_table LIKE source_table ENGINE = 'OSS' ARCHIVE_MODE = 'LOADING'
     */
    @Test
    @Ignore
    public void testBaseTableLoading() {
        String sourceDbName = PropertiesUtil.polardbXDBName1(true);
        List<String> allTables = Arrays.stream(ExecuteTableSelect.selectBaseOneTable()).map(array -> array[0])
            .collect(Collectors.toList());

        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            for (String tableName : allTables) {
                String ddl = String.format("/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ "
                        + "create table testBaseTable_%s like %s.%s engine = '%s' archive_mode ='loading'", tableName,
                    sourceDbName, tableName, engine.name());
                statement.execute(ddl);
                ResultSet rs1 =
                    statement.executeQuery(String.format("select count(*) from %s.%s", sourceDbName, tableName));
                rs1.next();
                int innodbCount = rs1.getInt(1);
                ResultSet rs2 =
                    statement.executeQuery(String.format("select count(*) from testBaseTable_%s", tableName));
                rs2.next();
                int ossCount = rs2.getInt(1);
                Assert.assertTrue("table innodbCount == ossCount", innodbCount == ossCount);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    /**
     * create table xxx like tpch.xxx engine = 'oss' archive_mode ='loading'
     */
    @Ignore
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
            String[] tpchTableNames =
                {"nation", "region", "supplier", "part", "customer", "partsupp", "orders", "lineitem"};
            for (String tableName : tpchTableNames) {
                statement.execute(String.format(
                    "create table %s like tpch.%s engine = '%s' archive_mode ='loading'",
                    tableName, tableName, engine.name()));
                ResultSet rs1 = statement.executeQuery(String.format("select count(*) from tpch.%s", tableName));
                rs1.next();
                int innodbCount = rs1.getInt(1);
                ResultSet rs2 =
                    statement.executeQuery(String.format("select count(*) from %s", tableName));
                rs2.next();
                int ossCount = rs2.getInt(1);
                Assert.assertTrue("table innodbCount == ossCount", innodbCount == ossCount);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        // TODO: provide TPC-H 1GB baseline test.
    }

    @Test
    public void testCreateTableWithLoading() {
        try {
            createFileStorageTableWith10000Rows("testCreateTableWithLoadingTable");
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * FLASHBACK TABLE {recyclebin_name} TO BEFORE DROP RENAME TO {table_name};
     */
    @Test
    public void testOssRecycleBin() {
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String testTableName = "testOssRecycleBinTable";
            createFileStorageTableWith10000Rows(testTableName);
            statement.execute(String.format("drop table %s", testTableName));
            ResultSet resultSet = statement.executeQuery("show recyclebin;");
            while (resultSet.next()) {
                if (resultSet.getString("ORIGINAL_NAME").equalsIgnoreCase(testTableName)) {
                    String ossBinName = resultSet.getString("NAME");
                    statement.execute(
                        String.format("FLASHBACK TABLE %s TO BEFORE DROP RENAME TO %s", ossBinName, testTableName));
                    break;
                }
            }
            statement.executeQuery(String.format("select count(*) from %s", testTableName));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * PURGE TABLE table_name
     */
    @Test
    public void testOssPurgeTable() {
        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();
            String testTableName = "testOssPurgeTable";
            createFileStorageTableWith10000Rows(testTableName);
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

    /**
     * CREATE TABLE target_table LIKE source_table ENGINE = 'OSS' ARCHIVE_MODE = 'TTL';
     * <p>
     * ALTER TABLE table_name EXPIRE LOCAL PARTITION partition_name;
     */
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

            String[] dates =
                {"2020-12-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            statement.executeUpdate(String
                .format("create table %s like %s engine = '%s' archive_mode ='ttl'", ossTableName, innodbTableName,
                    engine.name()));

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
    public void testOssExpireLocalPartitionCrossDB() {
        String ossTableName = "oss_t_local_partition";
        String innodbTableName = "t_local_partition";
        try (Connection conn1 = getPolardbxConnection(testDataBase);
            Connection conn2 = getPolardbxConnection(testDataBase2)) {
            Statement statement1 = conn1.createStatement();
            Statement statement2 = conn2.createStatement();

            statement1.execute(String.format("CREATE TABLE %s (\n" +
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

            String[] dates =
                {"2020-12-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 1999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement1.executeUpdate(insert.toString());
            }

            statement2.executeUpdate(String
                .format("create table %s like %s.%s engine = '%s' archive_mode ='ttl'", ossTableName, testDataBase,
                    innodbTableName, engine.name()));

            long count1 = count(conn2, ossTableName);
            statement1.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210101");
            long count2 = count(conn2, ossTableName);
            statement1.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210201");
            long count3 = count(conn2, ossTableName);
            statement1.executeUpdate("ALTER TABLE t_local_partition EXPIRE LOCAL PARTITION p20210301");
            long count4 = count(conn2, ossTableName);

            Assert.assertTrue(String.valueOf(count1), count1 == 0);
            Assert.assertTrue(String.valueOf(count2), count2 == 2000);
            Assert.assertTrue(String.valueOf(count3), count3 == 4000);
            Assert.assertTrue(String.valueOf(count4), count4 == 6000);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testAGGFullTypeTable() {
        try (Connection conn = getConnection()) {
            String innodbTableName = "agg_full_type";
            String ossTableName = "oss_agg_full_type";
            FullTypeTestUtil.createInnodbTableWithData(conn, innodbTableName);
            createOssTableLoadingFromInnodbTable(conn, ossTableName, innodbTableName);

            Statement statement = conn.createStatement();

            ResultSet rs = statement.executeQuery(String.format("select COLUMN_NAME from information_schema.COLUMNS "
                + "where TABLE_SCHEMA = '%s' and table_name='%s'", testDataBase, innodbTableName));
            List<String> columns = new ArrayList<>();
            while (rs.next()) {
                String column = rs.getString(1);
                if (column.contains("bit")) {
                    continue;
                }
                if (column.contains("char")) {
                    continue;
                }
                if (column.contains("blob")) {
                    continue;
                }
                if (column.contains("text")) {
                    continue;
                }
                if (column.contains("binary")) {
                    continue;
                }
                if (column.contains("poly")) {
                    continue;
                }
                if (column.contains("set")) {
                    continue;
                }
                if (column.contains("geo")) {
                    continue;
                }
                if (column.contains("point")) {
                    continue;
                }
                if (column.contains("line")) {
                    continue;
                }
                if (column.contains("json")) {
                    continue;
                }
                columns.add(column);
            }
            rs.close();

            ResultSet resultSet = statement.executeQuery("select min(id) from " + innodbTableName);
            resultSet.next();
            long startId = resultSet.getLong(1) + 2;

            String[] aggs = new String[] {"count", "min", "max", "sum"};
            String asynHint =
                "/*+TDDL:cmd_extra(ENABLE_OSS_BUFFER_POOL=false, ENABLE_OSS_DELAY_MATERIALIZATION=false)*/";
            String syncHint = "/*+TDDL:cmd_extra(ENABLE_OSS_BUFFER_POOL=true, ENABLE_OSS_DELAY_MATERIALIZATION=true)*/";
            for (String agg : aggs) {
                for (String column : columns) {
                    // ignore unable countable column
                    if (agg.equalsIgnoreCase("sum")) {
                        if (column.contains("time")) {
                            continue;
                        }
                        if (column.contains("enum")) {
                            continue;
                        }
                        if (column.contains("date")) {
                            continue;
                        }
                        if (column.contains("year")) {
                            continue;
                        }
                    }
                    String sql = "select %s(%s) from %s;";
                    // async ossTableScan
                    String ossSql = asynHint + String.format(sql, agg, column, ossTableName);
                    String innodbSql = String.format(sql, agg, column, innodbTableName);
                    ResultSet ossRs = statement.executeQuery(ossSql);
                    List<List<Object>> ossRsList = JdbcUtil.getAllResult(ossRs, false);
                    ResultSet innodbRs = statement.executeQuery(innodbSql);
                    List<List<Object>> innodbRsList = JdbcUtil.getAllResult(innodbRs, false);
                    assertWithMessage("oss 返回结果与innodb 返回结果不一致" + ossSql)
                        .that(ossRsList)
                        .containsExactlyElementsIn(innodbRsList);

                    // sync ossTableScan
                    String ossSyncSql = syncHint + String.format(sql, agg, column, ossTableName);
                    ResultSet ossSyncRs = statement.executeQuery(ossSyncSql);
                    List<List<Object>> ossSyncRsList = JdbcUtil.getAllResult(ossSyncRs, false);
                    assertWithMessage("oss 返回结果与innodb 返回结果不一致" + ossSyncSql)
                        .that(ossSyncRsList)
                        .containsExactlyElementsIn(innodbRsList);

                    // test filter
                    sql = "select %s(%s) from %s where id > " + startId + ";";
                    // async ossTableScan
                    ossSql = asynHint + String.format(sql, agg, column, ossTableName);
                    innodbSql = String.format(sql, agg, column, innodbTableName);
                    ossRs = statement.executeQuery(ossSql);
                    ossRsList = JdbcUtil.getAllResult(ossRs, false);
                    innodbRs = statement.executeQuery(innodbSql);
                    innodbRsList = JdbcUtil.getAllResult(innodbRs, false);
                    assertWithMessage("oss 返回结果与innodb 返回结果不一致" + ossSql)
                        .that(ossRsList)
                        .containsExactlyElementsIn(innodbRsList);

                    // sync ossTableScan
                    ossSyncSql = syncHint + String.format(sql, agg, column, ossTableName);
                    ossSyncRs = statement.executeQuery(ossSyncSql);
                    ossSyncRsList = JdbcUtil.getAllResult(ossSyncRs, false);
                    assertWithMessage("oss 返回结果与innodb 返回结果不一致" + ossSyncSql)
                        .that(ossSyncRsList)
                        .containsExactlyElementsIn(innodbRsList);
                }
            }

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

    /**
     * ALTER TABLE table_name DROP FILE file_name [,file_name...]
     */
    @Test
    public void testAlterTableDropFile() {
        try (Connection connection = getConnection()) {
            String tableName = "testDropFileTable";
            createFileStorageTableWith10000Rows(tableName);
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

    /**
     * ALTER FILESTORAGE OSS purge before TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStoragePurgeBeforeTimeStamp() {
        try (Connection connection = getConnection()) {
            String ossTableName = "testAlterFileStoragePurgeBeforeTimeStamp";
            createFileStorageTableWith10000Rows(ossTableName);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + ossTableName);
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");

                LocalDateTime beforeDropTime = LocalDateTime.now();
                Thread.sleep(2000);

                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", ossTableName, fileName));

                    Thread.sleep(2000);
                    LocalDateTime afterDropTime = LocalDateTime.now();

                    long countBeforeDropTime = countAsOfTimeStamp(connection2, ossTableName, beforeDropTime);
                    long countAfterDropTable = countAsOfTimeStamp(connection2, ossTableName, afterDropTime);

                    Assert.assertTrue(countBeforeDropTime > countAfterDropTable);

                    statement2.executeUpdate(String.format("ALTER FILESTORAGE '%s' PURGE BEFORE TIMESTAMP '%s'",
                        engine.name(),
                        String.format("%04d-%02d-%02d %02d:%02d:%02d",
                            afterDropTime.getYear(),
                            afterDropTime.getMonthValue(),
                            afterDropTime.getDayOfMonth(),
                            afterDropTime.getHour(),
                            afterDropTime.getMinute(),
                            afterDropTime.getSecond())));

                    countBeforeDropTime = countAsOfTimeStamp(connection2, ossTableName, beforeDropTime);
                    countAfterDropTable = countAsOfTimeStamp(connection2, ossTableName, afterDropTime);

                    Assert.assertTrue(countBeforeDropTime == countAfterDropTable);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterFileStorageAsOfTimeStampTable";
            createFileStorageTableWith10000Rows(tableName);
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

            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            lastCount = Integer.MIN_VALUE;
            for (int i = dateTimeList.size() - 1; i >= 0; i--) {
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    LocalDateTime localDateTime = dateTimeList.get(i);
                    statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                        engine.name(),
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

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp2() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterFileStorageAsOfTimeStamp2";
            createFileStorageTableWith10000Rows(tableName);
            LocalDateTime localDateTime = LocalDateTime.now();
            Thread.sleep(1000);

            Statement statement = connection.createStatement();
            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
                long count1 = count(connection, tableName);
                Assert.assertTrue(count1 == 10000);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER TABLE table_name PURGE BEFORE TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStoragePurgeBeforeTimeStamp2() {
        try (Connection connection = getConnection()) {

            String tableName = "testAlterFileStoragePurgeBeforeTimeStamp2";
            createFileStorageTableWith10000Rows(tableName);

            Statement statement = connection.createStatement();
            statement.executeUpdate("drop table " + tableName);

            Thread.sleep(1000);
            LocalDateTime localDateTime = LocalDateTime.now();

            ResultSet rs = statement.executeQuery("show recyclebin");
            rs.next();
            Assert.assertTrue(rs.getString("ORIGINAL_NAME").equalsIgnoreCase(tableName));
            Assert.assertFalse(rs.next());

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format("alter fileStorage '%s' purge before timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            rs = statement.executeQuery("show recyclebin");
            Assert.assertFalse(rs.next());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp4() {
        try (Connection connection = getConnection()) {
            LocalDateTime localDateTime = LocalDateTime.now();
            Thread.sleep(1000);
            String tableName = "testAlterFileStorageAsOfTimeStamp4";
            createFileStorageTableWith10000Rows(tableName);

            Statement statement = connection.createStatement();
            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Assert.assertTrue(count(connection, tableName) == 0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER TABLE source_table EXCHANGE PARTITION partition_name WITH TABLE target_table;
     */
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

            String[] dates =
                {"2007-12-01", "2008-01-01", "2008-01-01", "2008-02-01", "2008-03-01", "2008-04-01", "2008-05-01"};
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

            statement.execute(String
                .format("ALTER TABLE %s EXCHANGE PARTITION p0 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count1 = count(conn, ossTestTableName);
            statement.execute(String
                .format("ALTER TABLE %s EXCHANGE PARTITION p1 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count2 = count(conn, ossTestTableName);
            statement.execute(String
                .format("ALTER TABLE %s EXCHANGE PARTITION p2 WITH TABLE %s", innodbTestTableName, ossTestTableName));
            long count3 = count(conn, ossTestTableName);
            Assert.assertTrue(count3 > count2 && count2 > count1 && count1 > 0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * INSERT INTO target_table SELECT * FROM source_table.
     */
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
    public void testQueryFlashBackAsOfTimestamp() {
        try (Connection conn = getConnection()) {
            String ossTableName = "oss_testQueryFlashBackAsOfTimestampTable";
            String innodbTableName = "testQueryFlashBackAsOfTimestampTable";
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

            String[] dates =
                {"2020-12-01", "2021-01-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"};
            for (String date : dates) {
                StringBuilder insert = new StringBuilder();
                insert.append("insert into ").append(innodbTableName).append(" ('gmt_modified') values ");
                for (int i = 0; i < 999; i++) {
                    insert.append("('").append(date).append("')").append(",");
                }
                insert.append("('").append(date).append("')");
                statement.executeUpdate(insert.toString());
            }

            statement.executeUpdate(String
                .format("create table %s like %s engine = '%s' archive_mode ='ttl'", ossTableName, innodbTableName,
                    engine.name()));

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

    @Test
    public void TestUnArchiveOssTable() {
        String BUILD = "CREATE TABLE %s ( id bigint NOT NULL AUTO_INCREMENT,"
            + " gmt_modified DATETIME NOT NULL,PRIMARY KEY (id, gmt_modified))"
            + " PARTITION BY HASH(id) PARTITIONS %s"
            + " LOCAL PARTITION BY RANGE (gmt_modified)"
            + " STARTWITH '2021-01-01'"
            + " INTERVAL 1 MONTH"
            + " EXPIRE AFTER 1"
            + " PRE ALLOCATE 3"
            + " PIVOTDATE NOW();";
        String SHOWARCHIVE = "SHOW ARCHIVE";
        String BUILDOSS = "CREATE TABLE %s_oss LIKE %s engine = '%s' archive_mode = 'ttl'";
        String SHOWTABLEGROUP = "show tablegroup where TABLE_GROUP_NAME not like \"oss%\"";
        int[] partition = new int[] {8, 10};

        String sql;
        List<String> tables = new ArrayList<>();
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            for (int j : partition) {
                tables.add("table_" + i + "_" + j);
                partitions.add(j);
            }
        }

        try (Connection conn = getPolardbxConnection(testDataBase3)) {
            Statement stmt = conn.createStatement();
            for (int i = 0; i < tables.size(); i++) {
                sql = String.format(BUILD, tables.get(i), partitions.get(i));
                stmt.execute(sql);
                sql = String.format(BUILDOSS, tables.get(i), tables.get(i), engine.name());
                stmt.execute(sql);
            }

            //show archive
            int archiveSize = JdbcUtil.getAllResult(stmt.executeQuery(SHOWARCHIVE), false).size();
            assertWithMessage("show archive 结果不合符预期").that(archiveSize)
                .isEqualTo(tables.size());

            //unarchive table
            stmt.execute("unarchive table " + tables.get(0));
            int unarchiveTable = JdbcUtil.getAllResult(stmt.executeQuery(SHOWARCHIVE), false).size();
            assertWithMessage(String.format("unarchive table %s 结果不合符预期", tables.get(0))).that(unarchiveTable)
                .isEqualTo(tables.size() - 1);
            //unarchive tablegroup
            sql = SHOWTABLEGROUP;
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            String name = rs.getString("TABLE_GROUP_NAME");
            stmt.execute("unarchive tablegroup " + name);
            int unarchiveTableGroup = JdbcUtil.getAllResult(stmt.executeQuery(SHOWARCHIVE), false).size();
            assertWithMessage(String.format("unarchive tablegroup %s 结果不合符预期", name)).that(unarchiveTableGroup)
                .isLessThan(unarchiveTable);

            //unarchive database
            stmt.execute("unarchive database " + testDataBase3);
            int unarchiveDatabase = JdbcUtil.getAllResult(stmt.executeQuery(SHOWARCHIVE), false).size();
            assertWithMessage(String.format("unarchive database %s 结果不合符预期", tables.get(0))).that(unarchiveDatabase)
                .isEqualTo(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testVarcharPruning() {
        String innodbTableName = "testVarcharPruning";
        String ossTableName = "oss_testVarcharPruning";
        List<String> varcharValues = new ArrayList<>();
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    varchar_column varchar(100) DEFAULT NULL,\n" +
                "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    KEY (varchar_column),\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innodbTableName));

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innodbTableName).append("(`id`,`varchar_column`) values ");
            for (int i = 0; i < 9999; i++) {
                String val = UUID.randomUUID().toString();
                varcharValues.add(val);
                insert.append("(0, '").append(val).append("')").append(",");
            }
            insert.append("(0, null)");
            statement.executeUpdate(insert.toString());

            // create oss table
            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
                ossTableName, innodbTableName, engine.name()));
            statement.execute("drop table " + innodbTableName);

            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                statement.executeQuery(String.format("trace select * from %s where varchar_column = '%s'", ossTableName,
                    varcharValues.get(random.nextInt(10000))));
                ResultSet resultSet = statement.executeQuery("show trace");
                int traceCount = 0;
                while (resultSet.next()) {
                    traceCount++;
                }
                // table has 8 partition, with bloomfilter we expect at least pruning half (4 partition).
                Assert.assertTrue(traceCount <= 4 && traceCount > 0);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNullVarcharPruning() {
        String innodbTableName = "testNullVarcharPruning";
        String ossTableName = "oss_testNullVarcharPruning";
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    varchar_column varchar(100) DEFAULT NULL,\n" +
                "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    KEY (varchar_column),\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innodbTableName));

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innodbTableName).append("(`id`) values ");
            for (int i = 0; i < 9999; i++) {
                insert.append("(0)").append(",");
            }
            insert.append("(0)");
            statement.executeUpdate(insert.toString());

            // create oss table
            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
                ossTableName, innodbTableName, engine.name()));
            statement.execute("drop table " + innodbTableName);

            statement.executeQuery(String.format("trace select * from %s where varchar_column = '1'", ossTableName));
            ResultSet resultSet = statement.executeQuery("show trace");
            int traceCount = 0;
            while (resultSet.next()) {
                traceCount++;
            }

            Assert.assertTrue(traceCount == 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDecimalPruning() {
        String innodbTableName = "testDecimalPruning";
        String ossTableName = "oss_testDecimalPruning";
        List<BigDecimal> decimalValues = new ArrayList<>();
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    decimal_column decimal(20,10) DEFAULT NULL,\n" +
                "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    KEY (decimal_column),\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innodbTableName));

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innodbTableName).append("(`id`,`decimal_column`) values ");
            Random rand = new Random();
            for (int i = 0; i < 9999; i++) {
                BigDecimal bigDecimal = new BigDecimal(rand.nextDouble() * 1000000);
                bigDecimal = bigDecimal.setScale(10, BigDecimal.ROUND_UP);
                decimalValues.add(bigDecimal);
                insert.append("(0, '").append(bigDecimal).append("')").append(",");
            }
            insert.append("(0, null)");
            statement.executeUpdate(insert.toString());

            // create oss table
            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
                ossTableName, innodbTableName, engine.name()));
            statement.execute("drop table " + innodbTableName);

            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                statement.executeQuery(String.format("trace select * from %s where decimal_column = %s", ossTableName,
                    decimalValues.get(random.nextInt(10000))));
                ResultSet resultSet = statement.executeQuery("show trace");
                int traceCount = 0;
                while (resultSet.next()) {
                    traceCount++;
                }
                // table has 8 partition, with bloomfilter we expect at least pruning half (4 partition).
                Assert.assertTrue(traceCount <= 4 && traceCount > 0);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNullDecimalPruning() {
        String innodbTableName = "testNullDecimalPruning";
        String ossTableName = "oss_testNullDecimalPruning";
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    decimal_column decimal(20,10) DEFAULT NULL,\n" +
                "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    KEY (decimal_column),\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innodbTableName));

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innodbTableName).append("(`id`) values ");
            Random rand = new Random();
            for (int i = 0; i < 9999; i++) {
                insert.append("(0)").append(",");
            }
            insert.append("(0)");
            statement.executeUpdate(insert.toString());

            // create oss table
            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
                ossTableName, innodbTableName, engine.name()));
            statement.execute("drop table " + innodbTableName);

            statement.executeQuery(String.format("trace select * from %s where decimal_column = 1.1", ossTableName));
            ResultSet resultSet = statement.executeQuery("show trace");
            int traceCount = 0;
            while (resultSet.next()) {
                traceCount++;
            }

            Assert.assertTrue(traceCount == 0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initLocalDisk() {
        try (Connection connection = getMetaConnection()) {
            initLocalDisk(connection);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void initLocalDisk(Connection metadbConn) {
        try {
            FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
            fileStorageInfoAccessor.setConnection(metadbConn);

            FileStorageInfoRecord record1 = new FileStorageInfoRecord();
            record1.instId = "";
            record1.engine = "local_disk";
            record1.fileUri = "file:///tmp/local-orc-dir/";
            record1.fileSystemConf = "";
            record1.priority = 1;
            record1.regionId = "";
            record1.availableZoneId = "";
            record1.cachePolicy = CachePolicy.META_AND_DATA_CACHE.getValue();
            record1.deletePolicy = DeletePolicy.MASTER_ONLY.getValue();
            record1.status = 1;

            List<FileStorageInfoRecord> fileStorageInfoRecordList = new ArrayList<>();
            fileStorageInfoRecordList.add(record1);
            fileStorageInfoAccessor.insertIgnore(fileStorageInfoRecordList);
            Statement statement = metadbConn.createStatement();
            statement.execute(
                "update config_listener set op_version = op_version + 1 where data_id = 'polardbx.file.storage.info'");
            Thread.sleep(5000);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDropFileStorageLocalDisk() {
        initLocalDisk();
        try (Connection connection = getConnection()) {
            String localDiskTableName = "testDropFileStorageLocalDisk";
            createLocalDiskTableWith10000Rows(localDiskTableName);
            Statement statement = connection.createStatement();
            statement.execute("drop table " + localDiskTableName);
            createLocalDiskTableWith10000Rows(localDiskTableName);
            statement.execute("Drop FileStorage 'local_disk'");
            Thread.sleep(5000);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            initLocalDisk();
        }
    }

    public void createFileStorageTableWith10000Rows(String tableName) throws SQLException {
        createFileStorageTableWith10000Rows(tableName, engine);
    }

    public void createLocalDiskTableWith10000Rows(String tableName) throws SQLException {
        createFileStorageTableWith10000Rows(tableName, Engine.LOCAL_DISK);
    }

    public void createFileStorageTableWith10000Rows(String tableName, Engine engine) throws SQLException {
        String innodbTableName = tableName + "_innodb";
        createInnodbTableWith10000Rows(innodbTableName);
        try (Connection connection = getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
                tableName, innodbTableName, engine.name()));
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
        ResultSet resultSet = statement.executeQuery(String
            .format("select count(*) from %s as of TIMESTAMP '%s'", tableName,
                String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond())));
        resultSet.next();
        return resultSet.getLong(1);
    }

    public boolean createOssTableLoadingFromInnodbTable(Connection conn, String ossTableName, String innodbTableName)
        throws SQLException {
        Statement statement = conn.createStatement();
        return statement.execute(String.format(
            "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine ='%s' archive_mode = 'loading'",
            ossTableName, innodbTableName, engine.name()));
    }
}
