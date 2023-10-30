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

package com.alibaba.polardbx.qatest.ddl.auto.gsi.group3;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.junit.Assert.assertEquals;

public class UpdateTest extends DDLBaseNewDBTestCase {
    private boolean useAffectedRows;
    private Connection oldTddl;
    private Connection oldMySql;

    public UpdateTest(boolean useAffectedRows) {
        this.useAffectedRows = useAffectedRows;
    }

    @Parameterized.Parameters(name = "{index}:useAffectedRows={0}")
    public static List<Boolean[]> prepareData() {
        return ImmutableList.of(new Boolean[] {false}, new Boolean[] {true});
    }

    @Before
    public void before() {
        if (useAffectedRows && !useXproto()) {
            useAffectedRows = false;
        }
        if (useAffectedRows) {
            oldTddl = tddlConnection;
            tddlConnection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
            useDb(tddlConnection, tddlDatabase1);
            oldMySql = mysqlConnection;
            mysqlConnection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows();
            useDb(mysqlConnection, mysqlDatabase1);
        }
    }

    @After
    public void after() throws SQLException {
        if (useAffectedRows) {
            tddlConnection.close();
            tddlConnection = oldTddl;
            mysqlConnection.close();
            mysqlConnection = oldMySql;
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    final ImmutableList<String> blobValues = ImmutableList.<String>builder()
        .add("x'0A08080E10011894AB0E0A06080E10071832'")
        .add("x'0A07080010011886580A080800100218C997020A0708001003188B210A070800100418F9350A060800100518000A06080010"
            + "0618000A060800100718000A060800100818000A060800100918000A060800100A18000A060800100B18000A060800100C180"
            + "00A060800100D18000A060800100E18000A070800100F18DC0B0A060800101018000A070800101118E8070A06080010121800"
            + "0A060800101318000A060800101418000A060800101518000A060800101618000A060800101718000A060800101818000A060"
            + "800101918000A060800101B18000A060800101C18000A060800101D18000A060800101E18000A060800101F18000A06080010"
            + "2018000A060800102118000A060800102218000A060800102318000A060800102418000A060800102518000A0608001026180"
            + "00A060800102718000A060800102818000A060800102918000A060800102A18000A060800102B18000A060800102C18000A07"
            + "080110011886580A080801100218C997020A0708011003188B210A070801100418F9350A070802101118E8070A070802100F1"
            + "8DC0B0A06080510011800'")
        .add("0x0A08080E10011894AB0E0A06080E10071832")
        .add("'String'")
        .add("'中文'")
        .add("'2018-00-00 00:00:00'")
        .add("-123")
        .add("123+456")
        .add("123.321")
        .add("123.321+456.654")
        .add("_utf8'中文'")
        .add("_binary'中文'")
        .build();

    final ImmutableList<String> colDefs = ImmutableList.<String>builder()
        .add("blob")
        .add("varchar(4096)")
        .add("varchar(4096) character set binary")
        .add("varchar(4096) character set utf8mb4")
        .add("varbinary(4096)")
        .build();

    @Test
    public void updateBlobTest() {
        for (String colDef : colDefs) {
            final String tableName = "update_test_blob";
            final String gsiName = "g_update_test_blob";
            dropTableIfExists(tableName);
            dropTableIfExistsInMySql(tableName);

            final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "`zoneId` int(11) DEFAULT NULL,\n"
                + "`userId` int(11) DEFAULT NULL,\n"
                + "`blobfield` " + colDef + ",\n"
                + "PRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
            final String partitionDef = " partition by hash(`zoneId`) partition 3";

            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

            final String createGsi = "CREATE UNIQUE GLOBAL INDEX `" + gsiName
                + "` ON `"
                + tableName
                + "`(`id`) COVERING(`userId`, `zoneId`) partition by hash(`id`) partition 3";
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

            final String insert = "insert into " + tableName + "(id,zoneId,userId) values (1,1,1)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            final String index = " force index(" + gsiName + ")";
            final String mysqlUpdate = "update " + tableName + " set blobfield=%s where id=1";
            final String tddlUpdate =
                "/*+TDDL:CMD_EXTRA()*/ update " + tableName + index + " set blobfield=%s where id=1";
            for (String v : blobValues) {
                System.out.println(colDef + ": " + v);
                boolean checkResult = true;
                try {
                    executeOnMysqlAndTddl(tddlConnection, mysqlConnection,
                        String.format(tddlUpdate, v),
                        String.format(mysqlUpdate, v),
                        null, true);
                } catch (Throwable e) {
                    System.out.println(e);
                    checkResult = false;
                }
                if (checkResult) {
                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                }
            }
        }
    }

    @Test
    public void updatePushDownBlobTest() {
        for (String colDef : colDefs) {
            final String tableName = "update_push_down_test_blob";
            dropTableIfExists(tableName);
            dropTableIfExistsInMySql(tableName);

            final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
                + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "`zoneId` int(11) DEFAULT NULL,\n"
                + "`userId` int(11) DEFAULT NULL,\n"
                + "`blobfield` " + colDef + ",\n"
                + "PRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
            final String partitionDef = " partition by hash(`zoneId`) partition 3";

            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

            final String insert = "insert into " + tableName + "(id,zoneId,userId) values (1,1,1)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

            final String update = "/*+TDDL:CMD_EXTRA()*/ update " + tableName + " set blobfield=%s where id=1";
            for (String v : blobValues) {
                System.out.println(colDef + ": " + v);
                boolean checkResult = true;
                try {
                    System.out.println(colDef + ": " + v);
                    executeOnMysqlAndTddl(tddlConnection, mysqlConnection,
                        String.format(update, v),
                        String.format(update, v),
                        null, true);
                } catch (Throwable e) {
                    System.out.println(e);
                    checkResult = false;
                }
                if (!checkResult) {
                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                }
            }
        }
    }

    @Test
    public void updateBinaryTest() {
        final String tableName = "update_test_binary";
        final String gsiName = "g_update_test_binary";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "`zoneId` int(11) DEFAULT NULL,\n"
            + "`userId` int(11) DEFAULT NULL,\n"
            + "`blobfield` varbinary(512),\n"
            + "PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
        final String partitionDef = " partition by hash(`zoneId`) partitions 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String createGsi = "CREATE UNIQUE GLOBAL INDEX `" + gsiName
            + "` ON `"
            + tableName
            + "`(`id`) COVERING(`userId`, `zoneId`) partition by hash(`id`) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "insert into " + tableName + "(id,zoneId,userId) values (1,1,1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String index = " force index(" + gsiName + ")";
        final String mysqlUpdate = "update " + tableName + " set blobfield=%s where id=1";
        final String tddlUpdate = "/*+TDDL:CMD_EXTRA()*/ update " + tableName + index + " set blobfield=%s where id=1";
        for (String v : blobValues) {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                String.format(mysqlUpdate, v),
                String.format(tddlUpdate, v),
                null, true);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * GSI 有 On Update Current_Timestamp 列，但是不在 Update 语句的 Set 列表中
     */
    @Test
    public void updateGsiTimestampTest() throws SQLException {
        final String tableName = "update_gsi_timestamp_test";
        final String gsiName = "g_update_gsi_timestamp_test";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "`c` TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
            + "PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
        final String partitionDef = " PARTITION BY HASH(`id`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final String createGsi = "CREATE UNIQUE GLOBAL INDEX `" + gsiName
            + "` ON `"
            + tableName
            + "`(`id`) COVERING(`c`)  PARTITION BY HASH(`id`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "INSERT INTO " + tableName + "(`b`) values (1),(2),(3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        final String update = "UPDATE " + tableName + " SET b=1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    /**
     * Update 自增列为 NULL
     */
    @Test
    public void relocateAutoIncNullValueTest() throws SQLException {
        final String tableName = "relocate_auto_inc_null_test";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
        final String partitionDef = " PARTITION BY HASH(`id`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final String insert = "INSERT INTO " + tableName + " values (null, 1),(null, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String update = "UPDATE " + tableName + " SET id=null";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");

        update = "UPDATE " + tableName + " SET id=null*10";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");

        update = "UPDATE " + tableName + " SET b=10,id=null*10 WHERE b=2";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");
    }

    /**
     * Update 自增列为 NULL
     */
    @Test
    public void updateAutoIncNullValueTest() throws SQLException {
        final String tableName = "update_auto_inc_null_test";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "`b` int(11) DEFAULT NULL,\n"
            + "PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100004 DEFAULT CHARSET = utf8mb4";
        final String partitionDef = " PARTITION BY HASH(`b`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        final String insert = "INSERT INTO " + tableName + " values (null, 1),(null, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String update = "UPDATE " + tableName + " SET id=null";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");

        update = "UPDATE " + tableName + " SET id=null*10";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");

        update = "UPDATE " + tableName + " SET b=10,id=null*10 WHERE b=2";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "");
    }

    /**
     * 测试 UPDATE 在回填时的正确性
     */
    @Test
    public void updateGsiBackfillTest() throws Exception {
        updateGsiBackfillTestInternal("update_gsi_backfill_test_shard_tb", "partition by hash(`b`) PARTITIONS 3");
        updateGsiBackfillTestInternal("update_gsi_backfill_test_single_tb", "single");
        updateGsiBackfillTestInternal("update_gsi_backfill_test_broadcast_tb", "broadcast");
    }

    public void updateGsiBackfillTestInternal(String tableName, String gsiPartitionDef) throws Exception {
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `a` bigint(11) NOT NULL,\n"
            + "  `b` bigint(20) NOT NULL,\n"
            + "  `c` bigint(20) NOT NULL,\n"
            + "  PRIMARY KEY(`a`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`c`) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        for (int i = 0; i < 15; i++) {
            String insert = "insert into " + tableName + "(a,b,c) values(" + i + "," + (i + 1) + "," + (i + 2) + ")";
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        }

        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        Callable<Void> backfillTask = () -> {
            Connection connection = null;
            try {
                connection = getPolardbxConnection();
                // Use repartition to check since it can create shard / single / broadcast GSI
                // Rely on GSI checker to find out inconsistency between primary table and GSI
                final String createIndex =
                    "/*+TDDL:CMD_EXTRA(GSI_BACKFILL_BATCH_SIZE=1,GSI_BACKFILL_SPEED_LIMITATION=1,"
                        + "GSI_BACKFILL_SPEED_MIN=1,GSI_BACKFILL_PARALLELISM=4)*/ alter table "
                        + tableName + " " + gsiPartitionDef;
                JdbcUtil.executeUpdateSuccess(connection, createIndex);
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            return null;
        };

        Callable<Void> updateTask = () -> {
            Connection connection = null;
            try {
                connection = getPolardbxConnection();
                // wait to let backfill thread proceed
                Thread.sleep(8 * 1000);
                String update = "trace update " + tableName + " set a=-1 where a=14";
                JdbcUtil.executeUpdateSuccess(connection, update);
                System.out.println(getTrace(connection));
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            return null;
        };

        ArrayList<Future<Void>> results = new ArrayList<>();
        results.add(threadPool.submit(backfillTask));
        results.add(threadPool.submit(updateTask));

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void testGsiSetTimestamp() throws Exception {
        String tableName = "update_gsi_set_timestamp_test_tb";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql = String.format(
            "create table %s (id int, b int, t timestamp(6) default current_timestamp(6) on update current_timestamp(6)) partition by hash(b) PARTITIONS 3;",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("create global index %s on %s(b) covering(t) partition by hash(b) PARTITIONS 3;", gsiName,
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,b) values(1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Thread.sleep(1000);
        sql = String.format("update %s set id=1,b=2 where id=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testGsiSet() throws Exception {
        String tableName = "update_gsi_set_test_tb";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql = String.format(
            "create table %s (id int, b int, t timestamp(6) default current_timestamp(6) on update current_timestamp(6)) partition by hash(b) PARTITIONS 3;",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("create global index %s on %s(b) covering(t) partition by hash(b) PARTITIONS 3;", gsiName,
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,b) values(1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Thread.sleep(1000);
        sql = String.format("update %s set b=2,id=100 where id=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testRelocateSkipHint() throws Exception {
        String tableName = "update_relocate_skip_hint_tb";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String sql =
            String.format("create table %s (id int primary key, b int, c int) partition by hash(id) PARTITIONS 3;",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(id,b,c) values(1,2,3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Skip
        sql = String.format("trace update %s set id=1 where id=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertEquals(getTrace(tddlConnection).size(), 1);

        // Push UPDATE
        sql = String.format(
            "trace /*+TDDL:CMD_EXTRA(DML_RELOCATE_SKIP_UNCHANGED_ROW=FALSE)*/ update %s set id=1 where id=1",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertEquals(getTrace(tddlConnection).size(), 2);

        sql = String.format("create global index %s on %s(c) covering(b) partition by hash(c) PARTITIONS 3;", gsiName,
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Skip
        sql = String.format("trace update %s set id=1,c=3 where id=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertEquals(getTrace(tddlConnection).size(), 1);

        // Push UPDATE
        sql = String.format(
            "trace /*+TDDL:CMD_EXTRA(DML_RELOCATE_SKIP_UNCHANGED_ROW=FALSE)*/ update %s set id=1,c=3 where id=1",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        System.out.println(getTrace(tddlConnection));
        assertEquals(getTrace(tddlConnection).size(), 3);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testRelocate() {
        String tableName = "update_relocate_tb";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createSql = String.format(
            "create table %s (id int primary key, a varchar(100), b TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)) ",
            tableName);
        String partitionDef = "partition by hash(id) PARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);

        String insert = String.format("insert into %s (id,a) values (1, 'fdas')", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String update = String.format("update %s set id=1,a=0 where id=1", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testUgsi() throws SQLException {
        String tableName = "update_ugsi_tb";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int) partition by range(`c`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String createGsiSql = String.format(
            "create global unique index %s on %s(b) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);

        String sql = String.format("insert into %s values(1,null,-5),(1,null,5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("update %s set b=1 where c=-5", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testUgsi1() throws SQLException {
        String tableName = "update_ugsi_tb1";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int) partition by range(`c`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String createGsiSql = String.format(
            "create global unique index %s on %s(b) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);

        String sql = String.format("insert into %s values(1,null,null),(1,null,5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("update %s set b=1 where c is null", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testUgsi2() throws SQLException {
        String tableName = "update_ugsi_tb2";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int) partition by range(`c`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String createGsiSql = String.format(
            "create global unique index %s on %s(b) partition by range(`b`) (partition p0 values less than(0), partition p1 values less than(10), partition p2 values less than MAXVALUE)",
            gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);

        String sql = String.format("insert into %s values(1,null,null),(1,null,5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("update %s set c=-5 where c is null", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testSingleTablePushdown() throws SQLException {
        String tableName1 = "update_test_single_tbl_1";
        String tableName2 = "update_test_single_tbl_2";

        String create1 = String.format("create table %s (a int primary key, b int) single", tableName1);
        String create2 = String.format("create table %s (a int primary key, b int) single", tableName2);

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create2);

        String sql = String.format("insert into %s values (1,2)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("insert into %s values (1,2)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("update %s set b=10+(select b from %s) where a=1", tableName1, tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet rs = JdbcUtil.executeQuery(String.format("select * from %s", tableName1), tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getString(1).equalsIgnoreCase("1"));
        Assert.assertTrue(rs.getString(2).equalsIgnoreCase("12"));
        rs.close();
    }

    @Test
    public void testUpdateMultiTableCol() throws SQLException {
        String tableName1 = "update_test_multi_tbl_col_1";
        String tableName2 = "update_test_multi_tbl_col_2";

        String create1 = String.format("create table %s (a int primary key, b int) partition by hash(a)", tableName1);
        String create2 =
            String.format("create table %s (c int primary key, d int, e int, f int) partition by hash(c)", tableName2);

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create2);

        String gsiName1 = tableName1 + "_gsi";
        String createGsi = String.format("create global index %s on %s(b) partition by hash(b)", gsiName1, tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String sql = String.format("insert into %s values (1,2)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("insert into %s values (1,2,3,4)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("update %s as t1 inner join %s as t2 on t1.a=t2.c set t2.d=40,t2.e=40,t2.f=40,t1.b=20",
            tableName1, tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        ResultSet rs = JdbcUtil.executeQuery(String.format("select * from %s", tableName1), tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getString(1).equalsIgnoreCase("1"));
        Assert.assertTrue(rs.getString(2).equalsIgnoreCase("20"));
        rs.close();

        rs = JdbcUtil.executeQuery(String.format("select * from %s", tableName2), tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getString(1).equalsIgnoreCase("1"));
        Assert.assertTrue(rs.getString(2).equalsIgnoreCase("40"));
        Assert.assertTrue(rs.getString(3).equalsIgnoreCase("40"));
        Assert.assertTrue(rs.getString(4).equalsIgnoreCase("40"));
        rs.close();
    }

    @Test
    public void testUpdateZeroDate() throws SQLException {
        String tableName = "update_zero_date_tbl";

        String[] tddlKeyDefs = {
            "primary key (a)", "primary key (a,b)", "primary key (a), global unique index g1(c) partition by hash(c)",
            "primary key (a,b), clustered index g1(c) partition by hash(c)"};
        String[] mysqlKeyDefs = {
            "primary key (a)", "primary key (a,b)", "primary key (a), unique index g1(c)",
            "primary key (a,b), index g1(c)"};
        String createSql = String.format(
            "create table %s (a int, b datetime DEFAULT '0000-00-00 00:00:00', c int, d int, %%s) ", tableName);
        String[] partDefs =
            {"partition by hash(a)", "partition by hash(b)", "partition by hash(c)", "single", "broadcast"};

        int round = 0;
        for (int i = 0; i < tddlKeyDefs.length; i++) {
            for (String partDef : partDefs) {
                boolean hasGsi = (tddlKeyDefs[i].contains("global") || tddlKeyDefs[i].contains("clustered"));
                if (hasGsi && !partDef.contains("partition")) {
                    continue;
                }
                System.out.println("Round " + round++);
                System.out.println(tddlKeyDefs[i]);
                System.out.println(partDef);

                final String polardbxTable = String.format(createSql, tddlKeyDefs[i]) + partDef;
                final String mysqlTable = String.format(createSql, mysqlKeyDefs[i]);

                final List<String> sqlExecuted = new ArrayList<>();
                boolean succeed = false;

                try {
                    dropTableIfExists(tableName);
                    dropTableIfExistsInMySql(tableName);
                    JdbcUtil.executeUpdateSuccess(tddlConnection, polardbxTable);
                    JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlTable);

                    String insert = String.format("insert into %s(a,c,d) values (1,1,1)", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
                    sqlExecuted.add(insert);

                    String update = String.format("update %s set c=2 where a=1", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }

                    insert = String.format("insert into %s(a,b,c,d) values (3,'2022-10-10 10:10:10',3,3)", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
                    sqlExecuted.add(insert);

                    update = String.format("update %s set b='0000-00-00 00:00:00', c=4 where a=3", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }

                    update = String.format("update %s set a=a+1 where b='0000-00-00 00:00:00'", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }

                    update = String.format("update %s set c=c+1 where a=4", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }

                    update = String.format("update %s set d=d+1 where a=4", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }

                    update = String.format("update %s set d=last_insert_id(d+1) where a=4", tableName);
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
                    sqlExecuted.add(update);

                    selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                    if (hasGsi) {
                        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, "g1"));
                    }
                    succeed = true;
                } finally {
                    if (!succeed) {
                        System.out.println("MySQL Table: \n" + mysqlTable);
                        System.out.println("PolarDB-X Table: \n" + polardbxTable);
                        System.out.println(String.join(";", sqlExecuted));
                        System.out.println();
                    }
                }
            }
        }
    }

    @Test
    public void testUpdateCurrentTimestamp() throws SQLException {
        String tableName = "update_cur_ts_tbl";
        String gsiName = tableName + "_gsi";

        String create = String.format(
            "create table %s (a int primary key, b timestamp default '2022-12-12 12:12:12' on update current_timestamp(), c int) partition by hash(a)",
            tableName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        create = String.format("create global index %s on %s(c) covering (b) partition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert = String.format("insert into %s(a,c) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String update = String.format("update %s set c=2", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        update = String.format("update %s set a=1,c=2", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testUpdateCurrentTimestamp1() throws SQLException {
        String tableName = "update_cur_ts_tbl1";
        String gsiName = tableName + "_gsi";

        String create = String.format(
            "create table %s (a int primary key, b timestamp default '2022-12-12 12:12:12' on update current_timestamp(), c int) partition by hash(a)",
            tableName);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        create = String.format("create global index %s on %s(b) partition by hash(b)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert = String.format("insert into %s(a,c) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String update = String.format("update %s set a=1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        update = String.format("update %s set a=1,c=2", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);

        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testUpdateBinaryFunc1() throws SQLException {
        String tableName = "update_update_binary_tbl1";
        String create = String.format("create table %s (a int primary key auto_increment, b varbinary(32))", tableName);
        String partDef = "partition by key(b)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234"};

        for (int i = 0; i < binaryValues.length; i++) {
            for (int j = 0; j < 10; j++) {
                String insert = String.format("insert into %s values (null, %s)", tableName, binaryValues[i]);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
            }
            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String update =
                String.format("update %s set b=%s where b=%s", tableName, binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String hint = "/*+TDDL:CMD_EXTRA(ENABLE_PUSH_PROJECT=FALSE)*/";
            String update = hint + String.format("update %s set b=%s where b=%s", tableName, binaryValues[i],
                binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String hint = "/*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_BATCH_SIZE=1)*/";
            String update = hint + String.format("update %s set b=%s where b=%s", tableName, binaryValues[i],
                binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String hint = "/*+TDDL:CMD_EXTRA(ENABLE_PUSH_PROJECT=FALSE,UPDATE_DELETE_SELECT_BATCH_SIZE=1)*/";
            String update = hint + String.format("update %s set b=%s where b=%s", tableName, binaryValues[i],
                binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testUpdateBinaryFunc2() throws SQLException {
        String tableName = "update_update_binary_tbl2";
        String create = String.format("create table %s (a int primary key auto_increment, b varbinary(32))", tableName);
        String partDef = "partition by key(a)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] binaryValues = new String[] {"unhex('BBE5')", "0xBBE6", "1234"};

        for (int i = 0; i < binaryValues.length; i++) {
            for (int j = 0; j < 10; j++) {
                String insert = String.format("insert into %s values (null, %s)", tableName, binaryValues[i]);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
            }
            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String update =
                String.format("update %s set b=%s where b=%s", tableName, binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL)*/";
            String update =
                hint + String.format("update %s set b=%s where b=%s", tableName, binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }

        for (int i = 0; i < binaryValues.length; i++) {
            String hint = "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,UPDATE_DELETE_SELECT_BATCH_SIZE=1)*/";
            String update =
                hint + String.format("update %s set b=%s where b=%s", tableName, binaryValues[i], binaryValues[i]);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);

            selectContentSameAssert("select b from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * 主表拆分键和gsi拆分键不一样
     * update 主表拆分键
     * 主表 UPDATE 转 SELECT + DELETE + INSERT
     * 处于write only 阶段的gsi UPDATE 转 SELECT + DELETE + INSERT
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly2() throws SQLException {
        final String tableName = "update_test_tb_with_write_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String gsiName = "g_update_c2_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "insert into " + tableName
                + "(c1, c2, c8) values(4, 5, '2020-06-16 06:49:32'), (2, 3, '2020-06-16 06:49:32'), (3, 4, '2020-06-16 06:49:32');";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        final String hint = "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_TRIVIAL_UPDATE=FALSE)*/ ";
        final String updateSql = "update " + tableName + " set c1 = 38 where c1 = 4";
        final String updateSql2 = "update " + tableName + " set c1 = 4 where c1 = 38";

        // checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // write only
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, updateSql, "trace " + hint + updateSql, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        org.junit.Assert.assertThat(trace.size(), Matchers.is(1 + 2 + 2));

        // public
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, updateSql2, "trace " + updateSql2, null, true);
        final List<List<String>> trace2 = getTrace(tddlConnection);

        org.junit.Assert.assertThat(trace2.size(), Matchers.is(1 + 2 + 1));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // checkGsi(tddlConnection, gsiName);
    }
}
