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

package com.alibaba.polardbx.qatest.failpoint.newpartition.failpoint;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.ddl.auto.dag.BaseDdlEngineTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author luoyanxin
 */
public class BaseAlterTableGroupFailPointTestCase extends BaseDdlEngineTestCase {

    protected static final String FAIL_POINT_SCHEMA_NAME = "fail_point";

    public static final String PARTITION_BY_BIGINT_KEY =
        " partition by key(id) partitions 8";
    public static final String PARTITION_BY_INT_KEY =
        " partition by key(c_int_32) partitions 8";
    public static final String PARTITION_BY_BIGINT_HASH =
        " partition by hash(id) partitions 8";
    public static final String PARTITION_BY_INT_HASH =
        " partition by hash(c_int_32) partitions 8";

    public static final String PARTITION_BY_BIGINT_RANGE =
        " partition by range(id) (partition p1 values less than(100040), "
            + "partition p2 values less than(100080), "
            + "partition p3 values less than(100120), "
            + "partition p4 values less than(100160), "
            + "partition p5 values less than(100200), "
            + "partition p6 values less than(100240), "
            + "partition p7 values less than(100280), "
            + "partition p8 values less than(100320))";
    public static final String PARTITION_BY_BIGINT_LIST =
        " partition by list(id) ("
            + " partition p1 values in (100000,100001,100002,100003,100004,100005,100006,100007,100008,100009),"
            + " partition p2 values in (100010,100011,100012,100013,100014,100015,100016,100017,100018,100019),"
            + " partition p3 values in (100020,100021,100022,100023,100024,100025,100026,100027,100028,100029),"
            + " partition p4 values in (100030,100031,100032,100033,100034,100035,100036,100037,100038,100039),"
            + " partition p5 values in (100040,100041,100042,100043,100044,100045,100046,100047,100048,100049),"
            + " partition p6 values in (100050,100051,100052,100053,100054,100055,100056,100057,100058,100059),"
            + " partition p7 values in (100060,100061,100062,100063,100064,100065,100066,100067,100068,100069),"
            + " partition p8 values in (100070,100071,100072,100073,100074,100075,100076,100077,100078,100079,"
            + "100080,100081,100082,100083,100084,100085,100086,100087,100088,100089,"
            + "100090,100091,100092,100093,100094,100095,100096,100097,100098,100099,"
            + "100100,100110,100120,100130,100140,100150,100160,100170,100180,100190,"
            + "100101,100111,100121,100131,100141,100151,100161,100171,100181,100191,"
            + "100102,100112,100122,100132,100142,100152,100162,100172,100182,100192,"
            + "100103,100113,100123,100133,100143,100153,100163,100173,100183,100193,"
            + "100104,100114,100124,100134,100144,100154,100164,100174,100184,100194,"
            + "100105,100115,100125,100135,100145,100155,100165,100175,100185,100195,"
            + "100106,100116,100126,100136,100146,100156,100166,100176,100186,100196,"
            + "100107,100117,100127,100137,100147,100157,100167,100177,100187,100197,"
            + "100108,100118,100128,100138,100148,100158,100168,100178,100188,100198,"
            + "100109,100119,100129,100139,100149,100159,100169,100179,100189,100199,"
            + "100200,100210,100220,100230,100240,100250,100260,100270,100280,100290,"
            + "100201,100211,100221,100231,100241,100251,100261,100271,100281,100291,"
            + "100202,100212,100222,100232,100242,100252,100262,100272,100282,100292,"
            + "100203,100213,100223,100233,100243,100253,100263,100273,100283,100293,"
            + "100204,100214,100224,100234,100244,100254,100264,100274,100284,100294,"
            + "100205,100215,100225,100235,100245,100255,100265,100275,100285,100295,"
            + "100206,100216,100226,100236,100246,100256,100266,100276,100286,100296,"
            + "100207,100217,100227,100237,100247,100257,100267,100277,100287,100297,"
            + "100208,100218,100228,100238,100248,100258,100268,100278,100288,100298,"
            + "100209,100219,100229,100239,100249,100259,100269,100279,100289,100299))";

    public static final String BROAD_CAST =
        " broadcast";

    protected boolean enableFpEachDdlTaskExecuteTwiceTest = true;
    protected boolean enableFpRandomPhysicalDdlExceptionTest = true;
    protected boolean enableFpRandomFailTest = true;
    protected boolean enableFpRandomSuspendTest = true;
    protected boolean enableFpEachDdlTaskBackAndForth = true;
    protected static String tgName = "sp_tg";
    protected static String tb1 = "sp_t1";
    protected static String tb2 = "sp_t2";
    protected static String tb3 = "sp_t3";
    protected static String tb4 = "sp_t4";
    protected static String unionAllTables =
        "select * from (select 1 as col, t1.* from " + tb1 + " t1 union all " + "select 2 as col, t2.* from " + tb2
            + " t2 union all "
            + "select 3 as col, t3.* from " + tb3
            + " t3 union all " + "select 4 as col, t4.* from " + tb4 + " t4) a order by col, id";

    public static List<Pair<PartitionStrategy, String>> partitionStrategy = new ArrayList<>(Arrays
        .asList(new Pair<>(PartitionStrategy.KEY, PARTITION_BY_BIGINT_KEY),
            new Pair<>(PartitionStrategy.HASH, PARTITION_BY_BIGINT_HASH),
            new Pair<>(PartitionStrategy.RANGE, PARTITION_BY_BIGINT_RANGE),
            new Pair<>(PartitionStrategy.LIST, PARTITION_BY_BIGINT_LIST))
    );

    protected Pair<PartitionStrategy, String[]> createTableStats;

    public BaseAlterTableGroupFailPointTestCase() {
    }

    protected void doPrepare() {
        createTables(createTableStats.getValue(), true);
    }

    @Parameterized.Parameters(name = "{index}:statements={0}")
    public static List<Pair<PartitionStrategy, String[]>[]> prepareDate() {
        List<Pair<PartitionStrategy, String[]>[]> statements = new ArrayList<>();
        for (Pair<PartitionStrategy, String> item : partitionStrategy) {
            Pair<PartitionStrategy, String[]> pair = new Pair<>(item.left, new String[] {
                ExecuteTableSelect.getFullTypeMultiPkTableDef(tb1, item.right),
                ExecuteTableSelect.getFullTypeMultiPkTableDef(tb2, item.right),
                ExecuteTableSelect.getFullTypeMultiPkTableDef(tb3, item.right),
                ExecuteTableSelect.getFullTypeMultiPkTableDef(tb4, item.right)});
            Pair<PartitionStrategy, String[]> pairs[] = new Pair[1];
            pairs[0] = pair;
            statements.add(pairs);
        }

        return statements;
    }

    protected static Connection failPointConnection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.createPartDatabase(tmpConnection, FAIL_POINT_SCHEMA_NAME);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.dropDatabase(tmpConnection, FAIL_POINT_SCHEMA_NAME);
        }
    }

    @Before
    public void doBefore() throws SQLException {
        if (!usingNewPartDb()) {
            return;
        }
        useDb(tddlConnection, FAIL_POINT_SCHEMA_NAME);
        this.failPointConnection = tddlConnection;
        clearFailPoints();
        JdbcUtil.executeUpdateSuccess(failPointConnection, "create tablegroup if not exists " + tgName);
    }

    @After
    public void doAfter() throws SQLException {
        if (!usingNewPartDb()) {
            return;
        }
        clearFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {
        if (!usingNewPartDb()) {
            return;
        }
        if (!enableFpEachDdlTaskBackAndForth) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {
        if (!usingNewPartDb()) {
            return;
        }
        if (!enableFpEachDdlTaskExecuteTwiceTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {
        if (!usingNewPartDb()) {
            return;
        }
        if (!enableFpRandomPhysicalDdlExceptionTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {
        if (!usingNewPartDb()) {
            return;
        }
        if (!enableFpRandomFailTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "30");
        execDdlWithFailPoints();
        doClean();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {
        if (!usingNewPartDb()) {
            return;
        }
        if (!enableFpRandomSuspendTest) {
            return;
        }
        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "30,3000");
        execDdlWithFailPoints();
    }

    protected void execDdlWithFailPoints() {
    }

    protected void doClean() {
    }

    protected void createTables(String[] CreateTableStrs, boolean setTableGroup) {

        JdbcUtil
            .executeUpdateSuccess(failPointConnection, "drop table if exists " + tb1);
        JdbcUtil
            .executeUpdateSuccess(failPointConnection, "drop table if exists " + tb2);
        JdbcUtil
            .executeUpdateSuccess(failPointConnection, "drop table if exists " + tb3);
        JdbcUtil
            .executeUpdateSuccess(failPointConnection, "drop table if exists " + tb4);
        for (int i = 0; i < CreateTableStrs.length; i++) {
            JdbcUtil
                .executeUpdateSuccess(failPointConnection, CreateTableStrs[i]);
            if (i == 0 && setTableGroup) {
                JdbcUtil
                    .executeUpdateSuccess(failPointConnection,
                        "alter table " + tb1 + " set tablegroup='" + tgName + "'");
            }
            // Prepare data
            List<String> inserts = prepareInsertStatements("sp_t" + String.valueOf(i + 1));
            for (String insert : inserts) {

                Statement stmt = null;
                try {
                    stmt = failPointConnection.createStatement();
                    stmt.executeUpdate(insert);
                } catch (SQLException e) {
                    assertWithMessage("语句并未按照预期执行成功:" + insert + e.getMessage()).fail();
                } finally {
                    JdbcUtil.close(stmt);
                }
            }
        }
    }

    protected List<String> prepareInsertStatements(String tableName) {
        return ImmutableList.<String>builder()
            .add("insert into " + tableName
                + "(id,c_bit_1) values(null,0)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_bit_1) values(null,1)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_bit_8) values(null,0)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_bit_8) values(null,1)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_bit_8) values(null,2)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_bit_8) values(null,255)                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_bit_16) values(null,0)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_16) values(null,1)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_16) values(null,2)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_16) values(null,65535)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_bit_32) values(null,0)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_32) values(null,1)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_32) values(null,2)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_32) values(null,4294967295)                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_bit_64) values(null,0)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_64) values(null,1)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_64) values(null,2)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_bit_64) values(null,18446744073709551615)                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1) values(null,-1)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1) values(null,0)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1) values(null,1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1) values(null,127)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1_un) values(null,0)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1_un) values(null,1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1_un) values(null,127)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_1_un) values(null,255)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4) values(null,-1)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4) values(null,0)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4) values(null,1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4) values(null,127)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4_un) values(null,0)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4_un) values(null,1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4_un) values(null,127)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_4_un) values(null,255)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8) values(null,-1)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8) values(null,0)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8) values(null,1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8) values(null,127)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8_un) values(null,0)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8_un) values(null,1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8_un) values(null,127)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_tinyint_8_un) values(null,255)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_smallint_1) values(null,-1)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_smallint_1) values(null,0)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_smallint_1) values(null,1)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_smallint_1) values(null,32767)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16) values(null,-1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16) values(null,0)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16) values(null,1)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16) values(null,32767)                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16_un) values(null,0)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16_un) values(null,1)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_smallint_16_un) values(null,32767)                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_1) values(null,-1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_1) values(null,0)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_1) values(null,1)                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_1) values(null,65535)                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24) values(null,-1)                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24) values(null,0)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24) values(null,1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24) values(null,32767)                                                                  ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24_un) values(null,0)                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24_un) values(null,1)                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_mediumint_24_un) values(null,32767)                                                               ;")
            .add("insert into " + tableName
                + "(id,c_int_1) values(null,-1)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_int_1) values(null,0)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_int_1) values(null,1)                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_int_1) values(null,32767)                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_int_32) values(null,-1)                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_int_32) values(null,0)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_int_32) values(null,1)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_int_32) values(null,32767)                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_int_32_un) values(null,0)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_int_32_un) values(null,1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_int_32_un) values(null,32767)                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_bigint_1) values(null,-1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_bigint_1) values(null,0)                                                                             ;")
            .add("insert into " + tableName
                + "(id,c_bigint_1) values(null,1)                                                                             ;")
            .add("insert into " + tableName
                + "(id,c_bigint_1) values(null,32767)                                                          ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64) values(null,-1)                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64) values(null,0)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64) values(null,1)                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64) values(null,32767)                                                         ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64_un) values(null,0)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64_un) values(null,1)                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_bigint_64_un) values(null,32767)                                                      ;")
            .add("insert into " + tableName
                + "(id,c_decimal) values(null,'100.000')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_decimal) values(null,'100.003')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_decimal) values(null,'-100.003')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_decimal) values(null,'-100.0000001')                                                                 ;")
            .add("insert into " + tableName
                + "(id,c_decimal_pr) values(null,'100.000')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_decimal_pr) values(null,'100.003')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_decimal_pr) values(null,'-100.003')                                                                  ;")
            .add("insert into " + tableName
                + "(id,c_decimal_pr) values(null,'-100.0000001')                                                              ;")
            .add("insert into " + tableName
                + "(id,c_float) values(null,'100.000')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_float) values(null,'100.003')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_float) values(null,'-100.003')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_float) values(null,'-100.0000001')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_float_pr) values(null,'100.000')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_float_pr) values(null,'100.003')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_float_pr) values(null,'-100.003')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_float_pr) values(null,'-100.0000001')                                                                ;")
            .add("insert into " + tableName
                + "(id,c_float_un) values(null,'100.000')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_float_un) values(null,'100.003')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_double) values(null,'100.000')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_double) values(null,'100.003')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_double) values(null,'-100.003')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_double) values(null,'-100.0000001')                                                                  ;")
            .add("insert into " + tableName
                + "(id,c_double_pr) values(null,'100.000')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_double_pr) values(null,'100.003')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_double_pr) values(null,'-100.003')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_double_pr) values(null,'-100.0000001')                                                               ;")
            .add("insert into " + tableName
                + "(id,c_double_un) values(null,'100.000')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_double_un) values(null,'100.003')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'0000-00-00')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'1999-12-31')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'0000-00-00 01:01:01')                                                             ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'1969-09-00')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'2018-00-00')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_date) values(null,'2017-12-12')                                                                      ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'0000-00-00 00:00:00')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'1999-12-31 23:59:59')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'0000-00-00 01:01:01')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'1969-09-00 23:59:59')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'2018-00-00 00:00:00')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime) values(null,'2017-12-12 23:59:59')                                                         ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'0000-00-00 00:00:00.0')                                                     ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'1999-12-31 23:59:59.9')                                                     ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'0000-00-00 01:01:01.12')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'1969-09-00 23:59:59.06')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'2018-00-00 00:00:00.04')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_datetime_1) values(null,'2017-12-12 23:59:59.045')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'0000-00-00 00:00:00.000')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'1999-12-31 23:59:59.999')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'0000-00-00 01:01:01.121')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'1969-09-00 23:59:59.0006')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'2018-00-00 00:00:00.0004')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_datetime_3) values(null,'2017-12-12 23:59:59.00045')                                                 ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'0000-00-00 00:00:00.000000')                                                ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'1999-12-31 23:59:59.999999')                                                ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'0000-00-00 01:01:01.121121')                                                ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'1969-09-00 23:59:59.0000006')                                               ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'2018-00-00 00:00:00.0000004')                                               ;")
            .add("insert into " + tableName
                + "(id,c_datetime_6) values(null,'2017-12-12 23:59:59.00000045')                                              ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'2000-01-01 00:00:00')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'1999-12-31 23:59:59')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'2000-01-01 01:01:01')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'1999-09-01 23:59:59')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'2018-01-01 00:00:00')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp) values(null,'2017-12-12 23:59:59')                                                        ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'2000-01-01 00:00:00.0')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'1999-12-31 23:59:59.9')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'2000-01-01 01:01:01.12')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'1999-09-01 23:59:59.06')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'2018-01-01 00:00:00.04')                                                   ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_1) values(null,'2017-12-12 23:59:59.045')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'2000-01-01 00:00:00.000')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'1999-12-31 23:59:59.999')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'2000-01-01 01:01:01.121')                                                  ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'1999-09-01 23:59:59.0006')                                                 ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'2018-01-01 00:00:00.0004')                                                 ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_3) values(null,'2017-12-12 23:59:59.00045')                                                ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'2000-01-01 00:00:00.000000')                                               ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'1999-12-31 23:59:59.999999')                                               ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'2000-01-01 01:01:01.121121')                                               ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'1999-09-01 23:59:59.0000006')                                              ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'2018-01-01 00:00:00.0000004')                                              ;")
            .add("insert into " + tableName
                + "(id,c_timestamp_6) values(null,'2017-12-12 23:59:59.00000045')                                             ;")
            .add("insert into " + tableName
                + "(id,c_time) values(null,'00:00:00')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_time) values(null,'01:01:01')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_time) values(null,'23:59:59')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_time_1) values(null,'00:00:00.1')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_time_1) values(null,'01:01:01.6')                                                                    ;")
            .add("insert into " + tableName
                + "(id,c_time_1) values(null,'23:59:59.45')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_time_3) values(null,'00:00:00.111')                                                                  ;")
            .add("insert into " + tableName
                + "(id,c_time_3) values(null,'01:01:01.106')                                                                  ;")
            .add("insert into " + tableName
                + "(id,c_time_3) values(null,'23:59:59.00045')                                                                ;")
            .add("insert into " + tableName
                + "(id,c_time_6) values(null,'00:00:00.111111')                                                               ;")
            .add("insert into " + tableName
                + "(id,c_time_6) values(null,'01:01:01.106106')                                                               ;")
            .add("insert into " + tableName
                + "(id,c_time_6) values(null,'23:59:59.00000045')                                                             ;")
            .add("insert into " + tableName
                + "(id,c_year) values(null,'0000')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_year) values(null,'1999')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_year) values(null,'1970')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_year) values(null,'2000')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_year_4) values(null,'0000')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_year_4) values(null,'1999')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_year_4) values(null,'1970')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_year_4) values(null,'2000')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_char) values(null,'11')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_char) values(null,'99')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_char) values(null,'a中国a')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_varchar) values(null,'11')                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_varchar) values(null,'99')                                                                           ;")
            .add("insert into " + tableName
                + "(id,c_varchar) values(null,'a中国a')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_binary) values(null,'11')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_binary) values(null,'99')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_binary) values(null,'a中国a')                                                                        ;")
            .add("insert into " + tableName
                + "(id,c_varbinary) values(null,'11')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_varbinary) values(null,'99')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_varbinary) values(null,'a中国a')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_blob_tiny) values(null,'11')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_blob_tiny) values(null,'99')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_blob_tiny) values(null,'a中国a')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_blob) values(null,'11')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_blob) values(null,'99')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_blob) values(null,'a中国a')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_blob_medium) values(null,'11')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_blob_medium) values(null,'99')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_blob_medium) values(null,'a中国a')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_blob_long) values(null,'11')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_blob_long) values(null,'99')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_blob_long) values(null,'a中国a')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_text_tiny) values(null,'11')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_text_tiny) values(null,'99')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_text_tiny) values(null,'a中国a')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_text) values(null,'11')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_text) values(null,'99')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_text) values(null,'a中国a')                                                                          ;")
            .add("insert into " + tableName
                + "(id,c_text_medium) values(null,'11')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_text_medium) values(null,'99')                                                                       ;")
            .add("insert into " + tableName
                + "(id,c_text_medium) values(null,'a中国a')                                                                   ;")
            .add("insert into " + tableName
                + "(id,c_text_long) values(null,'11')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_text_long) values(null,'99')                                                                         ;")
            .add("insert into " + tableName
                + "(id,c_text_long) values(null,'a中国a')                                                                     ;")
            .add("insert into " + tableName
                + "(id,c_enum) values(null,'a')                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_enum) values(null,'b')                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_enum) values(null,NULL)                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_set) values(null,'a')                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_set) values(null,'b,a')                                                                              ;")
            .add("insert into " + tableName
                + "(id,c_set) values(null,'b,c,a')                                                                            ;")
            .add("insert into " + tableName
                + "(id,c_set) values(null,'c')                                                                                ;")
            .add("insert into " + tableName
                + "(id,c_set) values(null,NULL)                                                                               ;")
            .add("insert into " + tableName
                + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": 10}')                                                    ;")
            .add("insert into " + tableName
                + "(id,c_json) values(null,'{\"k1\": \"v1\", \"k2\": [10, 20]}')                                              ;")
            .add("insert into " + tableName
                + "(id,c_json) values(null,NULL)                                                                              ;")
            .add("insert into " + tableName
                + "(id, c_point) VALUE (null,ST_GEOMFROMTEXT('POINT(15 20)'))                                                ;")
            .add("insert into " + tableName
                + "(id, c_linestring) VALUE (null,ST_GEOMFROMTEXT('LINESTRING(0 0, 10 10, 20 25, 50 60)'))                   ;")
            .add("insert into " + tableName
                + "(id, c_polygon) VALUE (null,ST_GEOMFROMTEXT('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))')) ;")
            .add("insert into " + tableName
                + "(id, c_geometory) VALUE (null,ST_GEOMFROMTEXT('POINT(15 20)'))                                            ;")
            .build();
    }

}
