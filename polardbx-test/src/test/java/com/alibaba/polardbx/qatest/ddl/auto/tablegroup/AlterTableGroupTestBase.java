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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * @author luoyanxin
 */
public class AlterTableGroupTestBase extends DDLBaseNewDBTestCase {

    protected String logicalDatabase;
    protected static List<String> finalTableStatus;
    protected static String originUseDbName;
    protected static String originSqlMode;
    protected static final boolean needAutoDropDbAfterTest = true;
    protected static final boolean printExecutedSqlLog = false;
    protected static final String tableGroupName = "altertablegroup_tg";
    protected static final String pk = "id";

    public static final String PARTITION_BY_BIGINT_KEY =
        " partition by key(id) partitions 3";
    public static final String PARTITION_BY_INT_KEY =
        " partition by key(c_int_32) partitions 3";
    public static final String PARTITION_BY_INT_BIGINT_KEY =
        " partition by key(c_int_32, id) partitions 3";
    public static final String PARTITION_BY_INT_BIGINT_HASH =
        " partition by hash(c_int_32, id) partitions 3";
    public static final String PARTITION_BY_MONTH_HASH =
        " partition by hash(month(c_datetime)) partitions 3";
    public static final String PARTITION_BY_BIGINT_RANGE =
        " partition by range(id) (partition p1 values less than(100040), "
            + "partition p2 values less than(100080), "
            + "partition p3 values less than(100120), "
            + "partition p4 values less than(100160), "
            + "partition p5 values less than(100200), "
            + "partition p6 values less than(100240), "
            + "partition p7 values less than(100280), "
            + "partition p8 values less than(100320))";
    public static final String PARTITION_BY_INT_BIGINT_RANGE_COL =
        " partition by range columns(c_int_32, id) (partition p1 values less than(10, 100040), "
            + "partition p2 values less than(20, 100080), "
            + "partition p3 values less than(30, 100120), "
            + "partition p4 values less than(40, 100160), "
            + "partition p5 values less than(50, 100200), "
            + "partition p6 values less than(60, 100240), "
            + "partition p7 values less than(70, 100280), "
            + "partition p8 values less than(300, 100320))";

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

    public static final String PARTITION_BY_INT_BIGINT_LIST =
        " partition by list columns(c_int_32, id) ("
            + " partition p1 values in ((1,100000),(1,100001),(1,100002),(1,100003),(1,100004),(1,100005),(1,100006),(1,100007),(1,100008),(1,100009)),"
            + " partition p2 values in ((1,100010),(1,100011),(1,100012),(1,100013),(1,100014),(1,100015),(1,100016),(1,100017),(1,100018),(1,100019)),"
            + " partition p3 values in ((1,100020),(1,100021),(1,100022),(1,100023),(1,100024),(1,100025),(1,100026),(1,100027),(1,100028),(1,100029)),"
            + " partition p4 values in ((1,100030),(1,100031),(1,100032),(1,100033),(1,100034),(1,100035),(1,100036),(1,100037),(1,100038),(1,100039)),"
            + " partition p5 values in ((1,100040),(1,100041),(1,100042),(1,100043),(1,100044),(1,100045),(1,100046),(1,100047),(1,100048),(1,100049)),"
            + " partition p6 values in ((1,100050),(1,100051),(1,100052),(1,100053),(1,100054),(1,100055),(1,100056),(1,100057),(1,100058),(1,100059)),"
            + " partition p7 values in ((1,100060),(1,100061),(1,100062),(1,100063),(1,100064),(1,100065),(1,100066),(1,100067),(1,100068),(1,100069)),"
            + " partition p8 values in ((1,100070),(1,100071),(1,100072),(1,100073),(1,100074),(1,100075),(1,100076),(1,100077),(1,100078),(1,100079),"
            + "(1,100080),(1,100081),(1,100082),(1,100083),(1,100084),(1,100085),(1,100086),(1,100087),(1,100088),(1,100089),"
            + "(1,100090),(1,100091),(1,100092),(1,100093),(1,100094),(1,100095),(1,100096),(1,100097),(1,100098),(1,100099),"
            + "(1,100100),(1,100110),(1,100120),(1,100130),(1,100140),(1,100150),(1,100160),(1,100170),(1,100180),(1,100190),"
            + "(1,100101),(1,100111),(1,100121),(1,100131),(1,100141),(1,100151),(1,100161),(1,100171),(1,100181),(1,100191),"
            + "(1,100102),(1,100112),(1,100122),(1,100132),(1,100142),(1,100152),(1,100162),(1,100172),(1,100182),(1,100192),"
            + "(1,100103),(1,100113),(1,100123),(1,100133),(1,100143),(1,100153),(1,100163),(1,100173),(1,100183),(1,100193),"
            + "(1,100104),(1,100114),(1,100124),(1,100134),(1,100144),(1,100154),(1,100164),(1,100174),(1,100184),(1,100194),"
            + "(1,100105),(1,100115),(1,100125),(1,100135),(1,100145),(1,100155),(1,100165),(1,100175),(1,100185),(1,100195),"
            + "(1,100106),(1,100116),(1,100126),(1,100136),(1,100146),(1,100156),(1,100166),(1,100176),(1,100186),(1,100196),"
            + "(1,100107),(1,100117),(1,100127),(1,100137),(1,100147),(1,100157),(1,100167),(1,100177),(1,100187),(1,100197),"
            + "(1,100108),(1,100118),(1,100128),(1,100138),(1,100148),(1,100158),(1,100168),(1,100178),(1,100188),(1,100198),"
            + "(1,100109),(1,100119),(1,100129),(1,100139),(1,100149),(1,100159),(1,100169),(1,100179),(1,100189),(1,100199),"
            + "(1,100200),(1,100210),(1,100220),(1,100230),(1,100240),(1,100250),(1,100260),(1,100270),(1,100280),(1,100290),"
            + "(1,100201),(1,100211),(1,100221),(1,100231),(1,100241),(1,100251),(1,100261),(1,100271),(1,100281),(1,100291),"
            + "(1,100202),(1,100212),(1,100222),(1,100232),(1,100242),(1,100252),(1,100262),(1,100272),(1,100282),(1,100292),"
            + "(1,100203),(1,100213),(1,100223),(1,100233),(1,100243),(1,100253),(1,100263),(1,100273),(1,100283),(1,100293),"
            + "(1,100204),(1,100214),(1,100224),(1,100234),(1,100244),(1,100254),(1,100264),(1,100274),(1,100284),(1,100294),"
            + "(1,100205),(1,100215),(1,100225),(1,100235),(1,100245),(1,100255),(1,100265),(1,100275),(1,100285),(1,100295),"
            + "(1,100206),(1,100216),(1,100226),(1,100236),(1,100246),(1,100256),(1,100266),(1,100276),(1,100286),(1,100296),"
            + "(1,100207),(1,100217),(1,100227),(1,100237),(1,100247),(1,100257),(1,100267),(1,100277),(1,100287),(1,100297),"
            + "(1,100208),(1,100218),(1,100228),(1,100238),(1,100248),(1,100258),(1,100268),(1,100278),(1,100288),(1,100298),"
            + "(1,100209),(1,100219),(1,100229),(1,100239),(1,100249),(1,100259),(1,100269),(1,100279),(1,100289),(1,100299)))";

    public static final String SINGLE_TABLE = " single";
    public static final String BROADCAST_TABLE = " broadcast";

    public static final String partitionMode = "partition_mode='partitioning'";

    /*static List<PartitionRuleInfo> partitionRuleInfos = new ArrayList<>(Arrays
        .asList(
            new PartitionRuleInfo(PartitionStrategy.KEY,
                (x, y) -> AlterTableGroupBaseTest.initData1(x, y),
                PARTITION_BY_BIGINT_KEY, null),
            new PartitionRuleInfo(PartitionStrategy.KEY,
                (x, y) -> AlterTableGroupBaseTest.initData2(x, y),
                PARTITION_BY_INT_KEY, null),
            new PartitionRuleInfo(PartitionStrategy.KEY,
                (x, y) -> AlterTableGroupBaseTest.initData2(x, y),
                PARTITION_BY_INT_BIGINT_KEY, null),
            new PartitionRuleInfo(PartitionStrategy.HASH,
                (x, y) -> AlterTableGroupBaseTest.initData2(x, y),
                PARTITION_BY_INT_BIGINT_HASH, null),
            new PartitionRuleInfo(PartitionStrategy.HASH,
                (x, y) -> AlterTableGroupBaseTest.initData3(x, y),
                PARTITION_BY_MONTH_HASH, null),
            new PartitionRuleInfo(PartitionStrategy.RANGE,
                (x, y) -> AlterTableGroupBaseTest.initData1(x, y),
                PARTITION_BY_BIGINT_RANGE, null),
            new PartitionRuleInfo(PartitionStrategy.RANGE_COLUMNS,
                (x, y) -> AlterTableGroupBaseTest.initData2(x, y),
                PARTITION_BY_INT_BIGINT_RANGE_COL, null),
            new PartitionRuleInfo(PartitionStrategy.LIST,
                (x, y) -> AlterTableGroupBaseTest.initData1(x, y),
                PARTITION_BY_BIGINT_LIST, null),
            new PartitionRuleInfo(PartitionStrategy.LIST_COLUMNS,
                (x, y) -> AlterTableGroupBaseTest.initData4(x, y),
                PARTITION_BY_INT_BIGINT_LIST, null))
    );*/

    private static final int parallel = 4;

    public AlterTableGroupTestBase(String logicalDatabase, List<String> finalTableStatus) {
        this.logicalDatabase = logicalDatabase;
        this.finalTableStatus = finalTableStatus;
    }

    @BeforeClass
    public static void setUpTestCase() throws Exception {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            String tddlSql = "use polardbx";
            JdbcUtil.executeUpdate(tddlConnection, tddlSql);

            String sql = "select database(),@@sql_mode";
            PreparedStatement stmt = JdbcUtil.preparedStatement(sql, tddlConnection);
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery();
                if (rs.next()) {
                    originUseDbName = (String) rs.getObject(1);
                    originSqlMode = (String) rs.getObject(2);
                }
                rs.close();
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void setUp(boolean recreateDB, PartitionRuleInfo partitionRuleInfo, boolean withConcurrentDml,
                      boolean forSubPartTest, boolean useTargetDnInstId, boolean createUgsi) {
        if (!usingNewPartDb()) {
            return;
        }

        partitionRuleInfo.connection = getTddlConnection1();
        prepareDdlAndData(recreateDB, partitionRuleInfo, forSubPartTest, createUgsi);

        String targetDnInstId = "";
        if (useTargetDnInstId) {
            targetDnInstId = getTargetDnInstIdForMove(partitionRuleInfo);
        }

        String command = String.format(partitionRuleInfo.alterTableGroupCommand, targetDnInstId);

        if (withConcurrentDml) {
            dmlWhilePartitionReorg(partitionRuleInfo, command, "t2");
        } else {
            executePartReorg(partitionRuleInfo.tableStatus, partitionRuleInfo.physicalTableBackfillParallel,
                partitionRuleInfo.usePhysicalTableBackfill, command);
        }
    }

    public void setUpForMovePart(boolean recreateDB, PartitionRuleInfo partitionRuleInfo, boolean forSubPartTest) {
        if (!usingNewPartDb()) {
            return;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        prepareDdlAndData(recreateDB, partitionRuleInfo, forSubPartTest, false);
        String targetDnInstId = getTargetDnInstIdForMove(partitionRuleInfo);
        String command = String.format("%s'%s'", partitionRuleInfo.alterTableGroupCommand, targetDnInstId);
        dmlWhilePartitionReorg(partitionRuleInfo, command, "t2");

    }

    protected String getTargetDnInstIdForMove(PartitionRuleInfo partitionRuleInfo) {
        String targetInstId = "";

        Map<String, String> partInstIdMap = getPartInstIdMap("t1");
        String sourceInstId = partInstIdMap.get(partitionRuleInfo.targetPart);

        List<String> instIds = getStorageInstIds();
        for (String instId : instIds) {
            if (!instId.equalsIgnoreCase(sourceInstId)) {
                targetInstId = instId;
                break;
            }
        }

        return targetInstId;
    }

    protected void reCreateDatabase(Connection tddlConnection, String targetDbName) {
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        tddlSql = "drop database if exists " + targetDbName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        tddlSql = "use information_schema";
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        tddlSql = "create database " + targetDbName + " " + partitionMode;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        tddlSql = "use " + targetDbName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        tddlSql = "create tablegroup " + tableGroupName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
    }

    private void prepareDdlAndData(boolean recreateDB, PartitionRuleInfo partitionRuleInfo, boolean forSubPartTest,
                                   boolean createUgsi) {
        if (recreateDB) {
            reCreateDatabase(partitionRuleInfo.connection, this.logicalDatabase);
            if (forSubPartTest) {
                partitionRuleInfo.setLogicalTableNames(Arrays.asList("t1", "t2"));
            }
            for (String tableName : partitionRuleInfo.getLogicalTableNames()) {
                createTable(tableName, partitionRuleInfo.getPartitionRule(), !forSubPartTest);
                partitionRuleInfo.prepareData(tableName, forSubPartTest ? 3 : 280);
            }
        }
        if (partitionRuleInfo.isNeedGenDml()) {
            partitionRuleInfo.prepareDml(this.logicalDatabase);
        }
    }

    private void createIndexed(String tableName) {
        String createGsi =
            String.format("alter table %s add unique global index g1 (id) partition by key(id) partitions 3",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
    }

    private void executePartReorg(ComplexTaskMetaManager.ComplexTaskStatus status,
                                  boolean phyParallelBackfill, boolean usePhysicalBackfill, String command) {
        String sqlHint = "";
        if (!status.isPublic()) {
            sqlHint = String.format(
                "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='%s')*/",
                status.name());
        } else if (phyParallelBackfill) {
            sqlHint =
                "/*+TDDL:CMD_EXTRA(PHYSICAL_TABLE_START_SPLIT_SIZE = 100, PHYSICAL_TABLE_BACKFILL_PARALLELISM = 2, "
                    + "ENABLE_SLIDE_WINDOW_BACKFILL = true, SLIDE_WINDOW_SPLIT_SIZE = 2, SLIDE_WINDOW_TIME_INTERVAL = 1000, PHYSICAL_BACKFILL_ENABLE=false)*/";
        } else if (usePhysicalBackfill) {
            sqlHint =
                "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=true, PHYSICAL_BACKFILL_SPEED_TEST=false)*/";
        }
        String ignoreErr = "The DDL job has been cancelled or interrupted";
        Set<String> ignoreErrs = new HashSet<>();
        ignoreErrs.add(ignoreErr);
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sqlHint + command,
            ignoreErrs);
    }

    static public void executeDml(String sql, Connection connection) {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdate(connection, sqlWithHint);
    }

    static public void executeDml(String sql, Connection connection, Set<String> errIgnored) {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdateSuccessIgnoreErr(connection, sqlWithHint, errIgnored);
    }

    public void executeDmlSuccess(String sql) {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlWithHint);
    }

    protected void createTable(String logicalTableName, String partitionRule, boolean replaceAutoIncrement) {
        String createTableSql = ExecuteTableSelect.getFullTypeTableDef(logicalTableName, partitionRule);
        if (replaceAutoIncrement) {
            createTableSql = createTableSql.replace("AUTO_INCREMENT=1", "AUTO_INCREMENT=100000");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        String alterTableSetTg = "alter table `" + logicalTableName + "` set tablegroup=" + tableGroupName + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);
    }

    //for PARTITION_BY_BIGINT_KEY
    static public boolean initData1(String tableName, int rowCount, Connection connection) {
        IntStream.range(0, rowCount).forEach(
            i -> executeDml(String.format("insert into `%s`(id) values(null);",
                tableName), connection));
        return true;
    }

    //for PARTITION_BY_INT_KEY
    static public boolean initData2(String tableName, int rowCount, Connection connection) {
        IntStream.range(0, rowCount).forEach(
            i -> executeDml(String.format("insert into `%s`(c_int_32) values(%d);",
                tableName, i), connection));
        return true;
    }

    //for PARTITION_BY_MONTH_HASH
    static public boolean initData3(String tableName, int rowCount, Connection connection) {
        DateTime dateTime = new DateTime("2021-12-15T00:00:01Z");
        IntStream.range(0, rowCount).forEach(
            i -> executeDml(String.format("insert into `%s`(c_datetime) values('%s');",
                tableName, dateTime.minusHours(24 * 3 * i).toString("yyyy-MM-dd HH:mm:ss")), connection));
        return true;
    }

    // for PARTITION_BY_INT_BIGINT_LIST
    static public boolean initData4(String tableName, int rowCount, Connection connection) {
        IntStream.range(0, rowCount).forEach(
            i -> executeDml(String.format("insert into `%s`(c_int_32) values(%d);",
                tableName, 1), connection));
        return true;
    }

    static public boolean initData5(String tableName, int rowCount, Connection connection) {
        IntStream.range(1, rowCount + 1).forEach(i -> executeDml(
            String.format("insert into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(null,%d,%s,%s,%s);",
                tableName, i * 10 + 1, "'abc" + i + "'", "'def" + i + "'",
                i == 3 ? "date_add('2008-12-01 00:00:00', interval " + i + " year)" :
                    "date_add('2010-01-01 00:00:00', interval " + i + " year)")
            , connection));
        return true;
    }

    static public boolean initData9(String tableName, int rowCount, Connection connection) {
        /*
        *
        * partition by range (year(c_datetime))
subpartition by list (c_int_32)
(
partition p1 values less than (2001)
(
subpartition p1sp1 values in (11, 12, 13)
),
partition p2 values less than (2012)
(
subpartition p2sp1 values in (11, 12, 13),
subpartition p2sp2 values in (21, 22, 23)
),
partition p3 values less than (2023)
(
subpartition p3sp1 values in (11, 12, 13),
subpartition p3sp2 values in (21, 22, 23),
subpartition p3sp3 values in (31, 32, 33)
))
        * */
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1980, 2023);
            int c_int_32;
            if (year < 2001) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2012) {
                if (RandomUtils.nextBoolean()) {
                    c_int_32 = RandomUtils.nextInt(11, 14);
                } else {
                    c_int_32 = RandomUtils.nextInt(21, 24);
                }
            } else {
                if (RandomUtils.nextBoolean()) {
                    c_int_32 = RandomUtils.nextInt(11, 14);
                } else if (RandomUtils.nextBoolean()) {
                    c_int_32 = RandomUtils.nextInt(21, 24);
                } else {
                    c_int_32 = RandomUtils.nextInt(31, 34);
                }
            }
            executeDml(
                String.format("insert into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(null,%d,%s,%s,%s);",
                    tableName, c_int_32, "'abc" + i + "'", "'def" + i + "'", "'" + year + "-12-01 00:00:00'")
                , connection);
        });
        return true;
    }

    static public boolean initData24(String tableName, int rowCount, Connection connection) {
        /*
        *
        * partition by list columns (id,c_varchar)
subpartition by list (c_int_32)
(
partition p1 values in ((10,'abc1'), (11,'abc1'), (12,'abc1'))
(
subpartition p1sp1 values in (11, 12, 13)
),
partition p2 values in ((20,'abc2'), (21,'abc2'), (22,'abc2'))
(
subpartition p2sp1 values in (11, 12, 13),
subpartition p2sp2 values in (21, 22, 23)
),
partition p3 values in ((30,'abc3'), (31,'abc3'), (32,'abc3'))
(
subpartition p3sp1 values in (11, 12, 13),
subpartition p3sp2 values in (21, 22, 23),
subpartition p3sp3 values in (31, 32, 33)
))
        * */
        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
            + "(10, 11,'abc1', '10' , '2020-01-01 12-11-12'),"
            + "(11, 12,'abc1', '11' , '2020-01-01 12-11-12'),"
            + "(12, 13,'abc1', '12' , '2020-01-01 12-11-12'),"
            + "(20, 11,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(21, 21,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(22, 23,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(30, 31,'abc3', '10' , '2020-01-01 12-11-12'),"
            + "(31, 21,'abc3', '10' , '2020-01-01 12-11-12'),"
            + "(32, 13,'abc3', '10' , '2020-01-01 12-11-12')", connection);
        return true;
    }

    static public boolean initData12(String tableName, int rowCount, Connection connection) {
        /*
        *
        * partition by range columns (c_datetime,c_int_32)
subpartition by range (year(c_datetime))
(
partition p1 values less than ('2001-01-01 00:00:00',11)
(
subpartition p1sp1 values less than (2001)
),
partition p2 values less than ('2012-01-01 00:00:00',21)
(
subpartition p2sp1 values less than (2001),
subpartition p2sp2 values less than (2012)
),
partition p3 values less than ('2023-01-01 00:00:00',31)
(
subpartition p3sp1 values less than (2001),
subpartition p3sp2 values less than (2012),
subpartition p3sp3 values less than (2023)
))
        * */
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1980, 2024);
            int c_int_32;
            if (year < 2023) {
                c_int_32 = RandomUtils.nextInt(0, 100);
            } else {
                c_int_32 = RandomUtils.nextInt(0, 31);
            }
            executeDml(
                String.format("insert into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(null,%d,%s,%s,%s);",
                    tableName, c_int_32, "'abc" + i + "'", "'def" + i + "'", "'" + year + "-12-01 00:00:00'")
                , connection);
        });
        return true;
    }

    static public boolean initData16(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
 partition by list (id)
subpartition by key (c_int_32)
(
partition p1 values in (10, 11, 12) subpartitions 1,
partition p2 values in (20, 21, 22) subpartitions 2,
partition p3 values in (30, 31, 32) subpartitions 3
)
        * */
        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
                + "(10, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(11, 12,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(12, 13,'def1', '12' , '2020-01-01 12-11-12'),"

                + "(20, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(21, 12,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(22, 13,'def1', '12' , '2020-01-01 12-11-12'),"

                + "(30, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(31, 12,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(32, 13,'def1', '12' , '2020-01-01 12-11-12')"

            , connection);
        return true;
    }

    static public boolean initData8(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range (year(c_datetime))
subpartition by range columns (id,c_datetime)
(
partition p1 values less than (2001)
(
subpartition p1sp1 values less than (10,'2011-01-01 00:00:00')
),
partition p2 values less than (2012)
(
subpartition p2sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p2sp2 values less than (20,'2012-01-01 00:00:00')
),
partition p3 values less than (2023)
(
subpartition p3sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p3sp2 values less than (20,'2012-01-01 00:00:00'),
subpartition p3sp3 values less than (30,'2013-01-01 00:00:00')
))
        * */
        Set<Integer> set = new HashSet<>();
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1980, 2023);
            int id;
            if (year < 2001) {
                id = RandomUtils.nextInt(0, 10);
            } else if (year < 2012) {
                id = RandomUtils.nextInt(10, 20);
            } else {
                id = RandomUtils.nextInt(20, 30);
            }
            if (!set.contains(id)) {
                executeDml(
                    String.format(
                        "insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                        tableName, id, id, "'abc" + i + "'", "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                    , connection);
                set.add(id);
            }
        });
        return true;
    }

    static public boolean initData19(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list (id)
subpartition by list (c_int_32)
(
partition p1 values in (10, 11, 12)
(
subpartition p1sp1 values in (11, 12, 13)
),
partition p2 values in (20, 21, 22)
(
subpartition p2sp1 values in (11, 12, 13),
subpartition p2sp2 values in (21, 22, 23)
),
partition p3 values in (30, 31, 32)
(
subpartition p3sp1 values in (11, 12, 13),
subpartition p3sp2 values in (21, 22, 23),
subpartition p3sp3 values in (31, 32, 33)
))
        * */

        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
                + "(10, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(11, 12,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(12, 13,'def1', '12' , '2020-01-01 12-11-12'),"

                + "(20, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(21, 22,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(22, 13,'def1', '12' , '2020-01-01 12-11-12'),"

                + "(30, 11,'def1', '10' , '2020-01-01 12-11-12'),"
                + "(31, 22,'def1', '11' , '2020-01-01 12-11-12'),"
                + "(32, 33,'def1', '12' , '2020-01-01 12-11-12')"
            , connection);
        return true;
    }

    static public boolean initData20(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list (id)
subpartition by list columns (c_int_32,c_char)
(
partition p1 values in (11, 12, 13)
(
subpartition p1sp1 values in ((11,'def1'), (12,'def1'), (13,'def1'))
),
partition p2 values in (21, 22, 23)
(
subpartition p2sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p2sp2 values in ((21,'def2'), (22,'def2'), (23,'def2'))
),
partition p3 values in (31, 32, 33)
(
subpartition p3sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p3sp2 values in ((21,'def2'), (22,'def2'), (23,'def2')),
subpartition p3sp3 values in ((31,'def3'), (32,'def3'), (33,'def3'))
))
        * */
        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
                + "(11, 11,'def1', 'def1' , '2020-01-01 12-11-12'),"
                + "(12, 12,'def1', 'def1' , '2020-01-01 12-11-12'),"
                + "(13, 13,'def1', 'def1' , '2020-01-01 12-11-12'),"

                + "(21, 21,'def2', 'def2' , '2020-01-01 12-11-12'),"
                + "(22, 11,'def1', 'def1' , '2020-01-01 12-11-12'),"
                + "(23, 23,'def2', 'def2' , '2020-01-01 12-11-12'),"

                + "(31, 11,'def1', 'def1' , '2020-01-01 12-11-12'),"
                + "(32, 22,'def2', 'def2' , '2020-01-01 12-11-12'),"
                + "(33, 33,'def3', 'def3' , '2020-01-01 12-11-12')"

            , connection);
        return true;
    }

    static public boolean initData23(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list columns (id,c_varchar)
subpartition by range columns (id,c_datetime)
(
partition p1 values in ((10,'abc1'), (11,'abc1'), (12,'abc1'))
(
subpartition p1sp1 values less than (10,'2011-01-01 00:00:00')
),
partition p2 values in ((20,'abc2'), (21,'abc2'), (22,'abc2'))
(
subpartition p2sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p2sp2 values less than (20,'2012-01-01 00:00:00')
),
partition p3 values in ((30,'abc3'), (31,'abc3'), (32,'abc3'))
(
subpartition p3sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p3sp2 values less than (20,'2012-01-01 00:00:00'),
subpartition p3sp3 values less than (30,'2013-01-01 00:00:00')
))
        * */
        //pass

        return true;
    }

    static public boolean initData14(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range columns (c_datetime,c_int_32)
subpartition by list (c_int_32)
(
partition p1 values less than ('2001-01-01 00:00:00',11)
(
subpartition p1sp1 values in (11, 12, 13)
),
partition p2 values less than ('2012-01-01 00:00:00',21)
(
subpartition p2sp1 values in (11, 12, 13),
subpartition p2sp2 values in (21, 22, 23)
),
partition p3 values less than ('2023-01-01 00:00:00',31)
(
subpartition p3sp1 values in (11, 12, 13),
subpartition p3sp2 values in (21, 22, 23),
subpartition p3sp3 values in (31, 32, 33)
))
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2024);
            int c_int_32;
            if (year < 2002) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2013) {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
            } else {
                if (year == 2023) {
                    c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
                } else {
                    c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) :
                        (RandomUtils.nextBoolean() ? RandomUtils.nextInt(21, 24) : RandomUtils.nextInt(31, 34));
                }
            }
            executeDml(
                String.format("insert into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(null,%d,%s,%s,%s);",
                    tableName, c_int_32, "'abc1'", "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    static public boolean initData17(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list (id)
subpartition by range (year(c_datetime))
(
partition p1 values in (10, 11, 12)
(
subpartition p1sp1 values less than (2001)
),
partition p2 values in (20, 21, 22)
(
subpartition p2sp1 values less than (2001),
subpartition p2sp2 values less than (2012)
),
partition p3 values in (30, 31, 32)
(
subpartition p3sp1 values less than (2001),
subpartition p3sp2 values less than (2012),
subpartition p3sp3 values less than (2023)
))
        * */
        Set<Integer> set = new HashSet<>();
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int id = RandomUtils.nextInt(1, 4);
            id = id * 10 + RandomUtils.nextInt(0, 3);
            String c_varchar = "'abc" + id / 10 + "'";
            int year;
            if (id < 20) {
                year = RandomUtils.nextInt(1900, 2001);
            } else if (id < 30) {
                year = RandomUtils.nextInt(1900, 2012);
            } else {
                year = RandomUtils.nextInt(1900, 2023);
            }
            if (!set.contains(id)) {
                executeDml(
                    String.format(
                        "insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                        tableName, id, id, c_varchar, "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                    , connection);
                set.add(id);
            }
        });

        return true;
    }

    static public boolean initData18(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list (id)
subpartition by range columns (id,c_datetime)
(
partition p1 values in (10, 11, 12)
(
subpartition p1sp1 values less than (10,'2011-01-01 00:00:00')
),
partition p2 values in (20, 21, 22)
(
subpartition p2sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p2sp2 values less than (20,'2012-01-01 00:00:00')
),
partition p3 values in (30, 31, 32)
(
subpartition p3sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p3sp2 values less than (20,'2012-01-01 00:00:00'),
subpartition p3sp3 values less than (30,'2013-01-01 00:00:00')
))
        * */

        //pass

        return true;
    }

    static public boolean initData22(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list columns (id,c_varchar)
subpartition by range (year(c_datetime))
(
partition p1 values in ((10,'abc1'), (11,'abc1'), (12,'abc1'))
(
subpartition p1sp1 values less than (2001)
),
partition p2 values in ((20,'abc2'), (21,'abc2'), (22,'abc2'))
(
subpartition p2sp1 values less than (2001),
subpartition p2sp2 values less than (2012)
),
partition p3 values in ((30,'abc3'), (31,'abc3'), (32,'abc3'))
(
subpartition p3sp1 values less than (2001),
subpartition p3sp2 values less than (2012),
subpartition p3sp3 values less than (2023)
))
        * */
        Set<Integer> set = new HashSet<>();
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int id = RandomUtils.nextInt(1, 4);
            id = id * 10 + RandomUtils.nextInt(0, 3);
            String c_varchar = "'abc" + id / 10 + "'";
            int year;
            if (id < 20) {
                year = RandomUtils.nextInt(1900, 2001);
            } else if (id < 30) {
                year = RandomUtils.nextInt(1900, 2012);
            } else {
                year = RandomUtils.nextInt(1900, 2023);
            }
            if (!set.contains(id)) {
                executeDml(
                    String.format(
                        "insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                        tableName, id, id, c_varchar, "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                    , connection);
                set.add(id);
            }
        });

        return true;
    }

    static public boolean initData13(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range columns (c_datetime,c_int_32)
subpartition by range columns (id,c_datetime)
(
partition p1 values less than ('2001-01-01 00:00:00',11)
(
subpartition p1sp1 values less than (10,'2011-01-01 00:00:00')
),
partition p2 values less than ('2012-01-01 00:00:00',21)
(
subpartition p2sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p2sp2 values less than (20,'2012-01-01 00:00:00')
),
partition p3 values less than ('2023-01-01 00:00:00',31)
(
subpartition p3sp1 values less than (10,'2011-01-01 00:00:00'),
subpartition p3sp2 values less than (20,'2012-01-01 00:00:00'),
subpartition p3sp3 values less than (31,'2013-01-01 00:00:00')
))
        * */
        Set<Integer> set = new HashSet<>();
        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32 = RandomUtils.nextInt(1, 1000);
            int id = 0;
            if (year < 2001) {
                id = RandomUtils.nextInt(1, 10);
            } else if (year < 2012) {
                id = RandomUtils.nextInt(1, 20);
            } else {
                id = RandomUtils.nextInt(1, 31);
            }
            if (!set.contains(id)) {
                executeDml(
                    String.format(
                        "insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                        tableName, id, c_int_32, "'abc1'", "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                    , connection);
                set.add(id);
            }
        });

        return true;
    }

    static public boolean initData21(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list columns (id,c_varchar)
subpartition by key (c_int_32)
(
partition p1 values in ((10,'abc1'), (11,'abc1'), (12,'abc1')) subpartitions 1,
partition p2 values in ((20,'abc2'), (21,'abc2'), (22,'abc2')) subpartitions 2,
partition p3 values in ((30,'abc3'), (31,'abc3'), (32,'abc3')) subpartitions 3
)
        * */

        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
            + "(10, 11,'abc1', '10' , '2020-01-01 12-11-12'),"
            + "(11, 12,'abc1', '11' , '2020-01-01 12-11-12'),"
            + "(12, 13,'abc1', '12' , '2020-01-01 12-11-12'),"
            + "(20, 11,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(21, 21,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(22, 23,'abc2', '10' , '2020-01-01 12-11-12'),"
            + "(30, 31,'abc3', '10' , '2020-01-01 12-11-12'),"
            + "(31, 21,'abc3', '10' , '2020-01-01 12-11-12'),"
            + "(32, 13,'abc3', '10' , '2020-01-01 12-11-12')", connection);

        return true;
    }

    static public boolean initData25(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by list columns (id,c_varchar)
subpartition by list columns (c_int_32,c_char)
(
partition p1 values in ((11,'abc1'), (12,'abc1'), (13,'abc1'))
(
subpartition p1sp1 values in ((11,'def1'), (12,'def1'), (13,'def1'))
),
partition p2 values in ((21,'abc2'), (22,'abc2'), (23,'abc2'))
(
subpartition p2sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p2sp2 values in ((21,'def2'), (22,'def2'), (23,'def2'))
),
partition p3 values in ((31,'abc3'), (32,'abc3'), (33,'abc3'))
(
subpartition p3sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p3sp2 values in ((21,'def2'), (22,'def2'), (23,'def2')),
subpartition p3sp3 values in ((31,'def3'), (32,'def3'), (33,'def3'))
))
        * */

        executeDml("insert ignore into " + tableName + "(id,c_int_32,c_varchar,c_char,c_datetime) values"
            + "(11, 12,'abc1', 'def1' , '2020-01-01 12-11-12'),"
            + "(12, 13,'abc1', 'def1' , '2020-01-01 12-11-12'),"
            + "(13, 11,'abc1', 'def1' , '2020-01-01 12-11-12'),"
            + "(21, 12,'abc2', 'def1' , '2020-01-01 12-11-12'),"
            + "(22, 23,'abc2', 'def2' , '2020-01-01 12-11-12'),"
            + "(23, 11,'abc2', 'def1' , '2020-01-01 12-11-12'),"
            + "(31, 12,'abc3', 'def1' , '2020-01-01 12-11-12'),"
            + "(32, 23,'abc3', 'def2' , '2020-01-01 12-11-12'),"
            + "(33, 31,'abc3', 'def3' , '2020-01-01 12-11-12')", connection);

        return true;
    }

    static public boolean initData7(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range (year(c_datetime))
subpartition by range (year(c_datetime))
(
partition p1 values less than (2001)
(
subpartition p1sp1 values less than (2001)
),
partition p2 values less than (2012)
(
subpartition p2sp1 values less than (2001),
subpartition p2sp2 values less than (2012)
),
partition p3 values less than (2023)
(
subpartition p3sp1 values less than (2001),
subpartition p3sp2 values less than (2012),
subpartition p3sp3 values less than (2023)
))
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32 = RandomUtils.nextInt(1, 1000);
            int id = 0;

            executeDml(
                String.format("insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                    tableName, id, c_int_32, "'abc1'", "'def" + i + "'", "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    static public boolean initData10(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range (year(c_datetime))
subpartition by list columns (c_int_32,c_char)
(
partition p1 values less than (2001)
(
subpartition p1sp1 values in ((11,'def1'), (12,'def1'), (13,'def1'))
),
partition p2 values less than (2012)
(
subpartition p2sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p2sp2 values in ((21,'def2'), (22,'def2'), (23,'def2'))
),
partition p3 values less than (2023)
(
subpartition p3sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p3sp2 values in ((21,'def2'), (22,'def2'), (23,'def2')),
subpartition p3sp3 values in ((31,'def3'), (32,'def3'), (33,'def3'))
))
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32;
            if (year < 2001) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2012) {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
            } else {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) :
                    RandomUtils.nextBoolean() ? RandomUtils.nextInt(21, 24) : RandomUtils.nextInt(31, 34);
            }
            String c_char = "'def" + c_int_32 / 10 + "'";
            int id = 0;

            executeDml(
                String.format("insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                    tableName, id, c_int_32, "'abc1'", c_char, "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    static public boolean initData15(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range columns (c_datetime,c_int_32)
subpartition by list columns (c_int_32,c_char)
(
partition p1 values less than ('2001-01-01 00:00:00',10)
(
subpartition p1sp1 values in ((11,'def1'), (12,'def1'), (13,'def1'))
),
partition p2 values less than ('2012-01-01 00:00:00',20)
(
subpartition p2sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p2sp2 values in ((21,'def2'), (22,'def2'), (23,'def2'))
),
partition p3 values less than ('2023-01-01 00:00:00',30)
(
subpartition p3sp1 values in ((11,'def1'), (12,'def1'), (13,'def1')),
subpartition p3sp2 values in ((21,'def2'), (22,'def2'), (23,'def2')),
subpartition p3sp3 values in ((31,'def3'), (32,'def3'), (33,'def3'))
))
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32;
            if (year < 2001) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2012) {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
            } else {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) :
                    RandomUtils.nextBoolean() ? RandomUtils.nextInt(21, 24) : RandomUtils.nextInt(31, 34);
            }
            String c_char = "'def" + c_int_32 / 10 + "'";
            int id = 0;

            executeDml(
                String.format("insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                    tableName, id, c_int_32, "'abc1'", c_char, "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    static public boolean initData11(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range columns (c_datetime,c_int_32)
subpartition by key (c_int_32)
(
partition p1 values less than ('2001-01-01 00:00:00',11) subpartitions 1,
partition p2 values less than ('2012-01-01 00:00:00',21) subpartitions 2,
partition p3 values less than ('2023-01-01 00:00:00',31) subpartitions 3
)
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32;
            if (year < 2001) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2012) {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
            } else {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) :
                    RandomUtils.nextBoolean() ? RandomUtils.nextInt(21, 24) : RandomUtils.nextInt(31, 34);
            }
            String c_char = "'def" + c_int_32 / 10 + "'";
            int id = 0;

            executeDml(
                String.format("insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                    tableName, id, c_int_32, "'abc1'", c_char, "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    static public boolean initData6(String tableName, int rowCount, Connection connection) {
        /*
        *
        *
partition by range (year(c_datetime))
subpartition by key (c_int_32)
(
partition p1 values less than (2001) subpartitions 1,
partition p2 values less than (2012) subpartitions 2,
partition p3 values less than (2023) subpartitions 3
)
        * */

        IntStream.range(1, rowCount + 1).forEach(i -> {
            int year = RandomUtils.nextInt(1990, 2023);
            int c_int_32;
            if (year < 2001) {
                c_int_32 = RandomUtils.nextInt(11, 14);
            } else if (year < 2012) {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) : RandomUtils.nextInt(21, 24);
            } else {
                c_int_32 = RandomUtils.nextBoolean() ? RandomUtils.nextInt(11, 14) :
                    RandomUtils.nextBoolean() ? RandomUtils.nextInt(21, 24) : RandomUtils.nextInt(31, 34);
            }
            String c_char = "'def" + c_int_32 / 10 + "'";
            int id = 0;

            executeDml(
                String.format("insert ignore into `%s`(id,c_int_32,c_varchar,c_char,c_datetime) values(%d,%d,%s,%s,%s);",
                    tableName, id, c_int_32, "'abc1'", c_char, "'" + year + "-01-01 00:00:00'")
                , connection);
        });

        return true;
    }

    // for PARTITION_BY_BIGINT_KEY
    static public boolean initDML1(String tableName, int rowCount, String targetPart, List<String> partCol,
                                   List<List<String>> vals, String logicalDatabase, Connection connection) {
        partCol.add("id");
        for (int i = 0; ; i++) {
            Integer val = new Integer(100001);
            val = val + 1000 + i;
            String part_route_sql =
                String.format("select part_route('%s', '%s',%d) as res", logicalDatabase, tableName, val);
            ResultSet rs = JdbcUtil.executeQuery(part_route_sql, connection);

            try {
                if (rs.next()) {
                    String partName = rs.getString(1);
                    if (targetPart.equalsIgnoreCase(partName)) {
                        List<String> partVal = new ArrayList<>(1);
                        partVal.add(val.toString());
                        rowCount--;
                        vals.add(partVal);
                        if (rowCount <= 0) {
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + part_route_sql;
                Assert.fail(errorMs + " \n" + ex);
            }
        }
        return true;
    }

    // for PARTITION_BY_INT_KEY
    static public boolean initDML2(String tableName, int rowCount, String targetPart, List<String> partCol,
                                   List<List<String>> vals, String logicalDatabase, Connection connection) {
        partCol.add("c_int_32");
        for (int i = 0; ; i++) {
            Integer val = new Integer(1000);
            val = val + i;
            String part_route_sql =
                String.format("select part_route('%s', '%s',%d) as res", logicalDatabase, tableName, val);
            ResultSet rs = JdbcUtil.executeQuery(part_route_sql, connection);

            try {
                if (rs.next()) {
                    String partName = rs.getString(1);
                    if (targetPart.equalsIgnoreCase(partName)) {
                        List<String> partVal = new ArrayList<>(1);
                        partVal.add(val.toString());
                        rowCount--;
                        vals.add(partVal);
                        if (rowCount <= 0) {
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + part_route_sql;
                Assert.fail(errorMs + " \n" + ex);
            }
        }
        return true;
    }

    // for PARTITION_BY_INT_BIGINT_KEY/PARTITION_BY_INT_BIGINT_HASH
    static public boolean initDML3(String tableName, int rowCount, String targetPart, List<String> partCol,
                                   List<List<String>> vals, String logicalDatabase, Connection connection) {
        partCol.add("c_int_32");
        partCol.add("id");
        for (int i = 0; ; i++) {
            Integer val = new Integer(1000);
            val = val + i;
            String part_route_sql =
                String.format("select part_route('%s', '%s',%d, %d) as res", logicalDatabase, tableName, val, val);
            ResultSet rs = JdbcUtil.executeQuery(part_route_sql, connection);

            try {
                if (rs.next()) {
                    String partName = rs.getString(1);
                    if (targetPart.equalsIgnoreCase(partName)) {
                        List<String> partVal = new ArrayList<>(1);
                        partVal.add(val.toString());
                        partVal.add(val.toString());
                        vals.add(partVal);
                        rowCount--;
                        if (rowCount <= 0) {
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + part_route_sql;
                Assert.fail(errorMs + " \n" + ex);
            }
        }
        return true;
    }

    // for PARTITION_BY_MONTH_HASH
    static public boolean initDML4(String tableName, int rowCount, String targetPart, List<String> partCol,
                                   List<List<String>> vals, String logicalDatabase, Connection connection) {
        partCol.add("c_datetime");
        for (int i = 0; ; i++) {
            DateTime dateTime = new DateTime("2021-12-20T10:00:01Z");
            String val = dateTime.minusHours(24 * 30 * i).toString("yyyy-MM-dd HH:mm:ss");

            String part_route_sql =
                String.format("select part_route('%s', '%s','%s') as res", logicalDatabase, tableName, val);
            ResultSet rs = JdbcUtil.executeQuery(part_route_sql, connection);

            try {
                if (rs.next()) {
                    String partName = rs.getString(1);
                    if (targetPart.equalsIgnoreCase(partName)) {
                        List<String> partVal = new ArrayList<>(1);
                        partVal.add(val);
                        vals.add(partVal);
                        rowCount--;
                        if (rowCount <= 0) {
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + part_route_sql;
                Assert.fail(errorMs + " \n" + ex);
            }
        }
        return true;
    }

    // for PARTITION_BY_BIGINT_RANGE
    static public boolean initDML5(int rowCount, Integer minVal, Integer maxVal,
                                   List<String> partCol,
                                   List<List<String>> vals) {
        partCol.add("id");
        int min = minVal.intValue();
        int max = maxVal.intValue();
        for (int i = 0; i < rowCount; i = i + 2) {
            Integer val = new Integer(min);
            val = val + i;
            List<String> partVal = new ArrayList<>(1);
            partVal.add(val.toString());
            vals.add(partVal);
            val = new Integer(max);
            val = val - i - 1;
            partVal = new ArrayList<>(1);
            partVal.add(val.toString());
            vals.add(partVal);
            min++;
            max--;
            if (min >= max) {
                break;
            }
        }
        return true;
    }

    //for PARTITION_BY_INT_BIGINT_RANGE_COL/PARTITION_BY_INT_BIGINT_LIST
    static public boolean initDML6(int rowCount, Integer minVal1, Integer maxVal1,
                                   Integer minVal2, Integer maxVal2,
                                   List<String> partCol,
                                   List<List<String>> vals) {
        partCol.add("c_int_32");
        partCol.add("id");
        for (int i = 0; i < rowCount; i++) {
            Integer val1 = new Integer(minVal1);
            val1 = Math.max((val1 + i) % maxVal1, minVal1);
            Integer val2 = new Integer(minVal2);
            val2 = Math.max((val2 + i) % maxVal2, minVal2);
            if (i == rowCount - 1) {
                val1 = Math.max(maxVal1 - 1, minVal1);
                val2 = Math.max(maxVal2 - 1, minVal2);
            }
            List<String> partVal = new ArrayList<>(2);
            partVal.add(val1.toString());
            partVal.add(val2.toString());
            vals.add(partVal);
        }
        return true;
    }

    //for PARTITION_BY_BIGINT_LIST
    static public boolean initDML7(int rowCount, Integer minVal1, Integer maxVal1,
                                   List<String> partCol,
                                   List<List<String>> vals) {
        partCol.add("id");
        for (int i = 0; i < rowCount; i++) {
            Integer val1 = new Integer(minVal1);
            val1 = Math.max((val1 + i) % maxVal1, minVal1);
            if (i == rowCount - 1) {
                val1 = maxVal1 - 1;
            }
            List<String> partVal = new ArrayList<>(1);
            partVal.add(val1.toString());
            vals.add(partVal);
        }

        return true;
    }

    protected int getTraceCount(List<List<String>> trace, String type) {
        int count = 0;
        for (List<String> sqlTrace : trace) {
            if (sqlTrace.get(11).toLowerCase().contains(type)
                && !sqlTrace.get(11).toLowerCase().contains("for update")) {
                if (type.equalsIgnoreCase("update") && sqlTrace.get(12).contains("}")) {
                    count += Arrays.stream(sqlTrace.get(12).split("}")).count() - 1;
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    protected Map<String, String> getPartInstIdMap(String logicalTable) {
        Map<String, String> partGroupMap = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, String> groupInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, String> partInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show topology from %s", logicalTable);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            while (rs.next()) {
                String partName = rs.getString("PARTITION_NAME");
                String groupName = rs.getString("GROUP_NAME");
                partGroupMap.put(partName, groupName);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        sql = String.format("show ds where db='%s'", logicalDatabase);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            while (rs.next()) {
                String storageInstId = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                groupInstIdMap.put(groupName, storageInstId);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        for (Map.Entry<String, String> entry : partGroupMap.entrySet()) {
            partInstIdMap.put(entry.getKey(), groupInstIdMap.get(entry.getValue()));
        }
        return partInstIdMap;
    }

    protected List<String> getStorageInstIds() {
        return getStorageInstIds(logicalDatabase);
    }

    private static class DMLRunner implements Runnable {

        private final AtomicBoolean stop;
        private final int val1Min;
        private final int val1Max;
        private final int val2Min;
        private final int val2Max;
        private final String tableName;
        private String insertPatterm =
            "trace insert ignore into `%s`(id,c_int_32,c_datetime, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) values(%s,%s,'%s', now(),now(),now(),now())";
        private String updatePatterm = "update %s set c_int_32=%s where id=%s";
        private String deletePatterm = "delete from  %s where id=%s";

        private final Consumer<String> doDmlFunc;

        public DMLRunner(AtomicBoolean stop, int val1Min, int val1Max, int val2Min, int val2Max,
                         String tableName, Consumer<String> doDmlFunc) {
            this.stop = stop;
            this.doDmlFunc = doDmlFunc;
            this.val1Max = val1Max;
            this.val1Min = val1Min;
            this.val2Min = val2Min;
            this.val2Max = val2Max;
            this.tableName = tableName;
        }

        @Override
        public void run() {
            try {
                int count = 0;
                do {
                    String insert = prepareDML(0);
                    final String delete = prepareDML(1);
                    final String update = prepareDML(2);

                    doDmlFunc.accept(insert);
                    doDmlFunc.accept(update);
                    doDmlFunc.accept(delete);

                    insert = prepareDML(0);
                    doDmlFunc.accept(insert);
                    insert = prepareDML(0);
                    doDmlFunc.accept(insert);
                    count++;
                } while (!stop.get());

                System.out.println(Thread.currentThread().getName() + " quit after " + count + " round");
            } catch (Exception e) {
                throw new RuntimeException("DML failed!", e);
            }

        }

        private String prepareDML(int type) {
            String id_val;
            String c_int_32_val;
            String c_datetime_val;
            String statement = StringUtils.EMPTY;
            switch (type) {
            case 0:
                ///insert
                if (val1Max > 0) {
                    int v1 = ThreadLocalRandom.current().nextInt(val1Max);
                    v1 = Math.max(val1Min, v1);
                    id_val = String.valueOf(v1);
                } else {
                    id_val = "null";
                }
                if (val2Max > 0) {
                    int v2 = ThreadLocalRandom.current().nextInt(val2Max);
                    v2 = Math.max(val2Min, v2);
                    c_int_32_val = String.valueOf(v2);
                } else {
                    int v2 = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                    c_int_32_val = String.valueOf(v2);
                }
                DateTime dateTime = new DateTime("2021-12-15T00:00:01Z");
                int nextInt = ThreadLocalRandom.current().nextInt(366);
                c_datetime_val = dateTime.plusDays(nextInt).toString("yyyy-MM-dd HH:mm:ss");
                statement = String
                    .format(insertPatterm, tableName,
                        id_val, c_int_32_val, c_datetime_val);
                break;
            case 1:
                ///delete
                if (val1Max > 0) {
                    int v1 = ThreadLocalRandom.current().nextInt(val1Max);
                    v1 = Math.max(val1Min, v1);
                    id_val = String.valueOf(v1);
                } else {
                    int v1 = ThreadLocalRandom.current().nextInt(100001, 100001 + 400);
                    id_val = String.valueOf(v1);
                }
                statement = String
                    .format(deletePatterm, tableName, id_val);
                break;
            case 2:
                ///update
                if (val1Max > 0) {
                    int v1 = ThreadLocalRandom.current().nextInt(val1Max);
                    v1 = Math.max(val1Min, v1);
                    id_val = String.valueOf(v1);
                } else {
                    int v1 = ThreadLocalRandom.current().nextInt(300001, 400001);
                    id_val = String.valueOf(v1);
                }
                if (val2Max > 0) {
                    int v2 = ThreadLocalRandom.current().nextInt(val2Max);
                    v2 = Math.max(val2Min, v2);
                    c_int_32_val = String.valueOf(v2);
                } else {
                    int v2 = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                    c_int_32_val = String.valueOf(v2);
                }
                statement = String
                    .format(updatePatterm, tableName, c_int_32_val, id_val);
                break;
            }
            return statement;
        }

    }

    public void dmlWhilePartitionReorg(PartitionRuleInfo partitionRuleInfo, String command, String tableName) {
        final String hintStr = "";
        AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> dmlTasks = new ArrayList<>();
        final Integer max1, min1, max2, min2;
        if (partitionRuleInfo.partCol.size() > 1) {
            max1 = partitionRuleInfo.maxVal2;
            min1 = partitionRuleInfo.minVal2;
            max2 = partitionRuleInfo.maxVal1;
            min2 = partitionRuleInfo.minVal1;
        } else {
            max1 = partitionRuleInfo.maxVal1;
            min1 = partitionRuleInfo.minVal1;
            max2 = partitionRuleInfo.maxVal2;
            min2 = partitionRuleInfo.minVal2;
        }
        ExecutorService dmlPool = Executors.newFixedThreadPool(parallel);
        try {
            Set<String> ignoreError = new HashSet<>();
            ignoreError.add("Duplicate entry");
            IntStream.range(0, parallel).forEach(i -> dmlTasks.add(dmlPool
                .submit(new DMLRunner(stop, min1, max1, min2,
                    max2, tableName, (sql) -> {
                    executeDml(hintStr + sql, partitionRuleInfo.connection, ignoreError);
                }))));

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // ignore exception
            }
            executePartReorg(partitionRuleInfo.tableStatus, partitionRuleInfo.physicalTableBackfillParallel,
                partitionRuleInfo.usePhysicalTableBackfill, command);
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                // ignore exception
            }
            stop.set(true);
            for (Future future : dmlTasks) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            dmlPool.shutdown();
        }
    }

    static class PartitionRuleInfo {
        final PartitionStrategy strategy;
        final String partitionRule;
        String alterTableGroupCommand;
        ComplexTaskMetaManager.ComplexTaskStatus tableStatus;
        boolean physicalTableBackfillParallel = false;
        boolean usePhysicalTableBackfill = false;
        boolean needGenDml;
        int dmlType;
        List<String> partCol = new ArrayList<>();
        List<List<String>> partVals = new ArrayList<>();
        int rowCount;
        String targetPart;
        Integer minVal1 = 0;
        Integer maxVal1 = 0;
        Integer minVal2 = 0;
        Integer maxVal2 = 0;
        boolean pkIsPartCol;
        public Connection connection;
        int initDataType;
        boolean ignoreInit;

        List<String> logicalTableNames = new ArrayList<String>() {{
            add("t1");
            add("t2");
            add("t3");
            add("t4");
            add("t5-1");
            add("!``$%&()*+,-./:;<=>?@[]^_{|}~``");
            add("圣人立象以尽义(＾-＾)ノ聖を立てて義を尽くす");
            add("pQ9mV8sG7zF6xH5rI4uJ3bK2yL1oN0wMcXaWdVeRf2yL1oN0wMcXaWdVeRfhulux");
        }};

        public PartitionRuleInfo(PartitionStrategy partitionStrategy,
                                 int initDataType,
                                 String partitionRule,
                                 String alterTableGroupCommand) {
            this.strategy = partitionStrategy;
            this.initDataType = initDataType;
            this.partitionRule = partitionRule;
            this.alterTableGroupCommand = alterTableGroupCommand;
        }

        public PartitionRuleInfo(PartitionStrategy partitionStrategy,
                                 int initDataType,
                                 boolean ignoreInit,
                                 String partitionRule,
                                 String alterTableGroupCommand) {
            this(partitionStrategy, initDataType, partitionRule, alterTableGroupCommand);
            this.ignoreInit = ignoreInit;
        }

        public PartitionRuleInfo(PartitionStrategy partitionStrategy,
                                 int initDataType,
                                 boolean ignoreInit,
                                 String partitionRule,
                                 String alterTableGroupCommand,
                                 String targetPart) {
            this(partitionStrategy, initDataType, ignoreInit, partitionRule, alterTableGroupCommand);
            this.targetPart = targetPart;
        }

        public PartitionRuleInfo(PartitionStrategy partitionStrategy,
                                 int initDataType, String partitionRule,
                                 String alterTableGroupCommand, boolean needGenDml, int dmlType, int rowCount,
                                 String targetPart, Integer minVal1, Integer maxVal1,
                                 Integer minVal2, Integer maxVal2) {
            this.strategy = partitionStrategy;
            this.partitionRule = partitionRule;
            this.alterTableGroupCommand = alterTableGroupCommand;
            this.needGenDml = needGenDml;
            this.dmlType = dmlType;
            this.rowCount = rowCount;
            this.targetPart = targetPart;
            this.minVal1 = minVal1;
            this.maxVal1 = maxVal1;
            this.minVal2 = minVal2;
            this.maxVal2 = maxVal2;
            partCol.clear();
            partVals.clear();
            this.initDataType = initDataType;
        }

        public PartitionStrategy getStrategy() {
            return strategy;
        }

        public String getPartitionRule() {
            return partitionRule;
        }

        public String getAlterTableGroupCommand() {
            return alterTableGroupCommand;
        }

        public ComplexTaskMetaManager.ComplexTaskStatus getTableStatus() {
            return tableStatus;
        }

        public void setTableStatus(ComplexTaskMetaManager.ComplexTaskStatus tableStatus) {
            this.tableStatus = tableStatus;
        }

        public void setPhysicalTableBackfillParallel(Boolean physicalTableBackfillParallel) {
            this.physicalTableBackfillParallel = physicalTableBackfillParallel;
        }

        public void setUsePhysicalTableBackfill(boolean usePhysicalTableBackfill) {
            this.usePhysicalTableBackfill = usePhysicalTableBackfill;
        }

        public List<String> getLogicalTableNames() {
            return logicalTableNames;
        }

        public void setLogicalTableNames(List<String> logicalTableNames) {
            this.logicalTableNames = logicalTableNames;
        }

        public void prepareData(String tableName, Integer insertRow) {
            if (ignoreInit) {
                return;
            }

            try {
                String methodName = "initData" + initDataType;
                // 获取方法对象
                Method method =
                    AlterTableGroupTestBase.class.getMethod(methodName, String.class, int.class, Connection.class);
                // 调用方法
                method.invoke(null, tableName, insertRow, connection);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            if (needGenDml) {

            }
        }

        public boolean isNeedGenDml() {
            return needGenDml;
        }

        public int getDmlType() {
            return dmlType;
        }

        void prepareDml(String logicalDatabase) {
            partCol.clear();
            partVals.clear();
            switch (dmlType) {
            case 1:
                AlterTableGroupTestBase.initDML1("t1", rowCount, targetPart, partCol, partVals, logicalDatabase,
                    connection);
                break;
            case 2:
                AlterTableGroupTestBase.initDML2("t1", rowCount, targetPart, partCol, partVals, logicalDatabase,
                    connection);
                break;
            case 3:
                AlterTableGroupTestBase.initDML3("t1", rowCount, targetPart, partCol, partVals, logicalDatabase,
                    connection);
                break;
            case 4:
                AlterTableGroupTestBase.initDML4("t1", rowCount, targetPart, partCol, partVals, logicalDatabase,
                    connection);
                break;
            case 5:
                AlterTableGroupTestBase.initDML5(rowCount, minVal1, maxVal1, partCol, partVals);
                break;
            case 6:
                AlterTableGroupTestBase.initDML6(rowCount, minVal1, maxVal1, minVal2, maxVal2,
                    partCol,
                    partVals);
                break;
            case 7:
                AlterTableGroupTestBase.initDML7(rowCount, minVal1, maxVal1, partCol, partVals);
                break;
            default:
                assert false;
            }
            for (String col : partCol) {
                if (col.equalsIgnoreCase(pk)) {
                    pkIsPartCol = true;
                    break;
                }
            }
        }

        public List<String> getPartCol() {
            return partCol;
        }

        public List<List<String>> getPartVals() {
            return partVals;
        }

        public String getDeleteClause() {
            return getDeleteClause(partVals, true);
        }

        public String getDeleteClause(List<List<String>> rows, boolean deleteByPk) {
            StringBuilder sb = new StringBuilder();
            sb.append(" delete from t1 where ");
            int j = 0;
            int pkIndex = 0;
            if (isPkIsPartCol() && deleteByPk) {
                for (int i = 0; i < partCol.size(); i++) {
                    if (partCol.get(i).equalsIgnoreCase(pk)) {
                        pkIndex = i;
                        break;
                    }
                }
            }
            for (List<String> row : rows) {
                if (j > 0) {
                    sb.append(" or ");
                }
                sb.append("(");
                for (int i = 0; i < row.size(); i++) {
                    if (!(isPkIsPartCol() && deleteByPk)) {
                        if (i > 0) {
                            sb.append(" and ");
                        }
                    } else {
                        i = pkIndex;
                    }
                    sb.append(partCol.get(i));
                    sb.append("='");
                    sb.append(row.get(i));
                    sb.append("'");
                    if (!isPkIsPartCol()) {
                        break;
                    }
                }
                sb.append(")");
                j++;
            }
            return sb.toString();
        }

        public String getInsertClause(boolean isIgnore, boolean isReplace) {
            StringBuilder sb = new StringBuilder();
            List<Integer> pks;
            pks = null;
            if (!isReplace) {
                sb.append(" insert ");
                if (isIgnore) {
                    sb.append("ignore ");
                }
            } else {
                sb.append("replace ");
            }
            sb.append("into t1(");
            int i = 0;
            for (String col : partCol) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(col);
                i++;
            }
            if (isReplace && !isPkIsPartCol()) {
                sb.append(",");
                sb.append(pk);
                pks = getPkFromPart(targetPart, "");
            }
            sb.append(",");
            sb.append("c_timestamp) values");
            int j = 0;
            for (List<String> row : partVals) {
                if (j > 0) {
                    sb.append(",");
                }
                sb.append("(");
                int k = 0;
                for (String val : row) {
                    if (k > 0) {
                        sb.append(",");
                    }
                    sb.append("'");
                    sb.append(val);
                    sb.append("'");
                    k++;
                }
                if (isReplace && !isPkIsPartCol()) {
                    sb.append(",");
                    sb.append(pks.get(j));
                }
                sb.append(",now())");
                j++;
            }
            return sb.toString();
        }

        public String getInsertOnDuplicateKeyUpdate(List<String> row, List<String> updateRow) {
            StringBuilder sb = new StringBuilder();
            sb.append(" insert ");
            sb.append("into t1(");
            int i = 0;
            List<Integer> pks;
            pks = null;
            StringBuilder where = new StringBuilder();
            where.append("where ");
            for (String col : partCol) {
                if (i > 0) {
                    sb.append(",");
                    where.append(" and ");
                }
                sb.append(col);
                where.append(col);
                where.append("='");
                where.append(row.get(i));
                where.append("'");
                i++;
            }
            if (!isPkIsPartCol()) {
                sb.append(",");
                sb.append(pk);
                pks = getPkFromPart(targetPart, where.toString());
            }
            sb.append(",");
            sb.append("c_timestamp) values");
            int j = 0;
            if (j > 0) {
                sb.append(",");
            }
            sb.append("(");
            int k = 0;
            for (String val : row) {
                if (k > 0) {
                    sb.append(",");
                }
                sb.append("'");
                sb.append(val);
                sb.append("'");
                k++;
            }
            if (!isPkIsPartCol()) {
                sb.append(",");
                sb.append(pks.get(0));
            }
            sb.append(",now()) ");
            sb.append("on duplicate key update ");
            i = 0;
            for (String col : partCol) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(col);
                sb.append("=");
                sb.append("'");
                sb.append(updateRow.get(i));
                sb.append("'");
                i++;
            }

            return sb.toString();
        }

        public String getUpdateClause(List<String> row, List<String> updateRow) {
            StringBuilder sb = new StringBuilder();
            sb.append(" update t1 set ");
            for (int i = 0; i < updateRow.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(partCol.get(i));
                sb.append("='");
                sb.append(updateRow.get(i));
                sb.append("'");
            }
            sb.append(" where ");
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    sb.append(" and ");
                }
                sb.append(partCol.get(i));
                sb.append("='");
                sb.append(row.get(i));
                sb.append("'");
            }
            return sb.toString();
        }

        public String getUpdatePkClause(List<String> row) {
            int pkVal = getPkFromPartVal(row);
            StringBuilder sb = new StringBuilder();
            sb.append(" update t1 set ");
            sb.append(pk);
            sb.append("='");
            sb.append(pkVal + 500);
            sb.append("' where ");
            sb.append(pk);
            sb.append("='");
            sb.append(pkVal);
            sb.append("'");

            return sb.toString();
        }

        public List<Integer> getPkFromPart(String targetPart, String whereClause) {
            String sql = String.format("select id from t1 partition(%s) %s", targetPart, whereClause);
            if (partitionRule.equalsIgnoreCase(SINGLE_TABLE)) {
                sql = String.format("select id from t1 %s", whereClause);
            }

            List<Integer> res = new ArrayList<>();
            ResultSet rs = null;
            try {
                rs =
                    JdbcUtil.executeQuery(sql, connection);
                while (rs.next()) {
                    res.add(rs.getInt(1));
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
                Assert.fail(errorMs + " \n" + ex);
            }
            return res;
        }

        public Integer getPkFromPartVal(List<String> row) {

            StringBuilder where = new StringBuilder();
            where.append("where ");
            int i = 0;
            int pkVal = 0;
            for (String col : partCol) {
                if (i > 0) {
                    where.append(" and ");
                }
                where.append(col);
                where.append("='");
                where.append(row.get(i));
                where.append("'");
                i++;
            }

            String sql = String.format("select %s from t1 %s", pk, where.toString());
            try {
                ResultSet rs =
                    JdbcUtil.executeQuery(sql, connection);
                if (rs.next()) {
                    pkVal = rs.getInt(1);
                }

            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
                Assert.fail(errorMs + " \n" + ex);
            }
            return pkVal;
        }

        public List<List<String>> getPartValFromPart(String targetPart, String whereClause) {
            int i = 0;
            String cols = "";
            for (String col : partCol) {
                if (i > 0) {
                    cols = cols + ",";
                }
                cols = cols + col;
                i++;
            }
            String sql = String.format("select %s from t1 partition(%s) %s", cols, targetPart, whereClause);
            if (partitionRule.equalsIgnoreCase(SINGLE_TABLE)) {
                sql = String.format("select %s from t1 %s", cols, whereClause);
            }
            List<List<String>> res = new ArrayList<>();
            ResultSet rs = null;
            try {
                rs =
                    JdbcUtil.executeQuery(sql, connection);
                while (rs.next()) {
                    List<String> row = new ArrayList<>();

                    for (int j = 1; j <= partCol.size(); j++) {
                        row.add(rs.getString(j));
                    }
                    res.add(row);
                }
            } catch (Exception ex) {
                String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
                Assert.fail(errorMs + " \n" + ex);
            }
            return res;
        }

        public boolean isPkIsPartCol() {
            return pkIsPartCol;
        }

        @Override
        public String toString() {
            String partRule = partitionRule;
            if (partitionRule.length() > 50) {
                partRule = partitionRule.substring(0, 50) + "...";
            }
            return "PartitionRuleInfo{" +
                "strategy=" + strategy +
                ", tableStatus=" + tableStatus +
                ", partitionRule='" + partRule + '\'' +
                ", alterTableGroupCommand='" + alterTableGroupCommand + '\'' +
                ", logicalTableNames=" + logicalTableNames +
                '}';
        }
    }

    public boolean usingNewPartDb() {
        return true;
    }

}

