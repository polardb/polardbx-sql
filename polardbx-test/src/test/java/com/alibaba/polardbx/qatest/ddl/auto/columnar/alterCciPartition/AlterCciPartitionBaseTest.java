/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar.alterCciPartition;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Getter;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AlterCciPartitionBaseTest extends DDLBaseNewDBTestCase {

    protected String logicalDatabase;
    protected static List<String> finalTableStatus;
    protected static String originUseDbName;
    protected static String originSqlMode;
    protected static boolean needAutoDropDbAfterTest = true;
    protected static boolean printExecutedSqlLog = false;
    protected static String tableGroupName = "alter_cci_table_tg";
    protected static String tableName = "tT1";
    protected static String cciName = "cci_tT1";

    protected static String pk = "id";

    public static final String SKIP_REAL_ALTER_PARTITION_HINT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableAlterPartitionTask\")*/";

    public static final String partitionMode = "mode='auto'";

    public static final String createCci =
        "CREATE COLUMNAR CLUSTERED INDEX `%s` ON `%s`(`id`) %s";

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

    public AlterCciPartitionBaseTest(String logicalDatabase) {
        this.logicalDatabase = logicalDatabase;
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

    public void setUp(boolean recreateDB, PartitionRuleInfo partitionRuleInfo, boolean isTruncateOrDrop) {
        if (!usingNewPartDb()) {
            return;
        }
        partitionRuleInfo.connection = getTddlConnection1();
        prepareDdlAndData(recreateDB, partitionRuleInfo);
        executePartReorg(partitionRuleInfo.alterCommand, isTruncateOrDrop);
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

    protected void createTable(String logicalTableName, String partitionRule) {
        String createTableSql = ExecuteTableSelect.getFullTypeTableDef(logicalTableName, partitionRule);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            createTableSql.replace("AUTO_INCREMENT=1", "AUTO_INCREMENT=100000"));
        String alterTableSetTg = "alter table " + logicalTableName + " set tablegroup=" + tableGroupName + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);
    }

    protected void createCci(String logicalTableName, String cciName, String partitionRule) {
        String createCciSql =
            SKIP_WAIT_CCI_CREATION_HINT + String.format(createCci, cciName, logicalTableName, partitionRule);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createCciSql);
    }

    private void prepareDdlAndData(boolean recreateDB, PartitionRuleInfo partitionRuleInfo) {
        if (recreateDB) {
            reCreateDatabase(partitionRuleInfo.connection, this.logicalDatabase);
            for (String tableName : partitionRuleInfo.getLogicalTableNames()) {
                createTable(tableName, partitionRuleInfo.getPartitionRule());
                createCci(tableName, cciName, partitionRuleInfo.getPartitionRule());
            }
        }
    }

    private void executePartReorg(String command, boolean isDropOrTruncate) {
        String sqlHint = "/*+TDDL({'extra':{'FORBID_DDL_WITH_CCI':'FALSE'}})*/";
        if (isDropOrTruncate) {
            sqlHint =
                "/*+TDDL({'extra':{'ENABLE_DROP_TRUNCATE_CCI_PARTITION':'TRUE','FORBID_DDL_WITH_CCI':'FALSE'}})*/";
        }
        String ignoreErr =
            "The DDL job has been paused or cancelled. Please use SHOW DDL";
        Set<String> ignoreErrs = new HashSet<>();
        ignoreErrs.add(ignoreErr);
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sqlHint + command, ignoreErrs);
    }

    @Getter
    static class PartitionRuleInfo {
        final PartitionStrategy strategy;
        final String partitionRule;
        String alterCommand;
        ComplexTaskMetaManager.ComplexTaskStatus tableStatus;
        List<String> partCol = new ArrayList<>();
        List<List<String>> partVals = new ArrayList<>();
        public Connection connection;
        int initDataType;
        List<String> logicalTableNames = new ArrayList<String>() {{
            add("tT1");
        }};

        public PartitionRuleInfo(PartitionStrategy partitionStrategy,
                                 int initDataType, String partitionRule,
                                 String alterCommand) {
            this.strategy = partitionStrategy;
            this.partitionRule = partitionRule;
            this.alterCommand = SKIP_REAL_ALTER_PARTITION_HINT + alterCommand;
            this.initDataType = initDataType;
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
                ", alterTableGroupCommand='" + alterCommand + '\'' +
                ", logicalTableNames=" + logicalTableNames +
                '}';
        }
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
