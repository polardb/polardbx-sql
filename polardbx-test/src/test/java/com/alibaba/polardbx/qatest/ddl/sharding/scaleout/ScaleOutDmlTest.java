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

package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author luoyanxin
 */
@RunWith(Parameterized.class)
@NotThreadSafe
public class ScaleOutDmlTest extends ScaleOutBaseTest {

    private static List<ComplexTaskMetaManager.ComplexTaskStatus> moveTableStatus =
        Stream.of(ComplexTaskMetaManager.ComplexTaskStatus.ABSENT,
            ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC).collect(Collectors.toList());
    final ComplexTaskMetaManager.ComplexTaskStatus finalTableStatus;
    static boolean firstIn = true;
    static ComplexTaskMetaManager.ComplexTaskStatus currentStatus = ComplexTaskMetaManager.ComplexTaskStatus.ABSENT;
    final Boolean isCache;

    private String tableWithGsi = "tb_with_gsi";
    private String[] logicalTables = new String[] {"brd_tbl1", "shrd_tbl1", "tb_with_gsi"};
    private String[] allLogicalTables = new String[] {"brd_tbl1", "shrd_tbl1", "tb_with_gsi", "gsi_tb_with_gsi"};
    private String columnsTemplate =
        "(pk, id1, num1, num2, date1, timestamp1, datetime1, time1, year1, double1, binary1)";
    private String valuesTemplate1 = "(1, 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
    private String valuesTemplate2 = "(null, 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";

    private Map<String, List<String>> physicalTablesOnSourceGroup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private String hint;

    public ScaleOutDmlTest(StatusInfo tableStatus) {
        super("ScaleOutDmlTest", "polardbx_meta_db_polardbx",
            ImmutableList.of(tableStatus.getMoveTableStatus().toString()));
        finalTableStatus = tableStatus.getMoveTableStatus();
        isCache = tableStatus.isCache();
        if (!currentStatus.equals(finalTableStatus)) {
            firstIn = true;
            currentStatus = finalTableStatus;
        }
        hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, getTableDefinitions(), true);
            firstIn = false;
            physicalTablesOnSourceGroup.clear();
            for (String logicalTable : allLogicalTables) {
                if (logicalTable.toLowerCase().startsWith("brd_")) {
                    String phyTableName = getTbNamePattern(logicalTable, tddlConnection);
                    physicalTablesOnSourceGroup.put(logicalTable, ImmutableList.of(phyTableName));
                } else {
                    physicalTablesOnSourceGroup
                        .put(logicalTable, getAllPhysicalTables(new String[] {logicalTable}).get(sourceGroupKey));
                }
            }
        } else {
            String tddlSql = "use " + logicalDatabase;
            JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        }
        for (String tableName : logicalTables) {
            String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            JdbcUtil.executeUpdate(tddlConnection, sql);
        }
    }

    @Parameterized.Parameters(name = "{index}:TableStatus={0}")
    public static List<StatusInfo[]> prepareDate() {
        List<StatusInfo[]> status = new ArrayList<>();
        moveTableStatus.stream().forEach(c -> {
            status.add(new StatusInfo[] {new StatusInfo(c, false)});
            status.add(new StatusInfo[] {new StatusInfo(c, true)});
            status.add(new StatusInfo[] {new StatusInfo(c, null)});
        });
        return status;
    }

    @Test
    public void testInsert() {
        testInsertOrReplace(false, false, "");
    }

    @Test
    public void testReplace() {
        testInsertOrReplace(true, false, "");
    }

    @Test
    public void testInsertSelect() {
        testInsertOrReplaceSelect(false, false, "");
    }

    @Test
    public void testReplaceSelect() {
        testInsertOrReplaceSelect(true, false, "");
    }

    @Test
    public void testReplaceWithSet() {
        testReplaceWithValueList(false, "");
    }

    @Test
    public void testDelete() {
        String[] logicalTables = new String[] {"brd_tbl1", "shrd_tbl1"};
        String columns = columnsTemplate;
        String values = valuesTemplate1;
        for (String logicalTable : logicalTables) {
            String sql = String.format("insert into %s%s values %s;", logicalTable, columns, values);

            executeDml(hint + sql);
            String whereVal = "1";
            sql = String.format("delete from %s where num1=%s;", logicalTable, whereVal);
            executeDml(hint + sql);

            if ((finalTableStatus.isWritable() || finalTableStatus.isDeleteOnly()) && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable),
                    " where num1 <> " + whereVal);
            }
        }
    }

    @Test
    public void testSingleTableUpdate() {
        testUpdate(false, "");
    }

    @Test
    public void testMultilTableUpdate() {
        testMutilTablesUpdate();
    }

    @Test
    public void testDynamicFunction() {
        //CURRENT_USER(),USER(),
        String[] functionNames = ("LAST_INSERT_ID(),CONNECTION_ID(), CURRENT_USER,DATABASE()," +
            "SCHEMA(),SLEEP(0), VERSION(),RAND(),RAND(13),NOW(),CURRENT_DATE()," +
            " CURRENT_DATE,CURRENT_TIME(), CURRENT_TIME,CURRENT_TIMESTAMP(), " +
            "CURRENT_TIMESTAMP,CURDATE(),CURTIME(),SYSDATE(),UTC_DATE(),UTC_TIME()," +
            "UTC_TIMESTAMP(),LOCALTIME(), LOCALTIME,LOCALTIMESTAMP(), LOCALTIMESTAMP,UUID()," +
            "UUID_SHORT()").split(",");
        for (String functionName : functionNames) {
            testInsertOrReplace(false, true, functionName);
            testInsertOrReplace(true, true, functionName);
            testInsertOrReplaceSelect(false, true, functionName);
            testInsertOrReplaceSelect(true, true, functionName);
            testUpdate(true, functionName);
            testReplaceWithValueList(true, functionName);
        }

    }

    @Test
    public void testGsiTableDML() {
        boolean isReplace = false;
        String logicalTableName = "tb_with_gsi";
        String gsiTableName = "gsi_tb_with_gsi";
        String columns =
            "(ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)";
        String values = "";
        String sql = "";
        final int maxRange = 50;

        Random random = new Random();
        do {
            for (int i = 0; i < maxRange; i++) {
                values = MessageFormat.format(
                    "({0},{1},{2},{3},{4},now(),12.34,{5},{6},{7})", i,
                    random.nextInt(maxRange), random.nextInt(maxRange),
                    random.nextInt(maxRange), random.nextInt(maxRange),
                    random.nextInt(maxRange), random.nextInt(maxRange), random.nextInt(maxRange));
                sql = MessageFormat
                    .format("{0} into {1} {2} values {3}", isReplace ? "replace" : "insert", logicalTableName, columns,
                        values);
                executeDml(hint + sql);

            }
            if ((finalTableStatus.isWritable()) && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTableName), "");
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(gsiTableName), "");
            }
            isReplace = !isReplace;
        } while (isReplace);
        for (int i = 0; i < maxRange; i++) {
            sql = MessageFormat
                .format("update {0} set {1} = {2} where {3} = {4}",
                    logicalTableName, "ol_quantity", i, "ol_quantity",
                    i + 1);
            executeDml(hint + sql);
            sql = MessageFormat
                .format("update {0} set {1} = {2} where {3} = {4}", logicalTableName, "ol_d_id", i + 500, "ol_d_id",
                    i + 1);
            JdbcUtil.executeUpdate(tddlConnection, sql);
        }
        if (finalTableStatus.isWritable() && firstIn) {
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTableName), "");
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(gsiTableName), "");
        }
        for (int i = 0; i < maxRange; i++) {
            sql = MessageFormat
                .format("delete from {0} where {1}={2}",
                    logicalTableName, "ol_quantity", i);
            executeDml(hint + sql);
        }
        if (finalTableStatus.isWritable() && firstIn) {
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTableName), "");
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(gsiTableName), "");
        }
    }

    private void testInsertOrReplace(boolean isReplace, boolean isDynamicValue, String dynamicFunction) {
        cleanUpTables();
        String type = isReplace ? "replace" : "insert";
        String columns = columnsTemplate;
        String values1 = valuesTemplate1;
        String values2 = valuesTemplate2;
        int brdDbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+singledb+scaleoutdb

        if (isDynamicValue) {
            columns = "(pk, id2, id1, num1, num2, date1, timestamp1, datetime1, time1, year1, double1, binary1)";
            values1 =
                "(1, " + dynamicFunction + ", 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
            values2 =
                "(" + String.valueOf(1 + brdDbCount) + ", " + dynamicFunction
                    + ", 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
        }
        for (String logicalTable : logicalTables) {
            if (logicalTable.equalsIgnoreCase(tableWithGsi)) {
                continue;
            }
            String sql = String.format("%s into %s%s values %s,%s;", type, logicalTable, columns, values1, values2);

            executeDml(hint + sql);

            //insert ignore
            sql = String
                .format("%s into %s%s values %s,%s;", isReplace ? "replace" : "insert ignore", logicalTable, columns,
                    values1, values2);
            executeDml(hint + sql);

            //insert on duplicate key update
            if (!isReplace) {
                String onDuplicateUpdate =
                    "ON DUPLICATE KEY UPDATE id1='id111', num2=num2+200, num1=values(num1), timestamp1=now()";
                sql = String.format("%s into %s%s values %s,%s %s;", "insert", logicalTable, columns, values1, values2,
                    onDuplicateUpdate);
                executeDml(hint + sql);
            }
            if (finalTableStatus.isWritable() && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable), "");
            }
        }
    }

    private void testInsertOrReplaceSelect(boolean isReplace, boolean isDynamicValue, String dynamicFunction) {
        cleanUpTables();
        String type = isReplace ? "replace" : "insert";
        String[] logicalTables = new String[] {"brd_tbl1", "shrd_tbl1"};
        int brdDbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+singledb+scaleoutdb
        String selectColumns =
            "pk+" + String.valueOf(brdDbCount)
                + ", id1, num1, num2, date1, timestamp1, datetime1, time1, year(year1), double1, binary1";
        //String columns = "(" + selectColumns + ")";
        String columns = "(pk, id1, num1, num2, date1, timestamp1, datetime1, time1, year1, double1, binary1)";
        String values = "(null, 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
        if (isDynamicValue) {
            columns = "(pk, id2, id1, num1, num2, date1, timestamp1, datetime1, time1, year1, double1, binary1)";
            selectColumns = "pk+" + String.valueOf(brdDbCount) + "," + dynamicFunction
                + ", id1, num1, num2, date1, timestamp1, datetime1, time1, year(year1), double1, binary1";
            values =
                "(null, " + dynamicFunction + ", 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
        }
        for (String logicalTable : logicalTables) {
            String sql = String.format("insert into %s%s values %s;", logicalTable, columns, values);

            executeDml(hint + sql);
            sql = String
                .format("%s into %s%s select %s from %s;", type, logicalTable, columns, selectColumns, logicalTable);
            executeDml(hint + sql);
            if (finalTableStatus.isWritable() && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable), "");
            }
        }
    }

    private void testUpdate(boolean isDynamicValue, String dynamicFunction) {

        cleanUpTables();
        for (String logicalTable : logicalTables) {
            if (logicalTable.equalsIgnoreCase(tableWithGsi)) {
                continue;
            }
            String sql = String.format("insert into %s%s values %s;", logicalTable, columnsTemplate, valuesTemplate1);

            executeDml(hint + sql);

            sql = String.format("update %s set num1 = %s where num2 = %s;", logicalTable, 11, 2);
            executeDml(hint + sql);
            if (isDynamicValue) {
                sql = String.format("update %s set id2 = %s where num2 = %s;", logicalTable, dynamicFunction, 2);
                executeDml(hint + sql);
            }
            if (finalTableStatus.isWritable() && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable), "");
            }
        }
    }

    public void testReplaceWithValueList(boolean isDynamicValue, String dynamicFunction) {
        cleanUpTables();
        for (String logicalTable : logicalTables) {
            if (logicalTable.equalsIgnoreCase(tableWithGsi)) {
                continue;
            }
            String values = "(1, 'id1', 1, 2, '2020-03-24', now(),  now(), now(), 2020, 2.34, null)";
            String sql = String.format("insert into %s%s values %s;", logicalTable, columnsTemplate, values);

            executeDml(hint + sql);

            sql = String.format("replace %s set num1 = %s, num2 = %s;", logicalTable, 11, 2);
            executeDml(hint + sql);
            sql = String.format("replace %s set pk = %s, num1 = %s, num2 = %s;", logicalTable, 1, 12, 20);
            executeDml(hint + sql);
            if (isDynamicValue) {
                sql = String.format("replace %s set id2 = %s, num2 = %s;", logicalTable, dynamicFunction, 2);
                executeDml(hint + sql);
            }
            if (finalTableStatus.isWritable() && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable), "");
            }
        }
    }

    private void testMutilTablesUpdate() {
        cleanUpTables();
        String sql = "";
        for (String logicalTable : logicalTables) {
            sql = String.format("insert into %s%s values %s;", logicalTable, columnsTemplate, valuesTemplate1);
            if (logicalTable.equalsIgnoreCase(tableWithGsi)) {
                continue;
            }
            executeDml(hint + sql);
        }
        String hint = "/*+TDDL({'extra':{'ENABLE_COMPLEX_DML_CROSS_DB':'TRUE'}})*/";
        sql = String.format("%supdate %s t1,%s t2 set t1.num1=%s,t2.id2=%s where t1.num1 = t2.num1;",
            hint, logicalTables[0], logicalTables[1], 11, 12);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        for (String logicalTable : logicalTables) {
            if (logicalTable.equalsIgnoreCase(tableWithGsi)) {
                continue;
            }
            if (finalTableStatus.isWritable() && firstIn) {
                checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(logicalTable), "");
            }
        }

    }

    private void cleanUpTables() {
        executeDml("delete from brd_tbl1");
        executeDml("delete from shrd_tbl1");
        executeDml("delete from tb_with_gsi");
    }

    protected static List<String> getTableDefinitions() {

        List<String> createTables = new ArrayList<>();

        String createTable = " CREATE TABLE `tb_with_gsi` (\n" +
            "\t`ol_w_id` int(11) NOT NULL,\n" +
            "\t`ol_d_id` int(11) NOT NULL,\n" +
            "\t`ol_o_id` int(11) NOT NULL,\n" +
            "\t`ol_number` int(11) NOT NULL,\n" +
            "\t`ol_i_id` int(11) NOT NULL,\n" +
            "\t`ol_delivery_d` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "\t`ol_amount` decimal(6, 2) DEFAULT NULL,\n" +
            "\t`ol_supply_w_id` int(11) DEFAULT NULL,\n" +
            "\t`ol_quantity` int(11) DEFAULT NULL,\n" +
            "\t`ol_dist_info` char(24) DEFAULT NULL,\n" +
            "\tPRIMARY KEY (`ol_w_id`, `ol_d_id`),\n" +
            "\tKEY `auto_shard_key_ol_w_id` USING BTREE (`ol_w_id`),\n" +
            "\tGLOBAL INDEX `gsi_tb_with_gsi`(`ol_d_id`, `ol_quantity`, `ol_dist_info`) " +
            "COVERING (`ol_w_id`) DBPARTITION BY HASH(`ol_d_id`)) ENGINE = InnoDB DEFAULT " +
            "CHARSET = utf8  dbpartition by hash(`ol_w_id`)";
        createTables.add(createTable);
        createTable = "CREATE TABLE `brd_tbl1` (\n" +
            "  `pk` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
            "  `id1` varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
            "  `id2` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'id2_default',\n" +
            "  `num1` int(11) DEFAULT NULL,\n" +
            "  `num2` int(11) NOT NULL DEFAULT '-2',\n" +
            "  `date1` date DEFAULT NULL,\n" +
            "  `date2` date NOT NULL DEFAULT '2020-03-24',\n" +
            "  `timestamp1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `timestamp2` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  `timestamp3` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `timestamp4` timestamp NOT NULL DEFAULT '2020-03-24 15:10:12' ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `datetime1` datetime DEFAULT NULL,\n" +
            "  `time1` time DEFAULT NULL,\n" +
            "  `year1` year(4) DEFAULT NULL,\n" +
            "  `double1` double DEFAULT NULL,\n" +
            "  `double2` double NOT NULL DEFAULT '1.23',\n" +
            "  `binary1` blob,\n" +
            "  PRIMARY KEY (`pk`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=2000005 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci  broadcast";
        createTables.add(createTable);
        createTable = "CREATE TABLE `shrd_tbl1` (\n" +
            "  `pk` bigint(20) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
            "  `id1` varchar(30) DEFAULT NULL,\n" +
            "  `id2` varchar(50) NOT NULL DEFAULT 'id2_default',\n" +
            "  `num1` int(11) DEFAULT NULL,\n" +
            "  `num2` int(11) NOT NULL DEFAULT '-2',\n" +
            "  `date1` date DEFAULT NULL,\n" +
            "  `date2` date NOT NULL DEFAULT '2020-03-24',\n" +
            "  `timestamp1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `timestamp2` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  `timestamp3` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `timestamp4` timestamp NOT NULL DEFAULT '2020-03-24 15:10:12' ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  `datetime1` datetime DEFAULT NULL,\n" +
            "  `time1` time DEFAULT NULL,\n" +
            "  `year1` year(4) DEFAULT NULL,\n" +
            "  `double1` double DEFAULT NULL,\n" +
            "  `double2` double NOT NULL DEFAULT '1.23',\n" +
            "  `binary1` blob,\n" +
            "  PRIMARY KEY (`pk`),\n" +
            "  KEY `auto_shard_key_num2` (`num2`) USING BTREE\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8  dbpartition by hash(`pk`) tbpartition by hash(`num2`) tbpartitions 4";
        createTables.add(createTable);

        return createTables;
    }
}