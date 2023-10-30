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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.showFullCreateTable;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class CreateTableTest extends PartitionTestBase {

    private static String CREATE_TABLE_PATTERN1 = "create table %s(a int, b int) partition by hash(a) partitions 3";
    private static String CREATE_TABLE_PATTERN2 = "create table %s(a int, b int) partition by key(a) partitions 3";

    private static String CREATE_TABLE_PATTERN3 = "create table %s(a bigint, b int) partition by hash(a) partitions 3";

    private static String CREATE_TABLE_PATTERN4 =
        "create table %s(a int, b int) partition by list(a) (partition p1 values in(1,2,3), partition p2 values in(4,5,6))";
    private static String CREATE_TABLE_PATTERN5 =
        "create table %s(a int, b int) partition by list(a) (partition p2 values in(4,5,6), partition p1 values in(1,2,3))";
    private static String CREATE_TABLE_PATTERN6 =
        "create table %s(a int, b int) partition by list(a) (partition p1 values in(2,3,1), partition p2 values in(5,4,6))";

    private static String CREATE_TABLE_PATTERN7 =
        "create table %s(a int, b int) partition by list(a) (partition p2 values in(1,2,3), partition p1 values in(4,5,6))";

    private static String CREATE_TABLE_PATTERN8 =
        "create table %s(a bigint, b int) partition by list(a) (partition p1 values in(1,2,3), partition p2 values in(4,5,6))";

    private static String CREATE_TABLE_PATTERN9 =
        "create table %s(a int, b int) partition by list columns(a,b) (partition p1 values in((1,1),(2,1),(3,1)), partition p2 values in((4,1),(5,1),(6,1)))";
    private static String CREATE_TABLE_PATTERN10 =
        "create table %s(a int, b int) partition by list columns(a,b) (partition p2 values in((4,1),(5,1),(6,1)), partition p1 values in((1,1),(2,1),(3,1)))";
    private static String CREATE_TABLE_PATTERN11 =
        "create table %s(a int, b int) partition by list columns(a,b) (partition p2 values in((5,1),(4,1),(6,1)), partition p1 values in((2,1),(3,1),(1,1)))";

    private static String CREATE_TABLE_PATTERN12 =
        "create table %s(a bigint, b int) partition by list columns(a,b) (partition p1 values in((1,1),(2,1),(3,1)), partition p2 values in((4,1),(5,1),(6,1)))";

    private static String CREATE_TABLE_PATTERN13 =
        "create table %s(a datetime, b int) partition by hash(year(a)) partitions 3";

    private static String CREATE_TABLE_PATTERN14 =
        "create table %s(a datetime, b int) partition by hash(month(a)) partitions 3";

    private static String CREATE_TABLE_PATTERN15 =
        "create table %s(a int, b int) partition by range(a) (partition p1 values less than(10),partition p2 values less than(50))";
    private static String CREATE_TABLE_PATTERN16 =
        "create table %s(a bigint, b int) partition by range(a) (partition p1 values less than(10),partition p2 values less than(50))";
    private static String CREATE_TABLE_PATTERN17 =
        "create table %s(a bigint, b int) partition by range(a) (partition p3 values less than(10),partition p2 values less than(50))";

    private static String CREATE_TABLE_PATTERN18 =
        "create table %s(a int, b int) partition by range columns(a,b) (partition p1 values less than(10,20),partition p2 values less than(50,20))";

    private static String CREATE_TABLE_PATTERN19 =
        "create table %s(a int, b int) partition by range columns(a,b) (partition p1 values less than(10,20),partition p2 values less than(20,50))";

    private static String CREATE_TABLE_PATTEN20 =
        "create table %s(%s) %s";

    @Test
    public void hashTableTest() {
        String dropTable = "drop table if exists %s";
        String tb1 = "tb1";
        String tb2 = "tb2";
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN1, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(dropTable, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN1, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb2).longValue());

        String tb3 = "tb3";
        sql = String.format(dropTable, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN2, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb3).longValue());

        String tb4 = "tb4";
        sql = String.format(dropTable, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN3, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb1).longValue() != getTableGroupId(tb4).longValue());

        String tb5 = "tb5";
        sql = String.format(dropTable, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN13, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb6 = "tb6";
        sql = String.format(dropTable, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN13, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb5).longValue(),
            getTableGroupId(tb6).longValue());

        String tb7 = "tb7";
        sql = String.format(dropTable, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN14, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb6).longValue() != getTableGroupId(tb7).longValue());

        //"create table %s(%s) partition by %s";
        String tb8 = "tb8";
        String colInfo = "a int, b int, c int";
        String partInfo = "partition by hash(a,b,c) partitions 3";

        sql = String.format(dropTable, tb8);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb8, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb9 = "tb9";
        sql = String.format(dropTable, tb9);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb9, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb8).longValue(),
            getTableGroupId(tb9).longValue());
///////
        String tb10 = "tb10";
        colInfo = "a int, b varchar(20), c int";

        sql = String.format(dropTable, tb10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb10, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb10).longValue() != getTableGroupId(tb9).longValue());

        String tb11 = "tb11";

        sql = String.format(dropTable, tb11);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb11, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb10).longValue(),
            getTableGroupId(tb11).longValue());

        String tb12 = "tb12";
        colInfo = "a int, b varchar(30), c int";

        sql = String.format(dropTable, tb12);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb12, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb11).longValue() != getTableGroupId(tb12).longValue());

/////
        String tb13 = "tb13";
        colInfo = "a int, b varchar(20), c int";
        partInfo = "partition by hash(b) partitions 3 character set latin1";

        sql = String.format(dropTable, tb13);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb13, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb14 = "tb14";
        sql = String.format(dropTable, tb14);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb14, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb14).longValue(),
            getTableGroupId(tb13).longValue());

        String tb15 = "tb15";
        colInfo = "a int, b varchar(20), c int";
        partInfo = "partition by hash(b) partitions 3";

        sql = String.format(dropTable, tb15);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb15, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb15).longValue() != getTableGroupId(tb14).longValue());

    }

    @Test
    public void listTableTest() {
        String dropTable = "drop table if exists %s";
        String tb1 = "tb1";
        String tb2 = "tb2";
        String tb3 = "tb3";
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN4, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(dropTable, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN5, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(dropTable, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN6, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb2).longValue());
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb3).longValue(),
            getTableGroupId(tb2).longValue());

        String tb4 = "tb4";
        sql = String.format(dropTable, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN7, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb4).longValue() != getTableGroupId(tb1).longValue());

        String tb5 = "tb5";
        sql = String.format(dropTable, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN8, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb5).longValue() != getTableGroupId(tb1).longValue());

        String tb6 = "tb6";
        sql = String.format(dropTable, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN9, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb7 = "tb7";
        sql = String.format(dropTable, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN9, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb6).longValue(),
            getTableGroupId(tb7).longValue());

        String tb8 = "tb8";
        sql = String.format(dropTable, tb8);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN10, tb8);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb8).longValue(),
            getTableGroupId(tb7).longValue());

        String tb9 = "tb9";
        sql = String.format(dropTable, tb9);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN11, tb9);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb9).longValue(),
            getTableGroupId(tb7).longValue());

        String tb10 = "tb10";
        sql = String.format(dropTable, tb10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN12, tb10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb10).longValue() != getTableGroupId(tb7).longValue());

        String tb16 = "tb16";
        String colInfo = "a varchar(20), b int, c int";
        String partInfo =
            "partition by list columns(a,b) (partition p1 values in(('1',1),('3',3)), partition p2 values in(('2',2), ('4',4)))";
        sql = String.format(dropTable, tb16);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb16, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb17 = "tb17";
        colInfo = "a varchar(30), b int, c int";
        sql = String.format(dropTable, tb17);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb17, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb16).longValue() != getTableGroupId(tb17).longValue());

        String tb18 = "tb18";
        sql = String.format(dropTable, tb18);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb18, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb18).longValue(),
            getTableGroupId(tb17).longValue());

        String tb19 = "tb19";
        partInfo =
            "partition by list columns(a,b) (partition p1 values in(('1',1),('3',3)), partition p2 values in(('2',2), ('4',4))) character set latin1";
        sql = String.format(dropTable, tb19);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb19, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb18).longValue() != getTableGroupId(tb19).longValue());

        String tb20 = "tb20";
        sql = String.format(dropTable, tb20);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb20, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb19).longValue(),
            getTableGroupId(tb20).longValue());

    }

    @Test
    public void rangeTableTest() {
        String dropTable = "drop table if exists %s";
        String tb1 = "tb1";
        String tb2 = "tb2";
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN15, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(dropTable, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN15, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb2).longValue());

        String tb3 = "tb3";
        sql = String.format(dropTable, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN16, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb4 = "tb4";
        sql = String.format(dropTable, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN17, tb4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb3).longValue() != getTableGroupId(tb4).longValue());

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb3).longValue() != getTableGroupId(tb1).longValue());

        String tb5 = "tb5";
        sql = String.format(dropTable, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN18, tb5);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb6 = "tb6";
        sql = String.format(dropTable, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN18, tb6);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb5).longValue(),
            getTableGroupId(tb6).longValue());

        String tb7 = "tb7";
        sql = String.format(dropTable, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN19, tb7);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb7).longValue() != getTableGroupId(tb6).longValue());

        String tb8 = "tb8";
        String colInfo = "a varchar(20), b int, c int";
        String partInfo =
            "partition by range columns(b,a) (partition p1 values less than(10,'20'),partition p2 values less than(50,'30'))";
        sql = String.format(dropTable, tb8);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb8, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tb9 = "tb9";
        colInfo = "a varchar(31), b int, c int";
        sql = String.format(dropTable, tb9);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb9, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb8).longValue() != getTableGroupId(tb9).longValue());

        String tb10 = "tb10";
        sql = String.format(dropTable, tb10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb10, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb9).longValue(),
            getTableGroupId(tb10).longValue());

        String tb11 = "tb11";
        partInfo =
            "partition by range columns(b,a) (partition p1 values less than(10,'20'),partition p2 values less than(50,'30')) character set latin1";
        sql = String.format(dropTable, tb11);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb11, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb11).longValue() != getTableGroupId(tb10).longValue());

        String tb12 = "tb12";
        sql = String.format(dropTable, tb12);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTEN20, tb12, colInfo, partInfo);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb11).longValue(),
            getTableGroupId(tb12).longValue());
    }

    @Test
    public void setTableGroupForBroadCastTableTest() {
        String dropTable = "drop table if exists %s";
        String tb = "brd_tb";
        String tgName = "test_tg";
        String sql = String.format(dropTable, tb);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("create table %s (a int) broadcast tablegroup=%s", tb, tgName);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can't set the broadcast table's tablegroup explicitly");
    }

    @Test
    public void testCreateTableLike() throws Exception {

        String tableName = "tbl_with_create_table_like";
        String dropSql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE TABLE `%s` (\n"
                + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "\t`gmt_modified` datetime NOT NULL,\n"
                + "\t`seller_id` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` int(11) DEFAULT NULL,\n"
                + "\tPRIMARY KEY (`id`, `gmt_modified`),\n"
                + "\tINDEX `idx` (`seller_id`)\n"
                + ") ENGINE = InnoDB AUTO_INCREMENT = 100014 DEFAULT CHARSET = utf8mb4\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 12\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE NOW()", tableName));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s_like like %s", tableName, tableName));

        String newCreateTable = showFullCreateTable(tddlConnection, tableName + "_like");
        Assert.assertTrue(newCreateTable.contains("EXPIRE AFTER 12"));
        Assert.assertTrue(newCreateTable.contains("LOCAL PARTITION BY RANGE"));
    }

    @Test
    public void testLargeBlobBackfill() {
        // test client limits
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global CONN_POOL_XPROTO_MAX_PACKET_SIZE = 67108864");

        String dropTable = "drop table if exists `largeCol`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        String createTable =
            "CREATE TABLE  if not exists `largeCol` ( `id` int(11) NOT NULL AUTO_INCREMENT, `c1` longtext, PRIMARY KEY (`id`)) partition by key(id) partitions 1;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insertSql = "insert into largeCol(id, c1) values(null,%s)";
        int rowSize = 1024 * 1024;
        int rowCount = 136;
        String baseText =
            "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowSize; i = i + baseText.length()) {
            sb.append(baseText);
        }
        String sql = String.format(insertSql, sb.toString());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        //16 rows
        for (int i = 0; i < 4; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into largeCol(id,c1) select null, c1 from largeCol");
        }
        //15*3 rows
        for (int i = 0; i < 4; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into largeCol(id,c1) select null, c1 from largeCol limit 15");
        }
        //6 rows
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into largeCol(id,c1) select null, c1 from largeCol limit 6");

        sql = "alter table largeCol add global index g1(id) covering(c1) partition by key(id) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testLargeBlobBackfill2() {
        // Set client limit to 128MB to test auto shrink batch on limit of server settings
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global CONN_POOL_XPROTO_MAX_PACKET_SIZE = 134217728");

        String dropTable = "drop table if exists `largeCol`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);

        String createTable =
            "CREATE TABLE  if not exists `largeCol` ( `id` int(11) NOT NULL AUTO_INCREMENT, `c1` longtext, PRIMARY KEY (`id`)) partition by key(id) partitions 1;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insertSql = "insert into largeCol(id, c1) values(null,%s)";
        int rowSize = 1024 * 1024;
        int rowCount = 136;
        String baseText =
            "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowSize; i = i + baseText.length()) {
            sb.append(baseText);
        }
        String sql = String.format(insertSql, sb.toString());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        //16 rows
        for (int i = 0; i < 4; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into largeCol(id,c1) select null, c1 from largeCol");
        }
        //15*3 rows
        for (int i = 0; i < 4; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into largeCol(id,c1) select null, c1 from largeCol limit 15");
        }
        //6 rows
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into largeCol(id,c1) select null, c1 from largeCol limit 6");

        sql = "alter table largeCol add global index g1(id) covering(c1) partition by key(id) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // reset to 64M
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global CONN_POOL_XPROTO_MAX_PACKET_SIZE = 67108864");
    }

    @Test
    public void testShowCreateTableForEscape() {
        final String tableName = "```test``backtick`";
        final String createTable = "create table %s (```col-minus` int(11) default null, `c2` int(11) default null)";

        dropTableIfExists(tableName);

        String sql = String.format(createTable, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String showCreateTableSql = showCreateTable(tddlConnection, tableName);

        int lastIndex = TStringUtil.lastIndexOf(showCreateTableSql, ")");
        showCreateTableSql = TStringUtil.substring(showCreateTableSql, 0, lastIndex + 1).toLowerCase();
        showCreateTableSql = removeInvisibleChar(showCreateTableSql);
        sql = removeInvisibleChar(sql);

        if (!TStringUtil.equalsIgnoreCase(sql, showCreateTableSql)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\n").append("Expected: ").append(sql).append("\n");
            buf.append("  Actual: ").append(showCreateTableSql);
            Assert.fail(buf.toString());
        }

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithPercentSign() {
        String tableName = "tbWithPercSign";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("drop table if exists %s", tableName));
        String tableBody = "(a int, b int comment 'abc %s ddd') partition by key(a) partitions 2";
        String sql = String.format("create table %s %s", tableName, tableBody);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testBug50139035() {
        String sql =
            "create table testBug50139035(a int) ENGINE = InnoDB DEFAULT CHARSET = `utf8` DEFAULT COLLATE = `utf8_general_ci`";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private static final String EMPTY = "";

    private String removeInvisibleChar(String sql) {
        return sql == null ? EMPTY :
            sql.replace("\r", EMPTY).replace("\n", EMPTY).replace("\t", EMPTY).replace(" ", EMPTY);
    }
}
