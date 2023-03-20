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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Ignore;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PolarDBXPartitionManagementTest extends MysqlTest {

    public void testRefreshTopology() {
        String sql = "/*+TDDL:CMD_EXTRA(xx=true)*/\nrefresh topology";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testCreateTableWithTg() {
        String sql = "CREATE TABLE `tbl` (\n"
            + "  `a` varchar(32) NOT NULL,\n"
            + "  `b` datetime NOT NULL\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  \n"
            + "PARTITION BY RANGE COLUMNS(a,b)\n"
            + "(PARTITION p0 VALUES LESS THAN ('ab', '1998-01-01') ENGINE = InnoDB,\n"
            + " PARTITION p1 VALUES LESS THAN ('bc', '1999-01-01') ENGINE = InnoDB,\n"
            + " PARTITION p2 VALUES LESS THAN ('cd', '2000-01-01') ENGINE = InnoDB);";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        //Assert.assertTrue(false);
    }

    public void testCreateTableWithGsi() {
        String sql = "CREATE TABLE t_order (\n"
            + " `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + " `order_id` bigint DEFAULT NULL,\n"
            + " `buyer_id` varchar(20) DEFAULT NULL,\n"
            + " `seller_id` bigint DEFAULT NULL,\n"
            + " `order_snapshot` longtext DEFAULT NULL,\n"
            + " `order_detail` longtext DEFAULT NULL,\n"
            + " PRIMARY KEY (`id`),\n"
            + " GLOBAL INDEX `g_i_seller`(`seller_id`) COVERING (`id`, `order_id`, `buyer_id`, `order_snapshot`) partition by hash(`seller_id`) partitions 4\n"
            + ") partition by hash(`id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testCreateTableWithUGsi() {
        String sql = "CREATE TABLE t_order (\n"
            + " `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + " `order_id` bigint DEFAULT NULL,\n"
            + " `buyer_id` varchar(20) DEFAULT NULL,\n"
            + " `seller_id` bigint DEFAULT NULL,\n"
            + " `order_snapshot` longtext DEFAULT NULL,\n"
            + " `order_detail` longtext DEFAULT NULL,\n"
            + " PRIMARY KEY (`id`),\n"
            + " UNIQUE GLOBAL INDEX `g_i_seller`(`seller_id`) COVERING (`id`, `order_id`, `buyer_id`, `order_snapshot`) partition by hash(`seller_id`) partitions 4\n"
            + ") partition by hash(`id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testAlterTableAddGsi() {
        String sql =
            "ALTER TABLE t_order ADD GLOBAL INDEX `g_i_buyer` (`buyer_id`) COVERING (`order_snapshot`) partition by hash(`buyer_id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testAlterTableAddUGsi() {
        String sql =
            "ALTER TABLE t_order ADD UNIQUE GLOBAL INDEX `g_i_buyer` (`buyer_id`) COVERING (`order_snapshot`) partition by hash(`buyer_id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testCreateGsi() {
        String sql =
            "CREATE GLOBAL INDEX `g_i_seller` ON t_order (`seller_id`) partition by hash(`buyer_id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testCreateUGsi() {
        String sql =
            "CREATE UNIQUE GLOBAL INDEX `g_i_seller` ON t_order (`seller_id`) partition by hash(`buyer_id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testCreateRangeGsi() {
        String sql =
            "CREATE TABLE t_order4 (  `id` bigint(11) NOT NULL AUTO_INCREMENT,  `order_id` bigint DEFAULT NULL,  `buyer_id` varchar(20) DEFAULT NULL,  `seller_id` bigint DEFAULT NULL,  `order_snapshot` longtext DEFAULT NULL,  `order_detail` longtext DEFAULT NULL,  PRIMARY KEY (`id`),  UNIQUE GLOBAL INDEX `g_i_seller4`(`seller_id`) COVERING (`id`, `order_id`, `buyer_id`, `order_snapshot`)  PARTITION BY range(seller_id) (partition p1 values less than(200)) ) partition by hash(`id`) partitions 4;\n";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testCreateTable() {
        String sql = "CREATE TABLE t_order (\n"
            + " `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + " `order_id` bigint DEFAULT NULL,\n"
            + " `buyer_id` varchar(20) DEFAULT NULL,\n"
            + " `seller_id` bigint DEFAULT NULL,\n"
            + " `order_snapshot` longtext DEFAULT NULL,\n"
            + " `order_detail` longtext DEFAULT NULL,\n"
            + " PRIMARY KEY (`id`)\n"
            + ") partition by hash(`id`) partitions 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
    }

    public void testAlterTableSetTableGroup() {

        String sql = "alter table t1\n\tset tablegroup = 'abc'";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());

    }

    public void testCreateTableGroup() {
        String sql = "create tablegroup t1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement result = statementList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testCreateTableGroupIfNotExists() {
        String sql = "create tablegroup if not exists t1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement result = statementList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testShowTableGroup() {
        String sql = "show tablegroup order by SCHEMA_NAME limit 1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement result = statementList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableAddPartitionVal1() {
        String sql = "ALTER TABLE T_PART_LIST\n\t MODIFY PARTITION P2 ADD VALUES ('USERS')";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement result = statementList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableAddPartitionVal2() {
        String sql = "ALTER TABLE T_PART_LIST\n\t MODIFY PARTITION P3 DROP VALUES ('A')";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement result = statementList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testSplitTableGroupAtPoint() {
        String sql = "ALTER TABLEGROUP grp1 split PARTITION p0 at(15) into (partition p10, partition p11)";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testSplitTableGroupAtDefaultPoint() {
        String sql = "ALTER TABLEGROUP grp1 split PARTITION p0 ";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupSplitListPartition() {
        String sql =
            "ALTER TABLEGROUP grp1 split PARTITION p0 into (partition p10 VALUES IN (1, 2, 3), partition p11 VALUES IN (4, 5, 6), partition p12 VALUES IN (7, 8, 9))";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupSplitRangePartition() {
        String sql =
            "ALTER TABLEGROUP grp1 split PARTITION p0 into (partition p10 VALUES LESS THAN (1000), partition p11 VALUES LESS THAN (2000), partition p12 VALUES LESS THAN (3000))";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testSplitTableGroupAtHotKey() {
        String sql = "alter tablegroup tgName extract to partition by hot value(xxx)";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testMergeTableGroup() {
        String sql = "ALTER TABLEGROUP grp1 merge PARTITIONS p0, p1, p2 to p0 ";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testMoveTableGroup() {
        String sql = "ALTER TABLEGROUP grp1 move PARTITIONS p0, p1, p2 to xx ";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    @Ignore
    public void testTruncateTableGroup() {
        if (0 == 1) {
            String sql = "ALTER TABLEGROUP grp1 truncate PARTITIONS p0, p1, p2";
            SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
            List<SQLStatement> stmtList = parser.parseStatementList();

            SQLStatement result = stmtList.get(0);
            Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
        }
    }

    public void testTruncateTable() {
        String sql = "ALTER table t1\n\ttruncate PARTITION p0, p1";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupReorgListPartition() {
        String sql =
            "ALTER TABLEGROUP grp1 REORGANIZE PARTITION p0, p1 into (partition p10 VALUES IN (1, 2, 3), partition p11 VALUES IN (4, 5, 6), partition p12 VALUES IN (7, 8, 9))";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupReorgRangePartition() {
        String sql =
            "ALTER TABLEGROUP grp1 REORGANIZE PARTITION p0, p2 into (partition p10 VALUES LESS THAN (1000), partition p11 VALUES LESS THAN (2000), partition p12 VALUES LESS THAN (3000))";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupRenamePartition() {
        String sql = "ALTER TABLEGROUP grp1 rename partition p0 to p00, p1 to p10";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableAddGSI() {
        String sql =
            "alter table t_order1\n\tadd global index g_i_seller1 (order_id) COVERING (`id`, `order_id`, `seller_id`) PARTITION BY HASH (order_id) PARTITIONS 4;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupAddPartition() {
        String sql =
            "alter tablegroup tg34 add partition (partition p2 values less than (30), partition p3 values less than (30)) ;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }

    public void testAlterTableGroupDropPartition() {
        String sql = "alter tablegroup tg34 drop partition p1,p2 ;";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(sql.toUpperCase().trim(), result.toString().toUpperCase().trim());
    }
}
