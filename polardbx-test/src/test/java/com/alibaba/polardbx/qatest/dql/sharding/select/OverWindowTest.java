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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author hongxi.chx
 */
public class OverWindowTest extends ReadBaseTestCase {

    private static final String TABLE_NAME = "drds_test_window";

    @BeforeClass
    public static void prepareData() throws Exception {
        String createSql =
            "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/CREATE TABLE if not exists `" + TABLE_NAME + "` (\n"
                + "\t`id1` int(10) DEFAULT NULL,\n"
                + "\t`id2` int(10) DEFAULT NULL,\n"
                + "\t`id3` int(10) DEFAULT NULL,\n"
                + "        KEY `auto_shard_key_id1` USING BTREE (`id1`)\n"
                + ") ";

        String sql = "delete from  " + TABLE_NAME;
        String insertSql = "insert into " + TABLE_NAME + " values "
            + "(NULL,NULL,2),"
            + "(NULL,1,2),"
            + "(0,1,1),"
            + "(0,1,null),"
            + "(0,2,null),"
            + "(1,1,null)";
        String dropTable = "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=false)*/drop table if exists " + TABLE_NAME;

        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            //drop
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            //create
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + " dbpartition by hash(`id1`)");
            //insert
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.executeUpdateSuccess(mysqlConnection, dropTable);

            JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);

            JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        }
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testUnbounded() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format("select *,sum(id2) over (partition by id1),sum(id3) over (partition by id1) from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRowSliding() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *,\n"
            + " sum(id2) over (partition by id1 order by id2 asc ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 order by id3 desc ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ow2 \n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRangeSliding() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *,\n"
            + " sum(id2) over (partition by id1 order by id2 asc RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 order by id3 desc RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) ow2 \n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRowUnboundedFollowing() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *,\n"
            + " sum(id2) over (partition by id1 ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) ow2\n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRangeUnboundedFollowing() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *,\n"
            + " sum(id2) over (partition by id1 order by id2 asc RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 order by id3 desc RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) ow2 \n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRowUnboundedPreceding() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select \n"
            + " *,\n"
            + " sum(id2) over (partition by id1 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) ow2 \n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testRangeUnboundedPreceding() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("select *,\n"
            + " sum(id2) over (partition by id1 order by id2 asc RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) ow1,\n"
            + " sum(id3) over (partition by id1 order by id3 desc RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) ow2 \n"
            + "from " + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoin() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select * from (select sum(id2) over (partition by id1 order by id2 RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) w1 from "
                + TABLE_NAME + ") a "
                + " join " + ExecuteTableSelect.selectBaseOneTable()[2][0] + " b on a.w1 = b.pk");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId2Desc() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id2) over (partition by id1 order by id2 desc range between current row and UNBOUNDED FOLLOWING) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId2Asc() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id2) over (partition by id1 order by id2 asc range between current row and UNBOUNDED FOLLOWING) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId3DescUnboundedFollowing() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id3) over (partition by id1 order by id2 desc range between current row and UNBOUNDED FOLLOWING) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId3AscUnboundedFollowing() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id3) over (partition by id1 order by id2 asc range between current row and UNBOUNDED FOLLOWING) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId2DescNoFrame() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id2) over (partition by id1 order by id2 desc) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId2AscNoFrame() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id2) over (partition by id1 order by id2 asc) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId3DescNoFrame() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id3) over (partition by id1 order by id2 desc) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumId3AscNoFrame() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id3) over (partition by id1 order by id2 asc) ow2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSum1EmptyOver() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id1) over () ow1 from  "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSum2EmptyOver() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id2) over () ow1 from  "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSum3EmptyOver() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id3) over () ow1 from  "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCurrentRowSumSumEmptyOver() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format(
            "/*+TDDL:master()*/select *, sum(id1) over () ow1,sum(id1) over () aw2 from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testWindowFunctionUseMultiTbForTypeConvert() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "select    min( mediumint_test  )OVER window_name  from select_base_four_one_db_multi_tb  where( char_test= 'adaabcwer')XOR ((  decimal_test not between 1000000  AND   10  ))   WINDOW window_name AS(PARTITION BY double_test,year_test order by   pk ROWS 1 PRECEDING         )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testWindowFunctionUseMultiTbForProjectWinRule() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "select    row_number( )OVER window_name  from select_base_four_one_db_multi_tb   where !(((64)    <=>   (integer_test) ))WINDOW window_name AS(         )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testWindowFunctionUseMultiTbForAggTogether() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql = "select char_test"
            + ", min( tinyint_test  ) OVER (PARTITION BY  smallint_test  order by pk   ROWS  BETWEEN 1 PRECEDING and 5 FOLLOWING )"
            + ", avg( tinyint_test ) "
            + " from select_base_four_one_db_multi_tb"
            + " where ( varchar_test= 'adaabcwer')"
            + " && (((integer_test)not between(mediumint_test)AND (tinyint_test)));";

        JdbcUtil
            .executeQueryFaied(tddlConnection, sql, "ERR_VALIDATE");
    }

    @Test
    public void testSumType1() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "/*+TDDL:master()*/select *,sum(decimal_test) over (PARTITION BY pk) from select_base_one_multi_db_one_tb t;";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSumType2() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "/*+TDDL:master()*/select *,sum(double_test) over (PARTITION BY pk) from select_base_one_multi_db_one_tb t;";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSumType3() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "/*+TDDL:master()*/select *,sum(integer_test) over (PARTITION BY pk) from select_base_one_multi_db_one_tb t;";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
    /**window function**/
    /**
     * @since 5.4.9
     */
    @Test
    public void testCumeDistAndPercentRank() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format(
                "select PERCENT_RANK() over (PARTITION BY id1 ORDER BY id2) 'percentRank',id1,id2,RANK() over (PARTITION BY id1 ORDER BY id2) 'rank', count(*) over (PARTITION BY id1 ORDER BY id2) 'countID1',CUME_DIST() over (PARTITION BY id1 ORDER BY id2) from "
                    + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testFirstLastValue() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format(
                "select *,first_value(id3) over (PARTITION BY id1 ORDER BY id2 ROWS BETWEEN 1 PRECEDING AND 1 following),last_value(id3) over (PARTITION BY id1 ORDER BY id2 ROWS BETWEEN 1 PRECEDING AND unbounded following),"
                    + "first_value(id3) over (PARTITION BY id1 ORDER BY id2 ROWS BETWEEN unbounded PRECEDING AND 1 following),last_value(id3) over (PARTITION BY id1 ORDER BY id2 ROWS BETWEEN 1 PRECEDING AND unbounded following) from "
                    + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLeadLagValue() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format("select *,lag(id2,2,'abc') over(order by id1,id2),lead(id2,2,'abc') over(order by id1,id2) from "
                + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testNThValue() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format(
                "/*+TDDL:master()*/select *,nth_value(id2,1) over (PARTITION BY id1 ORDER BY id2 desc),nth_value(id2,2) over (PARTITION BY id1 ORDER BY id2 desc),nth_value(id2,1) over (PARTITION BY id1 ORDER BY id2 desc ROWS BETWEEN 1 PRECEDING AND 1 following),nth_value(id2,2) over (PARTITION BY id1 ORDER BY id2 desc ROWS BETWEEN 1 PRECEDING AND 1 following) from "
                    + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testNTileValue() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String
            .format(
                "/*+TDDL:master()*/select *,ntile(4) over (PARTITION BY id1 ORDER BY id2 desc ROWS BETWEEN 1 PRECEDING AND 1 following),ntile(4) over (PARTITION BY id1 ORDER BY id2) from "
                    + TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
