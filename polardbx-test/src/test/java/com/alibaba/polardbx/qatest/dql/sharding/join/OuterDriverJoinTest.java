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

package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * test group join for : inner/left/right
 *
 * @author hongxi.chx
 */
public class OuterDriverJoinTest extends ReadBaseTestCase {

    public OuterDriverJoinTest() {
    }

    @Test
    public void rightJoinWithHint1LocalTest() {
        //cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_multi_db_multi_tb, select_base_one_one_db_one_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint2LocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_SORT_MERGE_JOIN=false,ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by m.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint1LocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_multi_db_multi_tb, select_base_one_one_db_one_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint2LocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_multi_db_multi_tb, select_base_one_one_db_one_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by s.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint1ParallelLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint2ParallelLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_SORT_MERGE_JOIN=false,ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by m.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint1ParallelLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_multi_db_multi_tb, select_base_one_one_db_one_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint2ParallelLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_multi_db_multi_tb, select_base_one_one_db_one_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by s.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint1ParallelWithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint2ParallelWithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_SORT_MERGE_JOIN=false,ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by m.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint1ParallelWithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint2ParallelWithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by s.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void rightJoinWithHint1WithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test

    public void rightJoinWithHint2WithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_SORT_MERGE_JOIN=false,ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s right join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by m.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint1WithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHint2WithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=false) HASH_OUTER_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test order by s.pk limit 10;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void forSubqueryWithLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_two_one_db_multi_tb, select_base_one_one_db_multi_tb)*/"
                + "select pk,integer_test,(select count(integer_test) from select_base_one_one_db_multi_tb where a.varchar_test = varchar_test ) from select_base_two_one_db_multi_tb a;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void forSubqueryWithLocalParallelTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_multi_tb, select_base_two_one_db_multi_tb)*/"
                + "select pk,integer_test,(select count(integer_test) from select_base_one_one_db_multi_tb where a.varchar_test = varchar_test ) from select_base_two_one_db_multi_tb a order by pk;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void forSubqueryWithoutLocalTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_multi_tb, select_base_two_one_db_multi_tb)*/"
                + "select pk,integer_test,(select count(integer_test) from select_base_one_one_db_multi_tb where a.varchar_test = varchar_test ) from select_base_two_one_db_multi_tb a;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void forSubqueryWithoutLocalParallelTest() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true,parallelism=1,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true) HASH_OUTER_JOIN(select_base_one_one_db_multi_tb, select_base_two_one_db_multi_tb)*/"
                + "select pk,integer_test,(select count(integer_test) from select_base_one_one_db_multi_tb where a.varchar_test = varchar_test ) from select_base_two_one_db_multi_tb a;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLeftJoin1() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true) HASH_OUTER_JOIN(select_base_two_multi_db_one_tb, select_base_four_multi_db_one_tb)*/"
                + "select * from ( select a.pk a,b.pk b,a.varchar_test av,b.varchar_test bv from select_base_two_multi_db_one_tb a left join select_base_four_multi_db_one_tb b on a.pk > b.pk and a.varchar_test = b.varchar_test and a.pk < 100) m order by m.a;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "build=" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testLeftJoin2() {
        ///*+TDDL: cmd_extra(ENABLE_HASH_JOIN=false,ENABLE_BKA_JOIN=false)*/select * from select_base_one_multi_db_multi_tb s left join select_base_one_one_db_one_tb m on s.integer_test = m.pk AND s.varchar_test;
        String sql =
            "/*+TDDL: cmd_extra(ENABLE_HASH_JOIN=true) HASH_JOIN(select_base_one_one_db_one_tb, select_base_one_multi_db_multi_tb)*/"
                + "select * from ( select a.pk a,b.pk b,a.varchar_test av,b.varchar_test bv from select_base_two_multi_db_one_tb a left join select_base_four_multi_db_one_tb b on a.pk > b.pk and a.varchar_test = b.varchar_test and a.pk < 100) m order by m.a;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
