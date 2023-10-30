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

package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.cartesianProduct;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;


@FileStoreIgnore
public class StreamLookupJoinTest extends AutoReadBaseTestCase {

    String sql;

    @Parameterized.Parameters(name = "{index}:hint={0}, join={1}")
    public static List<Object[]> prepareDate() {

        List<String> hints = Lists.newArrayList(
            "/*+TDDL:cmd_extra(ENABLE_HASH_JOIN=false, ENABLE_NL_JOIN=false, ENABLE_SORT_MERGE_JOIN=false, RESUME_SCAN_STEP_SIZE=50)*/",
            "/*+TDDL:cmd_extra(ENABLE_HASH_JOIN=false, ENABLE_NL_JOIN=false, ENABLE_SORT_MERGE_JOIN=false, RESUME_SCAN_STEP_SIZE=500)*/",
            "/*+TDDL:cmd_extra(ENABLE_HASH_JOIN=false, ENABLE_NL_JOIN=false, ENABLE_SORT_MERGE_JOIN=false, RESUME_SCAN_STEP_SIZE=8)*/",
            "/*+TDDL:cmd_extra(ENABLE_HASH_JOIN=false, ENABLE_NL_JOIN=false, ENABLE_SORT_MERGE_JOIN=false, RESUME_SCAN_STEP_SIZE=5)*/",
            "/*+TDDL:ENABLE_SORT_MERGE_JOIN=false ENABLE_BKA_JOIN=false ENABLE_NL_JOIN=false RESUME_SCAN_STEP_SIZE=5*/",
            "/*+TDDL:RESUME_SCAN_STEP_SIZE=5 ENABLE_HASH_JOIN=false ENABLE_SORT_MERGE_JOIN=false ENABLE_BKA_JOIN=false*/"
        );

        List<String> sqls = Lists.newArrayList(
            //one BkaJoin
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk;",
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by two.pk limit 10;",
            //one BkaJoin, and the outer input is empty
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk and one.pk > 1000",
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk and one.pk > 1000 order by two.pk limit 10;",
            //one BkaJoin, and the inner input is empty
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk and two.pk > 1000;",
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk and two.pk > 1000 order by two.pk limit 10;",
            //one BkaJoin with order by
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk;",
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk limit 10;",
            //one BkaJoin with order by limit
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk limit 100;",
            //one BkaJoin with order by offset fetch
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk limit 50,100;",
            //two BkaJoin
            "select * from (select one.* from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk) T inner join select_base_three_multi_db_one_tb three on T.pk=three.pk;",
            "select * from (select one.* from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk) T inner join select_base_three_multi_db_one_tb three on T.pk=three.pk order by three.pk limit 10;",
            //two BkaJoin with order by
            "select * from (select one.* from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk limit 50,100 ) T inner join select_base_three_multi_db_one_tb three on T.pk=three.pk;",
            "select * from (select one.* from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk order by one.pk limit 50,100 ) T inner join select_base_three_multi_db_one_tb three on T.pk=three.pk order by three.pk limit 10;",
            // BkaJoin with for update
            "select * from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk for update;",
            "select * from (select one.* from select_base_one_multi_db_multi_tb one inner join select_base_two_one_db_multi_tb two on one.pk=two.pk) T inner join select_base_three_multi_db_one_tb three on T.pk=three.pk for update;"
        );
        return cartesianProduct(hints.toArray(), sqls.toArray());
    }

    public StreamLookupJoinTest(String hint, String sql) {
        this.hint = hint;
        this.sql = sql;
    }

    @Test
    public void testLookupJoin() {
        selectContentSameAssertWithDiffSql(hint + sql, sql, null,
            mysqlConnection, tddlConnection, true, false, false);
    }
}
