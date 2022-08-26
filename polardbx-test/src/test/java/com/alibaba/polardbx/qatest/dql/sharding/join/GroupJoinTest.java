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
public class GroupJoinTest extends ReadBaseTestCase {

    public GroupJoinTest() {
    }

    @Test
    public void rightJoinWithHintTest() {
        String sql =
            "/*+TDDL:HASH_GROUP_JOIN(select_base_one_multi_db_multi_tb, select_base_one_multi_db_one_tb) cmd_extra(ENABLE_CBO_GROUP_JOIN=true,ENABLE_PUSH_JOIN=FALSE,ENABLE_JOIN_CLUSTERING=false,ENABLE_PUSH_AGG=false,ENABLE_CBO_PUSH_AGG=false,MPP_QUERY_NEED_RESERVE=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true)*/"
                + "SELECT"
                + "   c_count,"
                + "   count(*) AS custdist "
                + "FROM "
                + "(SELECT"
                + "         m.pk,"
                + "         count(s.integer_test) AS c_count"
                + "       FROM select_base_one_multi_db_multi_tb s "
                + "         right join select_base_one_multi_db_one_tb m"
                + " ON s.integer_test = m.pk AND s.varchar_test NOT LIKE '%s%'"
                + "       GROUP BY m.pk) a "
                + "GROUP BY c_count ORDER BY c_count DESC;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "GroupJoin\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoinWithHintTest() {
        String sql =
            "/*+TDDL:HASH_GROUP_JOIN(select_base_one_multi_db_multi_tb, select_base_one_multi_db_one_tb) cmd_extra(ENABLE_CBO_GROUP_JOIN=true,ENABLE_JOIN_CLUSTERING=false,ENABLE_PUSH_AGG=false,ENABLE_CBO_PUSH_AGG=false,MPP_QUERY_NEED_RESERVE=true,parallelism=8,ENABLE_MPP=false,ENABLE_LOCAL_MODE=true)*/"
                + "SELECT"
                + "         m.pk,"
                + "         count(s.integer_test) AS c_count"
                + "       FROM select_base_one_multi_db_multi_tb m left join select_base_one_multi_db_one_tb s"
                + " ON m.pk = s.integer_test AND s.varchar_test NOT LIKE '%s%'       GROUP BY m.pk;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "GroupJoin\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void leftJoin1Test() {
        String sql = "/*+TDDL:HASH_GROUP_JOIN(select_base_one_multi_db_multi_tb, select_base_one_multi_db_one_tb) */"
            + "SELECT"
            + "         m.pk,"
            + "         count(s.integer_test) AS c_count"
            + "       FROM select_base_one_multi_db_multi_tb m left join select_base_one_multi_db_one_tb s"
            + " ON m.pk = s.integer_test AND s.varchar_test NOT LIKE '%s%'       GROUP BY m.pk;";
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "GroupJoin\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
