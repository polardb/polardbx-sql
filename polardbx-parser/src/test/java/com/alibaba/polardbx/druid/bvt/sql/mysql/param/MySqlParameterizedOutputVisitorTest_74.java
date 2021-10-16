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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlParameterizedOutputVisitorTest_74 extends TestCase {
    public void test_in() throws Exception {

        String sql = "select 0 from corona_select_multi_db_one_tb where( 9 =( (3,4) not in ((1,2 ),( 3,5)) ) ) =bigint_test";

        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeInList,
                                                                   VisitorFeature.OutputParameterizedUnMergeShardingTable);
        assertEquals("SELECT ?\n" +
                "FROM corona_select_multi_db_one_tb\n" +
                "WHERE ? = ((?, ?) NOT IN ((?, ?), (?, ?))) = bigint_test", psql);

        assertEquals("[0,9,3,4,1,2,3,5]", JSON.toJSONString(outParameters));
    }

    public void test_between() throws Exception {

        String sql = "select 0 from corona_select_multi_db_one_tb where( 9 =( 3 not between 1 and 5 ) ) =bigint_test";

        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeInList,
                VisitorFeature.OutputParameterizedUnMergeShardingTable);
        assertEquals("SELECT ?\n" +
                "FROM corona_select_multi_db_one_tb\n" +
                "WHERE ? = (? NOT BETWEEN ? AND ?) = bigint_test", psql);

        assertEquals("[0,9,3,1,5]", JSON.toJSONString(outParameters));
    }
}
