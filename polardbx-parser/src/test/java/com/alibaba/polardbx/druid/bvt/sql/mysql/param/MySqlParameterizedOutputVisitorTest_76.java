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

public class MySqlParameterizedOutputVisitorTest_76 extends TestCase {
    public void test_or() throws Exception {

        String sql = "select * from select_base_one_one_db_multi_tb where pk>=7 and pk>4 and pk <=49 and pk<18 order by pk limit 1";

        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT *\n" +
                "FROM select_base_one_one_db_multi_tb\n" +
                "WHERE pk >= ?\n" +
                "\tAND pk > ?\n" +
                "\tAND pk <= ?\n" +
                "\tAND pk < ?\n" +
                "ORDER BY pk\n" +
                "LIMIT ?", psql);

        assertEquals("[7,4,49,18,1]", JSON.toJSONString(outParameters));
    }

    public void test_and() throws Exception {
        String sql = "select * from select_base_one_one_db_multi_tb where pk=1 and pk=4 and pk=49 and pk=18 order by pk limit 1";

        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeAnd);
        assertEquals("SELECT *\n" +
                "FROM select_base_one_one_db_multi_tb\n" +
                "WHERE pk = ?\n" +
                "\tAND pk = ?\n" +
                "\tAND pk = ?\n" +
                "\tAND pk = ?\n" +
                "ORDER BY pk\n" +
                "LIMIT ?", psql);

        assertEquals("[1,4,49,18,1]", JSON.toJSONString(outParameters));
    }

}
