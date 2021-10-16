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

import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.druid.sql.visitor.VisitorFeature.OutputParameterizedQuesUnMergeInList;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_60_in extends TestCase {
    public void test_for_parameterize() throws Exception {

        String sql = "select * from a where id in (1, 2,3)";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT *\n" +
                "FROM a\n" +
                "WHERE id IN (?)", psql);
        assertEquals(3, params.size());
        assertEquals(1, params.get(0));
        assertEquals(2, params.get(1));
        assertEquals(3, params.get(2));
    }

    public void test_for_parameterize_1() throws Exception {

        String sql = "select * from a where (id,userId) in ((1,2), (2,3),(3,4))";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT *\n" +
                "FROM a\n" +
                "WHERE (id, userId) IN (?)", psql);
        assertEquals(3, params.size());
        assertEquals("[1, 2]", params.get(0).toString());
        assertEquals("[2, 3]", params.get(1).toString());
        assertEquals("[3, 4]", params.get(2).toString());
    }

    public void test_for_parameterize_2() throws Exception {

        String sql = "select * from a where (id,userId) in ((1,2), (2,3),(3,4))";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql,
                JdbcConstants.MYSQL,
                params,
                OutputParameterizedQuesUnMergeInList);
        assertEquals("SELECT *\n" + "FROM a\n" + "WHERE (id, userId) IN ((?, ?), (?, ?), (?, ?))", psql);
        assertEquals(6, params.size());
        assertEquals("1", params.get(0).toString());
        assertEquals("2", params.get(1).toString());
        assertEquals("2", params.get(2).toString());
        assertEquals("3", params.get(3).toString());
        assertEquals("3", params.get(4).toString());
        assertEquals("4", params.get(5).toString());
    }

    public void test_for_parameterize_3() throws Exception {

        String sql = "select * from a where (id,userId) in ((1,2))";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, OutputParameterizedQuesUnMergeInList);
        assertEquals("SELECT *\n" +
                     "FROM a\n" +
                     "WHERE (id, userId) IN ((?, ?))", psql);
        assertEquals(2, params.size());
        assertEquals("1", params.get(0).toString());
        assertEquals("2", params.get(1).toString());
    }
}
