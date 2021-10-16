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

/**
 * Created by wenshao on 16/9/23.
 */
public class MySqlParameterizedOutputVisitorTest_63 extends TestCase {
    public void test_for_parameterize() throws Exception {

        String sql = "select * from abc where id in (null)";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedUnMergeShardingTable);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (?)", psql);
        assertEquals(1, params.size());
        assertEquals("null", JSON.toJSONString(params.get(0)));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (NULL)", rsql);
    }

    public void test_for_parameterize_1() throws Exception {

        String sql = "select * from abc where id in (1,2,3,4) and id2 in (5,6,7)";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedQuesUnMergeInList);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (?, ?, ?, ?)\n" +
                "\tAND id2 IN (?, ?, ?)", psql);
        assertEquals(7, params.size());
        assertEquals("1", JSON.toJSONString(params.get(0)));

        String rsql = ParameterizedOutputVisitorUtils.restore(psql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (1, 2, 3, 4)\n" +
                "\tAND id2 IN (5, 6, 7)", rsql);
    }

    public void test_for_parameterize_2() throws Exception {

        String sql = "select * from abc where id in (null, null)";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedUnMergeShardingTable);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (?)", psql);
        assertEquals(2, params.size());
        assertEquals("null", JSON.toJSONString(params.get(0)));
        assertEquals("null", JSON.toJSONString(params.get(1)));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT *\n" +
                "FROM abc\n" +
                "WHERE id IN (NULL, NULL)", rsql);
    }
}
