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
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 * @ClassName MySqlParameterizedOutputVisitorTest_80_str_to_date
 * @description
 * @Author zzy
 * @Date 2019/12/3 16:58
 */
public class MySqlParameterizedOutputVisitorTest_80_str_to_date extends TestCase {

    public void test_0() throws Exception {
        String sql = "select str_to_date('2019-10-01 10:23:01', '%Y-%m-%d %H:%i:%s');";
        List<Object> outParameters = new ArrayList<Object>();

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT str_to_date(?, '%Y-%m-%d %H:%i:%s');", psql);

        assertEquals("[\"2019-10-01 10:23:01\"]", JSON.toJSONString(outParameters));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, DbType.mysql, outParameters);
        assertEquals("SELECT str_to_date('2019-10-01 10:23:01', '%Y-%m-%d %H:%i:%s');", rsql);
    }

    public void test_1() throws Exception {
        String sql = "select str_to_date('10:23:01', '%H:%i:%s');";
        List<Object> outParameters = new ArrayList<Object>();

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT str_to_date(?, '%H:%i:%s');", psql);

        assertEquals("[\"10:23:01\"]", JSON.toJSONString(outParameters));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, DbType.mysql, outParameters);
        assertEquals("SELECT str_to_date('10:23:01', '%H:%i:%s');", rsql);
    }

}
