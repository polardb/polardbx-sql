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
 * @ClassName MySqlParameterizedOutputVisitorTest_79_group_asc_desc
 * @description
 * @Author zzy
 * @Date 2019/9/2 13:57
 */
public class MySqlParameterizedOutputVisitorTest_79_group_asc_desc extends TestCase {

    public void test_group_asc_desc() throws Exception {
        String sql = "select date_test+1 , int_test, id ,sum(double_test) from test_datatype_list group by 1 desc ,2,3 asc order by 4 desc,3 asc;";
        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT date_test + ?, int_test, id\n" +
                "\t, sum(double_test)\n" +
                "FROM test_datatype_list\n" +
                "GROUP BY 1 DESC, 2, 3 ASC\n" +
                "ORDER BY 4 DESC, 3 ASC;", psql);

        assertEquals("[1]", JSON.toJSONString(outParameters));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, DbType.mysql, outParameters);
        assertEquals("SELECT date_test + 1, int_test, id\n" +
                "\t, sum(double_test)\n" +
                "FROM test_datatype_list\n" +
                "GROUP BY 1 DESC, 2, 3 ASC\n" +
                "ORDER BY 4 DESC, 3 ASC;", rsql);
    }

}
