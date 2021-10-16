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

public class MySqlParameterizedOutputVisitorTest_67 extends TestCase {
    public void test_for_parameterize() throws Exception {

        String sql = "select dep_id, dep_name, count(1) from t where dep_tpe = 'aa' group by dep_id having count(1) > 10";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql);
        assertEquals("SELECT dep_id, dep_name, count(1)\n" +
                "FROM t\n" +
                "WHERE dep_tpe = ?\n" +
                "GROUP BY dep_id\n" +
                "HAVING count(1) > ?", psql);
        assertEquals(2, params.size());
        assertEquals("\"aa\"", JSON.toJSONString(params.get(0)));
        assertEquals("10", JSON.toJSONString(params.get(1)));

        String rsql = ParameterizedOutputVisitorUtils.restore(psql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT dep_id, dep_name, count(1)\n" +
                "FROM t\n" +
                "WHERE dep_tpe = 'aa'\n" +
                "GROUP BY dep_id\n" +
                "HAVING count(1) > 10", rsql);
    }

}
