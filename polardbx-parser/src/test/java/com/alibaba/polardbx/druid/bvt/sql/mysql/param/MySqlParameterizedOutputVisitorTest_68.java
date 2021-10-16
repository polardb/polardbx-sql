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
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlParameterizedOutputVisitorTest_68 extends TestCase {
    public void test_for_parameterize() throws Exception {

        String sql = "select id , name from xxx group by 1,2 order by 1;";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql);
        assertEquals("SELECT id, name\n" +
                "FROM xxx\n" +
                "GROUP BY 1, 2\n" +
                "ORDER BY 1;", psql);
        assertEquals(0, params.size());
    }

}
