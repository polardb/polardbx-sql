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

package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class UseInsertColumnsCacheTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "insert into tc_biz_mytable (f1, f2,   f3, f4, f5, f6, f7, f8) values (1, 2, 3, 4, 5, 6, 7, 8)";

        SQLParserFeature[] features = {SQLParserFeature.EnableSQLBinaryOpExprGroup
                , SQLParserFeature.OptimizedForParameterized
                , SQLParserFeature.UseInsertColumnsCache
        };

        String psql1 = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL);
        String psql2 = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL);
        assertEquals("INSERT INTO tc_biz_mytable (f1, f2, f3, f4, f5\n" +
                "\t, f6, f7, f8)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?)", psql1);
        assertEquals("INSERT INTO tc_biz_mytable (f1, f2, f3, f4, f5\n" +
                "\t, f6, f7, f8)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?)", psql2);
    }
}
