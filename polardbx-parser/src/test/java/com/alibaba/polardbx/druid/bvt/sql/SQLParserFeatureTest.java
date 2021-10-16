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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;

public class SQLParserFeatureTest extends TestCase {
    public void test_simplify_column() throws Exception {
        String sql = " select tt ff fromm t";

        try {
            SQLStatement statement = SQLUtils.parseSingleStatement(sql, DbType.mysql, SQLParserFeature.PrintSQLWhileParsingFailed);
        } catch (Exception e) {
            assertEquals("syntax error, error in :'t ff fromm t, pos 19, line 1, column 15, token IDENTIFIER fromm, SQL :  select tt ff fromm t",
                    e.getMessage());
        }
    }
}
