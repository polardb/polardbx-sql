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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhoamiStatement;
import junit.framework.TestCase;

import java.util.List;

public class WhoamiTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "who am i";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);
        assertEquals(1, stmtList.size());

        SQLWhoamiStatement whoami = (SQLWhoamiStatement) stmtList.get(0);
        assertEquals("WHO AM I", whoami.toString());
        assertEquals("who am i", whoami.toLowerCaseString());
    }
}
