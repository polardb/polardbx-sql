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

package com.alibaba.polardbx.druid.bvt.bug;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

public class Issue2876 extends TestCase {
    public void test_0() throws Exception {
        String sql = "SELECT CONCAT(\"'\",b.PRIMARY_ID,\"'\") \n" +
                "FROM s_user_session_attributes a \n" +
                "LEFT JOIN s_user_session b ON a.SESSION_PRIMARY_ID=b.PRIMARY_ID \n" +
                "WHERE a.ATTRIBUTE_NAME='KAPTCHA_SESSION_KEY' AND b.LAST_ACCESS_TIME <= 1540429945459";

        System.out.println(sql);

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        SQLJoinTableSource joinTableSource = (SQLJoinTableSource) queryBlock.getFrom();

        assertEquals("a", joinTableSource.getLeft().getAlias());
        assertEquals("b", joinTableSource.getRight().getAlias());
    }
}
