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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

/**
 * Created by wenshao on 03/08/2017.
 */
public class SQLJoinTest extends TestCase {
    public void test_0() throws Exception {
        SQLSelectStatement stmt = (SQLSelectStatement)
                SQLUtils.parseStatements("select a.* from t_user a inner join t_group b on a.gid = b.id", JdbcConstants.MYSQL)
                .get(0);

        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        assertNotNull(queryBlock);

        SQLJoinTableSource join = (SQLJoinTableSource) queryBlock.getFrom();
        assertTrue(join.match("a", "b"));
    }
}
