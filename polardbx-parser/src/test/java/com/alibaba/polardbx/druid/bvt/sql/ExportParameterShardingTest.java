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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 23/11/2016.
 */
public class ExportParameterShardingTest extends TestCase {
    DbType dbType = JdbcConstants.MYSQL;

    public void test_exportParameter() throws Exception {
        String sql = "select * from t_user_0000 where oid = 1001";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        List<Object> parameters = visitor.getParameters();
        //visitor.setParameters(parameters);

        stmt.accept(visitor);

        System.out.println(out);
        System.out.println(JSON.toJSONString(parameters));

        String restoredSql = restore(out.toString(), parameters);
        assertEquals("SELECT *\n" +
                "FROM t_user_0000\n" +
                "WHERE oid = 1001", restoredSql);
    }

    public String restore(String sql, List<Object> parameters) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.setInputParameters(parameters);

        visitor.addTableMapping("t_user", "t_user_0000");

        stmt.accept(visitor);

        return out.toString();
    }
}
