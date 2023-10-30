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
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 23/11/2016.
 */
public class ExportParameterInTest extends TestCase {
    DbType dbType = JdbcConstants.MYSQL;

    public void test_exportParameter() throws Exception {
        String sql = "select * from t_user where oid = '102' and uid in (1, 2, 3)";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        List<Object> parameters = new ArrayList<Object>();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        visitor.setParameters(parameters);

        stmt.accept(visitor);

        System.out.println(out);
        System.out.println(JSON.toJSONString(parameters));

        restore(out.toString(), parameters);
    }

    public void testExportHexParameter() throws Exception {
        String sql = "select * from t_user where (blob1, blob2) in ((x'626C6F62206461746134',x'01'))";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        List<Object> parameters = new ArrayList<Object>();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        visitor.setParameters(parameters);

        stmt.accept(visitor);

        System.out.println(out);
        System.out.println(JSON.toJSONString(parameters));
        Assert.assertEquals("[[[\"YmxvYiBkYXRhNA==\",\"AQ==\"]]]", JSON.toJSONString(parameters));

        restore(out.toString(), parameters);
    }

    public void restore(String sql, List<Object> parameters) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, dbType);
        visitor.setParameters(parameters);
        stmt.accept(visitor);

        System.out.println(out);
    }
}
