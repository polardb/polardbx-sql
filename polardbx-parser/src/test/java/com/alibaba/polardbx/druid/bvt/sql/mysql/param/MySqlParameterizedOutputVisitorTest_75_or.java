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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlParameterizedOutputVisitorTest_75_or extends TestCase {
    public void test_or() throws Exception {

        String sql = "select * from t1 where id = 1 or id = 2 or id = 3";

        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT *\n" +
                "FROM t1\n" +
                "WHERE id = ?\n" +
                "\tOR id = ?\n" +
                "\tOR id = ?", psql);

        assertEquals("[1,2,3]", JSON.toJSONString(outParameters));
    }

    public void test_or_2() throws Exception {

        String sql = "select * from t1 where id = 1 or id = 2 or id = 3";

        List<Object> outParameters = new ArrayList<Object>(0);

        SQLStatement stmt = SQLUtils.parseStatements(sql, DbType.mysql).get(0);

        StringBuilder out = new StringBuilder(sql.length());
        ParameterizedVisitor visitor = ParameterizedOutputVisitorUtils.createParameterizedOutputVisitor(out, DbType.mysql);
        visitor.config(VisitorFeature.OutputParameterizedQuesUnMergeOr, true);
        if (outParameters != null) {
            visitor.setOutputParameters(outParameters);
        }
        stmt.accept(visitor);

        String psql = out.toString();
        assertEquals("SELECT *\n" +
                "FROM t1\n" +
                "WHERE id = ?\n" +
                "\tOR id = ?\n" +
                "\tOR id = ?", psql);

        assertEquals("[1,2,3]", JSON.toJSONString(outParameters));
    }
}
