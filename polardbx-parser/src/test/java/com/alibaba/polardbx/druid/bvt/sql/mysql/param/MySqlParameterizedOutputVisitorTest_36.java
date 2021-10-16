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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_36 extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;


        String sql = "INSERT INTO v_j (jb) VALUES (0x7801848fbd6ec23014465f057986c4ffc69150972e0c1dd9bc5cdbd7c56d8028762221c4bbd72addbb1e9def48df832c399281ecb9a4865345c996ac39e2ed743abe376e434cda0a8b7b26920908c6701e64a45a29c3c137bdde272403fbdbb5cdc391922b1ea323c3866d37ee55fc88aa01f76fd29167ab0618470fe1fb348f2d79ae751afafe8cf7dcc11273dd4108584a1731c132d60ec6ecc1c32e5f43b7e6a9e0bce2dcafac87a5de7e0fcd589af856f3054b85cb7460d20a6194e4ca2d9472fd75f3391e446c3c498d7b11a5e7009c098d946a4941614a2fb7e4cfeb01020fe0ad899602a34c09ce2c9388497903d26bf2fc090000ffff01196b1f)";



        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement statement = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        //  List<Object> parameters = new ArrayList<Object>();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, JdbcConstants.MYSQL);
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        //   visitor.setParameters(parameters);
        visitor.setExportTables(true);
        visitor.setPrettyFormat(false);
        statement.accept(visitor);
        assertEquals("INSERT INTO v_j (jb) VALUES (?)", out.toString());
    }
}
