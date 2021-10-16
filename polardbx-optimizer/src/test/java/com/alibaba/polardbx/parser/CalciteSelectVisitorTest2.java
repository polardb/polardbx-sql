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

package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;

public class CalciteSelectVisitorTest2 extends TestCase {
    public void test_for_calcite() throws Exception {
        String sql = "select * from elastic where not foo = 1";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
        stmt.accept(visitor);

        SqlSelect sqlNode = (SqlSelect) visitor.getSqlNode();

        SqlNode sqlNode1 = SqlParser.create(sql).parseQuery();
        System.out.println(sqlNode1);

        assertEquals(FnvHash.hashCode64(sqlNode1.toString())
            , FnvHash.hashCode64(sqlNode.toString()));
    }
}
