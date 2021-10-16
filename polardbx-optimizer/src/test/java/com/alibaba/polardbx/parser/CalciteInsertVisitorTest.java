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

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlInsert;
import org.junit.Assert;

import java.util.List;

/**
 * @author lijun.cailj 2017/11/22
 */
public class CalciteInsertVisitorTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "INSERT INTO tbl_name (col1,col2) VALUES(15,col1*2)";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statemen = statementList.get(0);

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
        statemen.accept(visitor);

        SqlInsert sqlNode = (SqlInsert) visitor.getSqlNode();

        System.out.println(sqlNode);

        Assert.assertEquals(sqlNode.toString(), "INSERT\n"
            + "INTO `tbl_name` (`col1`, `col2`)\n"
            + "VALUES(15, (`col1` * 2))");

    }

}
