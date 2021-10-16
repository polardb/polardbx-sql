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
import org.apache.calcite.sql.SqlUpdate;
import org.junit.Assert;

import java.util.List;

/**
 * @author lijun.cailj 2017/11/22
 */
public class CalciteUpdateVisitorTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "UPDATE t_price " + //
            "SET purchasePrice = 12.3, operaterId = 234, " + //
            "    operaterRealName = 'calijun', operateDateline = 'deifje' " + //
            "WHERE goodsId = 456";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
        stmt.accept(visitor);

        SqlUpdate sqlNode = (SqlUpdate) visitor.getSqlNode();

        System.out.println(sqlNode);

        Assert.assertEquals(sqlNode.toString(), "UPDATE `t_price` SET `purchasePrice` = 12.3\n"
            + ", `operaterId` = 234\n" + ", `operaterRealName` = 'calijun'\n"
            + ", `operateDateline` = 'deifje'\n" + "WHERE (`goodsId` = 456)");

    }

    public void test_1() throws Exception {
        String sql = "UPDATE t_price p " + //
            "SET purchasePrice = 12.3, operaterId = 234, " + //
            "    operaterRealName = 'calijun', operateDateline = 'deifje' " + //
            "WHERE goodsId = 456";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statemen = statementList.get(0);

        FastSqlToCalciteNodeVisitor visitor =
            new FastSqlToCalciteNodeVisitor(new ContextParameters(false), new ExecutionContext());
        statemen.accept(visitor);

        SqlUpdate sqlNode = (SqlUpdate) visitor.getSqlNode();

        System.out.println(sqlNode);

        Assert.assertEquals(sqlNode.toString(), "UPDATE `t_price` AS `p` SET `purchasePrice` = 12.3\n"
            + ", `operaterId` = 234\n" + ", `operaterRealName` = 'calijun'\n"
            + ", `operateDateline` = 'deifje'\n" + "WHERE (`goodsId` = 456)");

    }

}
