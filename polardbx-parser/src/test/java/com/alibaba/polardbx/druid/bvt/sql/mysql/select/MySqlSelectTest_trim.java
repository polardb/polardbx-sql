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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.ArrayList;
import java.util.List;

public class MySqlSelectTest_trim extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select trim('x' from 'xxdxx')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        //assertEquals("SELECT trim('x')", stmt.toString());


        List<Object> outParameters = new ArrayList<Object>();
        String parameterizeSql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters,
                VisitorFeature.OutputParameterizedQuesUnMergeInList,
                VisitorFeature.OutputParameterizedUnMergeShardingTable,
                VisitorFeature.OutputParameterizedQuesUnMergeValuesList);

        System.out.println(parameterizeSql);
        System.out.println(outParameters);

        sql = "select trim('x')";
        statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        //assertEquals("SELECT trim('x')", stmt.toString());


        outParameters = new ArrayList<Object>();
        parameterizeSql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters,
                VisitorFeature.OutputParameterizedQuesUnMergeInList,
                VisitorFeature.OutputParameterizedUnMergeShardingTable,
                VisitorFeature.OutputParameterizedQuesUnMergeValuesList);

        System.out.println(parameterizeSql);
        System.out.println(outParameters);
        sql = "select trim(TRAILING 'x' from 'xxxxxxxdxx')";
        statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        //assertEquals("SELECT trim('x')", stmt.toString());


        outParameters = new ArrayList<Object>();
        parameterizeSql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters,
                VisitorFeature.OutputParameterizedQuesUnMergeInList,
                VisitorFeature.OutputParameterizedUnMergeShardingTable,
                VisitorFeature.OutputParameterizedQuesUnMergeValuesList);

        System.out.println(parameterizeSql);
        System.out.println(outParameters);
    }

}