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

import com.alibaba.fastjson.JSON;
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

public class MySqlSelectTest_161 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT\n" +
                "DATE_FORMAT(FROM_UNIXTIME(`time` / 1000), '%Y-%m-%d %H:%i:%s'), `time`\n" +
                "FROM pvtz_day\n" +
                "ORDER BY `time` DESC\n" +
                "\n";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("SELECT DATE_FORMAT(FROM_UNIXTIME(`time` / 1000), '%Y-%m-%d %H:%i:%s')\n" +
                "\t, `time`\n" +
                "FROM pvtz_day\n" +
                "ORDER BY `time` DESC", stmt.toString());

        assertEquals("SELECT DATE_FORMAT(FROM_UNIXTIME(`time` / ?), '%Y-%m-%d %H:%i:%s')\n" +
                        "\t, `time`\n" +
                        "FROM pvtz_day\n" +
                        "ORDER BY `time` DESC"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

        List<Object> params = new ArrayList<Object>();
        assertEquals("SELECT DATE_FORMAT(FROM_UNIXTIME(`time` / ?), '%Y-%m-%d %H:%i:%s')\n" +
                        "\t, `time`\n" +
                        "FROM pvtz_day\n" +
                        "ORDER BY `time` DESC"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

        assertEquals(1, params.size());
        assertEquals("1000", JSON.toJSONString(params.get(0)));


    }

}