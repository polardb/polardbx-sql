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

import com.alibaba.polardbx.druid.sql.PagerUtils;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

public class PagerUtilsTest_Limit_mysql_question_placeholder extends TestCase {

    public void  testQuestionLimitPlaceholder1(){
        String sql = "select * from test_table limit ?";
        testQuestionLimitPlaceholderInternal(sql);
    }

    public void  testQuestionLimitPlaceholder2(){
        String sql = "select * from test_table limit ?, ?";
        testQuestionLimitPlaceholderInternal(sql);
    }

    private void testQuestionLimitPlaceholderInternal(String sql){
        List<SQLStatement> statements;
        try{
            statements = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        }catch(ParserException e){
            Assert.fail(e.getMessage());
            return;
        }
        if (statements == null || statements.size() == 0){
            Assert.fail("no sql found!");
            return;
        }
        if (statements.size() != 1) {
            Assert.fail("sql not support count : " + sql);
            return;
        }
        SQLSelectStatement statement = (SQLSelectStatement)statements.get(0);
        if (!(statement instanceof SQLSelectStatement)) {
            Assert.fail("sql not support count : " + sql);
            return;
        }
        SQLSelect select = statement.getSelect();
        PagerUtils.limit(select, JdbcConstants.MYSQL, 0, 200, true);
        SQLUtils.FormatOption options = new SQLUtils.FormatOption();
        options.setPrettyFormat(false);
        options.setUppCase(false);
        assertEquals(sql, SQLUtils.toSQLString(select, JdbcConstants.MYSQL, options));
    }

}
