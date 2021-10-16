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
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_150 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "(select __aid\n" +
                "  from unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                " where unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type in ('test1'))\n" +
                " \n" +
                " union\n" +
                "\n" +
                "(select __aid\n" +
                "  from unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                " where unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type in ('test'))\n" +
                "\n" +
                "MINUS\n" +
                "(\n" +
                "select __aid\n" +
                "  from unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                " where unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type in ('8', '1')\n" +
                "    )";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("(SELECT __aid\n" +
                "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN ('test1'))\n" +
                "UNION\n" +
                "(SELECT __aid\n" +
                "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN ('test'))\n" +
                "MINUS\n" +
                "(SELECT __aid\n" +
                "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN ('8', '1'))", stmt.toString());

        assertEquals("(SELECT __aid\n" +
                        "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                        "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN (?))\n" +
                        "UNION\n" +
                        "(SELECT __aid\n" +
                        "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                        "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN (?))\n" +
                        "MINUS\n" +
                        "(SELECT __aid\n" +
                        "FROM unidesk_ads.dmj_ex_1_unidesk_tag_all\n" +
                        "WHERE unidesk_ads.dmj_ex_1_unidesk_tag_all.pred_career_type IN (?))"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));


    }

}