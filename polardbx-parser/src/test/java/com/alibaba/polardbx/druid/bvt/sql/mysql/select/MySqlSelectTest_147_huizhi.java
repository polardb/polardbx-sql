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
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_147_huizhi extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "INSERT INTO hz_dev_hb.tb_tmp_cda_opera_281c (target, appeartimes, source_id) SELECT\n" +
                "           VARCHAR20,\n" +
                "           count(1)          AS appeartimes,\n" +
                "           'resource_count1' as source_id\n" +
                "         FROM hz_dev_hb.tb_fxzx_large t1\n" +
                "         WHERE 1 = 1 AND VARCHAR20 IS NOT NULL AND\n" +
                "               MISSIONID =\n" +
                "               'd2051b6549d9a028e83a8a9ab2c2'\n" +
                "         GROUP BY VARCHAR20";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLInsertStatement stmt = (SQLInsertStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("INSERT INTO hz_dev_hb.tb_tmp_cda_opera_281c (target, appeartimes, source_id)\n" +
                "SELECT VARCHAR20, count(1) AS appeartimes, 'resource_count1' AS source_id\n" +
                "FROM hz_dev_hb.tb_fxzx_large t1\n" +
                "WHERE 1 = 1\n" +
                "\tAND VARCHAR20 IS NOT NULL\n" +
                "\tAND MISSIONID = 'd2051b6549d9a028e83a8a9ab2c2'\n" +
                "GROUP BY VARCHAR20", stmt.toString());

        assertEquals("INSERT INTO hz_dev_hb.tb_tmp_cda_opera_281c(target, appeartimes, source_id)\n" +
                        "SELECT VARCHAR20, count(1) AS appeartimes, ? AS source_id\n" +
                        "FROM hz_dev_hb.tb_fxzx_large t1\n" +
                        "WHERE 1 = 1\n" +
                        "\tAND VARCHAR20 IS NOT NULL\n" +
                        "\tAND MISSIONID = ?\n" +
                        "GROUP BY VARCHAR20"
                , ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

        SQLSelectQueryBlock queryBlock = stmt.getQuery().getQueryBlock();
        assertEquals(3, queryBlock.getSelectList().size());
        assertEquals(SQLCharExpr.class, queryBlock.getSelectList().get(2).getExpr().getClass());
    }

}