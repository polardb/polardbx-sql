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
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_57 extends TestCase {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;
        String sql = "select `ktv_resource`.`VERSION` from `ktv_resource_0118` `ktv_resource` " +
                "where ((`ktv_resource`.`BUYER_ID` = 736485494) " +
                "   AND (`ktv_resource`.`STATUS` = 1) " +
                "   AND (`ktv_resource`.`START_TIME` <= '2017-10-24 00:27:21.839') " +
                "   AND (`ktv_resource`.`END_TIME` >= '2017-10-24 00:27:21.839') " +
                "   AND (`ktv_resource`.`seller_id` = 2933220011) " +
                "   AND (`ktv_resource`.`AVAILABLE_COUNT` IS NULL " +
                "       OR (`ktv_resource`.`AVAILABLE_COUNT` > 0) " +
                "       OR (`ktv_resource`.`AVAILABLE_COUNT` = -1))" +
                ") limit 0,20";

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType, SQLParserFeature.EnableSQLBinaryOpExprGroup);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLStatement stmt = stmtList.get(0);

        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, JdbcConstants.MYSQL);
        List<Object> parameters = new ArrayList<Object>();
        visitor.setParameterized(true);
        visitor.setParameterizedMergeInList(true);
        visitor.setParameters(parameters);
        /*visitor.setPrettyFormat(false);*/
        stmt.accept(visitor);
       /* JSONArray array = new JSONArray();
        for(String table : visitor.getTables()){
            array.add(table.replaceAll("`",""));
        }*/

        String psql = out.toString();

        System.out.println(psql);




        assertEquals("SELECT `ktv_resource`.`VERSION`\n" +
                "FROM ktv_resource `ktv_resource`\n" +
                "WHERE `ktv_resource`.`BUYER_ID` = ?\n" +
                "\tAND `ktv_resource`.`STATUS` = ?\n" +
                "\tAND `ktv_resource`.`START_TIME` <= ?\n" +
                "\tAND `ktv_resource`.`END_TIME` >= ?\n" +
                "\tAND `ktv_resource`.`seller_id` = ?\n" +
                "\tAND (`ktv_resource`.`AVAILABLE_COUNT` IS NULL\n" +
                "\t\tOR `ktv_resource`.`AVAILABLE_COUNT` > ?\n" +
                "\t\tOR `ktv_resource`.`AVAILABLE_COUNT` = ?)\n" +
                "LIMIT ?, ?", psql);

        String rsql = SQLUtils.format(psql, JdbcConstants.MYSQL, parameters);
        System.out.println(rsql);
    }
}
