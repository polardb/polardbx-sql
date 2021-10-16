/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Assert;

import java.util.List;

public class MySqlRevokeTest extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "REVOKE INSERT ON *.* FROM 'jeffrey'@'localhost';";

        
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
//        print(statementList);

        Assert.assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        for (SQLStatement stmt : statementList) {
            stmt.accept(visitor);
        }

        Assert.assertEquals(1, visitor.getTables().size());
        Assert.assertEquals(0, visitor.getColumns().size());
        Assert.assertEquals(0, visitor.getConditions().size());
        Assert.assertEquals(0, visitor.getOrderByColumns().size());
        
        {
            String output = SQLUtils.toSQLString(statementList, JdbcConstants.MYSQL);
            assertEquals("REVOKE INSERT ON *.* FROM 'jeffrey'@'localhost';", //
                                output);
        }
        {
            String output = SQLUtils.toSQLString(statementList, JdbcConstants.MYSQL, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            assertEquals("revoke INSERT on *.* from 'jeffrey'@'localhost';", //
                                output);
        }
    }

    public void test_1() throws Exception {
        String sql = "REVOKE SELECT ON TABLE * FROM 'ALIYUN$ads_user1@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE SELECT ON TABLE * FROM 'ALIYUN$ads_user1@aliyun.com'", //
                                output);
    }

    public void test_2() throws Exception {
        String sql = "REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'ALIYUN$ads_user1@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'ALIYUN$ads_user1@aliyun.com'", //
                                output);
    }

    public void test_3() throws Exception {
        String sql = "REVOKE grant option ON *.* FROM 'oa_2'@'localhost'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE GRANT OPTION ON *.* FROM 'oa_2'@'localhost'", //
                                output);
    }


}
