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
import com.alibaba.polardbx.druid.util.JdbcConstants;

public class MySqlRevokeTest_ads extends MysqlTest {


    public void test_1() throws Exception {
        String sql = "REVOKE SELECT ON TABLE * FROM 'ALIYUN$ads_user1@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE SELECT ON TABLE * FROM 'ALIYUN$ads_user1@aliyun.com'", //
                                output);
    }

    public void test_2() throws Exception {
        String sql = "REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'ALIYUN$ads_user1@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'ALIYUN$ads_user1@aliyun.com'", //
                                output);
    }

    public void test_3() throws Exception {
        String sql = "REVOKE LOAD DATA, DUMP DATA, SELECT ON TABLE DB1.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE LOAD DATA, DUMP DATA, SELECT ON TABLE DB1.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'", //
                                output);
    }

    public void test_4() throws Exception {
        String sql = "REVOKE SHOW DATABASES, SUPER ON SYSTEM * FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE SHOW DATABASES, SUPER ON SYSTEM * FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'", //
                                output);
    }

    public void test_5() throws Exception {
        String sql = "REVOKE SHOW DATABASES, SUPER ON *.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE SHOW DATABASES, SUPER ON *.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'", //
                                output);
    }

    public void test_6() throws Exception {
        String sql = "REVOKE INSERT, DELETE, UPDATE ON *.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE INSERT, DELETE, UPDATE ON *.* FROM 'ALIYUN$ads_user1@aliyun.com', 'ALIYUN$ads_user2@aliyun.com'", //
                                output);
    }

    public void test_7() throws Exception {
        String sql = "REVOKE INSERT, DELETE, UPDATE ON *.* FROM 'ALIYUN$ads_user1@aliyun.com'@'192.168.1.2', 'ALIYUN$ads_user2@aliyun.com'@'192.168.1/20'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE INSERT, DELETE, UPDATE ON *.* FROM 'ALIYUN$ads_user1@aliyun.com'@'192.168.1.2', 'ALIYUN$ads_user2@aliyun.com'@'192.168.1/20'", //
                                output);
    }

    public void test_8() throws Exception {
        String sql = "REVOKE INSERT, DELETE, UPDATE, SELECT(C1, C2) ON *.* FROM 'ALIYUN$ads_user1@aliyun.com'@'192.168.1.2', 'ALIYUN$ads_user2@aliyun.com'@'192.168.1/20'";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        String output = SQLUtils.toSQLString(stmt, JdbcConstants.MYSQL);
        assertEquals("REVOKE INSERT, DELETE, UPDATE, SELECT(C1, C2) ON *.* FROM 'ALIYUN$ads_user1@aliyun.com'@'192.168.1.2', 'ALIYUN$ads_user2@aliyun.com'@'192.168.1/20'", //
                                output);
    }


}
