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

package com.alibaba.polardbx.druid.bvt.sql.mysql.locality;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LocalityRuleTest {

    @Test
    public void testPartitionLocality() {
        List<String> sqlList = Arrays.asList(
            "create database d1 locality = 'hehe' ",
            "show create database d1",

            "create table t1(id int) locality = 'hehe'",
            "create table t1(id int) locality 'hehe'",
            "create table t1(id int) partition by range columns (id) locality = 'hehe' ",
            "create table t1(id int) partition by range columns (id) (" +
                " partition p0 values less than (10) locality 'hehe', " +
                " partition p1 values less than (20) locality 'hehe' " +
            ")",

            "create tablegroup tg1 locality='hehe'",
            "create tablegroup tg1 locality 'hehe'"
            );

        for (String sql : sqlList) {
            List<SQLStatement> stmtList = null;
            try {
                 stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);
            } catch (Exception e) {
                Assert.fail(String.format("parse SQL failed: %s , exception is %s", sql, e));
            }
            SQLStatement stmt = stmtList.get(0);
            Assert.assertEquals(normalizeSQL(sql), normalizeSQL(SQLUtils.toMySqlString(stmt)));
        }
    }

    @Test
    public void testPrimaryZone() {
        List<String> sqlList = Arrays.asList(
            "ALTER SYSTEM SET CONFIG PRIMARY_ZONE = 'hangzhou1'",
            "ALTER SYSTEM GET CONFIG PRIMARY_ZONE"
        );

        for (String sql : sqlList) {
            List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);
            SQLStatement stmt = stmtList.get(0);
            String result = SQLUtils.toMySqlString(stmt);
            Assert.assertEquals(normalizeSQL(sql), normalizeSQL(result));
        }
    }

    @Test
    public void testChangeRole() {
        List<String> sqlList = Arrays.asList(
            "ALTER SYSTEM CHANGE_ROLE ZONE 'az1' TO LEARNER",
            "ALTER SYSTEM CHANGE_ROLE ZONE 'az1' TO LEADER",
            "ALTER SYSTEM CHANGE_ROLE ZONE 'az1' TO FOLLOWER",
            "ALTER SYSTEM CHANGE_ROLE NODE '192.1.1.1:3306' TO LEADER",
            "ALTER SYSTEM CHANGE_ROLE NODE '192.1.1.1:3306' TO FOLLOWER",
            "ALTER SYSTEM CHANGE_ROLE NODE '192.1.1.1:3306' TO LEARNER"
        );
        for (String sql : sqlList) {
            List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);
            SQLStatement stmt = stmtList.get(0);
            String result = SQLUtils.toMySqlString(stmt);
            Assert.assertEquals(normalizeSQL(sql), normalizeSQL(result));
        }
    }

    private String normalizeSQL(String sql) {
        return sql.toLowerCase()
            .replaceAll("\n", "")
            .replaceAll("\t", "")
            .replaceAll("=", "")
            .replaceAll(" ", "");

    }
}
