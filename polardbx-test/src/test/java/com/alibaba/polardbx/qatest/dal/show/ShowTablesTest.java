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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

/**
 * @author arnkore 2017-06-08 15:49
 */
public class ShowTablesTest extends ReadBaseTestCase {

    @Test
    public void testShowTables() throws SQLException {
        String sql = "show tables";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("TABLES_IN_" + polardbxOneDB.toUpperCase(), rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowTablesFrom() throws SQLException {
        String sql = "show tables from " + polardbxOneDB;
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("TABLES_IN_" + polardbxOneDB.toUpperCase(), rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowTablesFromLike() throws SQLException {
        String sql = "show tables from " + polardbxOneDB + " like '%'";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 1);
        Assert
            .assertEquals("TABLES_IN_" + polardbxOneDB.toUpperCase(), rs.getMetaData().getColumnName(1).toUpperCase());
    }

    @Test
    public void testShowFullTablesFromLike() throws SQLException {
        String sql = "show full tables from " + polardbxOneDB.toUpperCase() + " like '%'";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 3);
        Assert
            .assertEquals("TABLES_IN_" + polardbxOneDB.toUpperCase(), rs.getMetaData().getColumnName(1).toUpperCase());
        Assert.assertEquals("TABLE_TYPE", rs.getMetaData().getColumnName(2).toUpperCase());
    }

    @Test
    public void testShowFullTablesFromLike2() throws SQLException {
        String sql = "show full tables from " + polardbxOneDB.toLowerCase() + " like '%'";
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        Assert.assertEquals(rs.getMetaData().getColumnCount(), 3);
        Assert
            .assertEquals("TABLES_IN_" + polardbxOneDB.toUpperCase(), rs.getMetaData().getColumnName(1).toUpperCase());
        Assert.assertEquals("TABLE_TYPE", rs.getMetaData().getColumnName(2).toUpperCase());
    }

    @Test
    public void testShowFullTablesFromLikeError() throws SQLException {
        String sql = "show full tables from " + "shit" + polardbxOneDB + "shit" + " like '%'";
        Statement stmt = tddlConnection.createStatement();
        boolean hasError = false;
        try {
            ResultSet rs = stmt.executeQuery(sql);
        } catch (Exception e) {
            hasError = true;
        } finally {
            Assert.assertTrue("this sql: " + sql + " should throw an error", hasError);
        }

    }

    @Test
    public void descTable_Order() {
        String table_name = "test_desc_order_table";
        String sql = "drop table if exists `" + table_name + "`";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "create table `" + table_name + "` (id int)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "desc `" + table_name + "`";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());

        sql = "desc " + polardbxOneDB + "." + "`" + table_name + "`";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, ImmutableList.of());
    }
}
