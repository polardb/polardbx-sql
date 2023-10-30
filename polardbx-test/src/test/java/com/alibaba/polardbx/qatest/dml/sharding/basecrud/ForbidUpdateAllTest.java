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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;
import static org.hamcrest.CoreMatchers.is;

/**
 * @author arnkore 2017-03-31 19:09
 */


public class ForbidUpdateAllTest extends CrudBasedLockTestCase {
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private final String tddlHint = "/*+TDDL:cmd_extra(FORBID_EXECUTE_DML_ALL=true)*/";

    public ForbidUpdateAllTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table1={0},table2={1}")
    public static List<String[]> prepareParams() {
        return Arrays.asList(ExecuteTableName.fiveBaseTypeTwoTable("update_delete_base_"));
    }

    @Before
    public void prepareData() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    @Test
    public void testIsNull() throws SQLException {
        String sql =
            String.format(tddlHint + "update %s set integer_test = 1 where isNull(varchar_test)", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "update %s set integer_test = 1 where isNull(1)", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testAlwaysTrueOrAlwaysFalse() throws SQLException {
        String sql = String.format(tddlHint + "update %s set integer_test = 1 where 1 = 1", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "update %s set integer_test = 1 where 1 <> 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testParameterizedSql() throws SQLException {
        String sql = String.format(tddlHint + "update %s set integer_test = 1 where pk = ?", baseOneTableName);
        PreparedStatement stmt = tddlConnection.prepareStatement(sql);
        stmt.setLong(1, 330);
        stmt.executeUpdate();
    }

    @Test
    @Ignore("do not support delete cannot push down with limit")
    public void testLimit() throws SQLException {
        String sql = String.format(tddlHint + "update %s set integer_test = 1 limit 1", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "update %s set integer_test = 1 where pk = 10 limit 2", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testUpdateSingleTableWithLimit() throws SQLException {
        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            String sql = String.format(tddlHint + "update %s set integer_test = 1 limit 1", baseOneTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        String sql = String.format(tddlHint + "update %s set integer_test = 1 where pk = 10 limit 2", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testUpdateWithBetweenAnd() throws SQLException {
        String sql =
            String.format(tddlHint + "update %s set integer_test = 1 WHERE pk BETWEEN 2 AND 7", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testUpdateWithOr() throws SQLException {
        String sql = String.format(tddlHint + "update %s set integer_test = 1 where pk =2 or pk=7", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testUpdateWithIn() throws SQLException {
        String sql = String.format(tddlHint + "update %s set integer_test = 1 where pk in (2,7,10)", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testUpdatePkWithOrderByNameLimit() throws SQLException {
        int limitNum = 2;
        String sql = String.format(tddlHint + "update %s set integer_test = 1 where pk=? order by varchar_test limit ?",
            baseOneTableName);
        PreparedStatement stmt = tddlConnection.prepareStatement(sql);
        stmt.setLong(1, 7);
        stmt.setInt(2, limitNum);
        stmt.executeUpdate();
    }

    @Test
    public void testUpdateWithNow() throws SQLException {
        String sql =
            String.format(tddlHint + "update %s set integer_test = 1 where date_test < now()", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testLowPriority() throws SQLException {
        String sql = String.format("update low_priority %s set integer_test = 1", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        try {
            sql = String.format(tddlHint + "update low_priority %s set integer_test = 1", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            is("Forbid executeSuccess DELETE ALL or UPDATE ALL sql");
        }

        sql = String.format("update low_priority %s set integer_test = 1 where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "update low_priority %s set integer_test = 1 where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            sql = String.format("update low_priority %s set integer_test = 1 limit 3", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        sql = String.format(tddlHint + "update low_priority %s set integer_test = 1 where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testIgnore() throws SQLException {
        try {
            String sql = String.format("update ignore %s set integer_test = 1", baseOneTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an NOT_SUPPORT");
        } catch (SQLException e) {
            is("Do not support update with ignore");
        }
    }

    @Test
    @Ignore("do not support subquery")
    public void testSubquery() throws SQLException {
        String sql = String.format(tddlHint
                + "update %s set integer_test = 1 where pk in (select distinct pk from %s where varchar_test = 'aaa')",
            baseOneTableName, baseTwoTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }
}
