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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;
import static org.hamcrest.CoreMatchers.is;

/**
 * @author arnkore 2017-04-18 20:55
 */


public class ForbidUpdateAllMultiTableTest extends AutoCrudBasedLockTestCase {
    private final String tddlHint =
        "/*+TDDL:cmd_extra(FORBID_EXECUTE_DML_ALL=true,ENABLE_COMPLEX_DML_CROSS_DB=true,COMPLEX_DML_WITH_TRX=false)*/";

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    public ForbidUpdateAllMultiTableTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table1={0},table2={1}")
    public static List<String[]> prepareParams() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoTable("update_delete_base_"));
    }

    @Before
    public void prepareData() throws Exception {
        tableDataPrepare(baseOneTableName, 100, TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME,
            mysqlConnection, tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 100, TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME,
            mysqlConnection, tddlConnection, columnDataGenerator);
    }

    @Test
    public void updateAllTest() throws Exception {
        try {
            String sql = String.format(tddlHint + "update %s,%s set %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "'",
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void updateWhereTest() throws Exception {
        try {
            String sql = String.format(tddlHint + "update %s,%s set %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "' where %s.pk=0",
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void updateWhereAliasTest() throws Exception {
        try {
            String sql = String.format(tddlHint + "update %s a,%s b set a.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "' where a.pk=0",
                baseOneTableName,
                baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void updateLeftJoinTest() throws Exception {
        try {
            String sql = String.format(tddlHint + "update %s left join %s on %s.pk=%s.pk  set %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "'",
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void updateJoinTest() throws Exception {
        String sql = String.format(
            tddlHint + "update %s a , %s b set a.varchar_test='" + columnDataGenerator.varchar_testValue
                + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' where a.pk=b.pk and b.pk > 3 ",
            baseOneTableName,
            baseTwoTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void updateLeftJoinAliasTest() throws Exception {
        try {
            String sql = String.format(tddlHint + "update %s a left join %s b on a.pk=b.pk  set a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'", baseOneTableName, baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void updateMultiTableIgnore() throws Exception {
        try {
            String sql = String.format(tddlHint + "update ignore %s a left join %s b on a.pk=b.pk  set a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'", baseOneTableName, baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            is("Do not support update with ignore");
        }
    }

    @Test
    public void updateMultiTableLowPriority() throws Exception {
        try {
            String sql =
                String.format(tddlHint + "update LOW_PRIORITY %s a left join %s b on a.pk=b.pk  set a.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "'", baseOneTableName, baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }

        try {
            String sql = String.format(
                tddlHint + "update LOW_PRIORITY %s left join %s on %s.pk=%s.pk  set %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "', %s.varchar_test='"
                    + columnDataGenerator.varchar_testValue + "'",
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }

        try {
            String sql = String.format(tddlHint + "update LOW_PRIORITY %s a,%s b set a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' where a.pk=b.pk", baseOneTableName, baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }

        try {
            String sql = String.format(
                tddlHint + "update LOW_PRIORITY %s,%s set %s.varchar_test='" + columnDataGenerator.varchar_testValue
                    + "', %s.varchar_test='" + columnDataGenerator.varchar_testValue + "' where %s.pk=%s.pk",
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName,
                baseOneTableName,
                baseTwoTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }
}
