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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 下面的语句是极其危险的操作，默认禁止掉，详情请见：https://aone.alibaba-inc.com/my/task#toPage=1&statusStage=1+2&openTaskId=10554339&。
 * 1. DELETE 语句不带 WHERE 条件
 *     2. UPDATE 语句不带 WHERE 条件
 *     3. 当 ENABLE_DELETE_WITH_LIMIT、ENABLE_UPDATE_WITH_LIMIT 开启的时候，
 * 如果 DELETE、UPDATE 操作既没有提供 where 条件也没有提供 limit 条件。
 * <p>
 * 用户如果确实想要执行上述两种语句怎么办？
 *     1. 通过使用 HINT 方式：TDDL:ENABLE_DELETE_WITHOUT_WHERE_FILTER、TDDL:ENABLE_UPDATE_WITHOUT_WHERE_FILTER*
 * 2. 实例级别允许在 diamond 上定义 ENABLE_DELETE_WITHOUT_WHERE_FILTER、ENABLE_UPDATE_WITHOUT_WHERE_FILTER 开关，
 * 来关闭上述禁止。
 *
 * @author arnkore 2017-03-21 17:36
 */

//fxj
public class ForbidDeleteAllMultiTableTest extends AutoCrudBasedLockTestCase {
    private final String tddlHint = "/*+TDDL:cmd_extra(FORBID_EXECUTE_DML_ALL=true,ENABLE_COMPLEX_DML_CROSS_DB=true)*/";

    private static final int INIT_DATA_COUNT = 100;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    public ForbidDeleteAllMultiTableTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table1={0},table2={1}")
    public static List<String[]> prepareParams() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoTable("update_delete_base_"));
    }

    @Before
    public void prepareData() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        assertDataExists();
    }

    private void assertDataExists() throws SQLException {
        assertDataExists(baseOneTableName);
        assertDataExists(baseTwoTableName);
    }

    private void assertDataExists(String tableName) throws SQLException {
        String sql = "select count(*) from " + tableName;
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean hasResult = rs.next();
        Assert.assertTrue(hasResult);
        int resultCount = rs.getInt(1);
        Assert.assertEquals(INIT_DATA_COUNT, resultCount);
    }

    @Test
    public void deleteWithJoinWhenDeleteAllA() throws SQLException {

        try {
            String sql = tddlHint + "delete  a.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }
    }

    @Test
    public void deleteWithJoinWhenDeleteAllB() throws SQLException {

        try {
            String sql = tddlHint + "delete  b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }
    }

    @Test
    public void deleteWithJoinWhenDeleteAllAAndB() throws SQLException {

        try {
            String sql = tddlHint + "delete a.*,b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }
    }

    @Test
    public void deleteAllWithUsing() throws SQLException {

        try {
            String sql = tddlHint + "delete from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }
    }

    @Test
    public void deleteAllWithUsingAndWhere() throws SQLException {

        try {
            String sql = tddlHint + "delete from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                + " b where a.pk = 10";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }

        try {
            String sql = tddlHint + "delete from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                + " b where a.pk = 10 or b.pk=10";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Ignore("why forbid?")
    @Test
    public void complexDeleteAll() throws Exception {

        try {
            String sql = tddlHint + "delete  b.* from " + baseOneTableName + " a, " + baseTwoTableName
                + " b where a.pk = b.pk";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void deleteMultiTableQuick() throws SQLException {

        try {
            String sql = tddlHint + "delete quick a.*,b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql =
                tddlHint + "delete quick from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql = tddlHint + "delete quick from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                + " b where a.pk = 10";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void deleteMultiTableIgnore() throws SQLException {

        try {
            String sql = tddlHint + "delete ignore a.*,b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql =
                tddlHint + "delete ignore from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql = tddlHint + "delete ignore from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                + " b where a.pk = 10";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
        }
    }

    @Test
    public void deleteMultiTableLowPriority() throws SQLException {

        try {
            String sql =
                tddlHint + "delete LOW_PRIORITY a.*,b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql =
                tddlHint + "delete LOW_PRIORITY from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                    + " b";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }

        try {
            String sql =
                tddlHint + "delete LOW_PRIORITY from a.*,b.* using " + baseOneTableName + " a, " + baseTwoTableName
                    + " b where a.pk = 10";
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Forbid to execute DELETE ALL or UPDATE ALL sql."));
            assertDataExists();
        }
    }
}
