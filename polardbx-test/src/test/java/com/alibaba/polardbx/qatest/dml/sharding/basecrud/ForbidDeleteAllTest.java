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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;
import static org.hamcrest.CoreMatchers.is;

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
public class ForbidDeleteAllTest extends CrudBasedLockTestCase {
    private final String tddlHint = "/*+TDDL:cmd_extra(FORBID_EXECUTE_DML_ALL=true)*/";

    private static final int INIT_DATA_COUNT = 100;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    public ForbidDeleteAllTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareParams() {
        return Arrays.asList(ExecuteTableName.fiveBaseTypeTwoTable("update_delete_base_"));
    }

    @Before
    public void prepareData() throws Exception {
        tableDataPrepare(baseOneTableName, INIT_DATA_COUNT,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        assertDataExists();
    }

    private void assertDataExists() throws SQLException {
        String sql = "select count(*) from " + baseOneTableName;
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean hasResult = rs.next();
        Assert.assertTrue(hasResult);
        int resultCount = rs.getInt(1);
        Assert.assertEquals(INIT_DATA_COUNT, resultCount);
    }

    @Test
    public void testIsNull() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where isNull(varchar_test)", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete from %s where isNull(1)", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testAlwaysTrueOrAlwaysFalse() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where 1 = 1", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete from %s where 1 <> 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testParameterizedSql() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where pk = ?", baseOneTableName);
        PreparedStatement stmt = tddlConnection.prepareStatement(sql);
        stmt.setLong(1, 330);
        stmt.executeUpdate();
    }

    @Test
    @Ignore("do not support delete cannot push down with limit")
    public void testLimit() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s limit 1", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete from %s where pk = 10 limit 2", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testSingleTableDeleteWithLimit() throws SQLException {
        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            String sql = String.format(tddlHint + "delete from %s limit 1", baseOneTableName);
            Statement stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        String sql = String.format(tddlHint + "delete from %s where pk = 10 limit 2", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testDeleteWithBetweenAnd() throws SQLException {
        String sql = String.format(tddlHint + "DELETE FROM %s WHERE pk BETWEEN 2 AND 7", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testDeleteWithOr() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where pk =2 or pk=7", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testDeleteWithIn() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where pk in (2,7,10)", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testDeletePkWithOrderByNameLimit() throws SQLException {
        int limitNum = 2;
        String sql =
            String.format(tddlHint + "delete from %s where pk=? order by varchar_test limit ?", baseOneTableName);
        PreparedStatement stmt = tddlConnection.prepareStatement(sql);
        stmt.setLong(1, 7);
        stmt.setInt(2, limitNum);
        stmt.executeUpdate();
    }

    @Test
    public void testDeleteWithNow() throws SQLException {
        String sql = String.format(tddlHint + "delete from %s where date_test < now()", baseOneTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testLowPriority() throws SQLException {
        String sql = null;
        Statement stmt = null;

        try {
            sql = String.format(tddlHint + "delete low_priority from %s", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            is("Forbid executeSuccess DELETE ALL or UPDATE ALL sql");
            assertDataExists();
        }

        sql = String.format("delete low_priority from %s", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format("delete low_priority from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete low_priority from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            sql = String.format("delete low_priority from %s limit 3", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        sql = String.format(tddlHint + "delete low_priority from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testIgnore() throws SQLException {
        String sql = null;
        Statement stmt = null;

        try {
            sql = String.format(tddlHint + "delete ignore from %s", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            is("Forbid executeSuccess DELETE ALL or UPDATE ALL sql");
            assertDataExists();
        }

        sql = String.format("delete ignore from %s", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format("delete ignore from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete ignore from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            sql = String.format("delete ignore from %s limit 3", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        sql = String.format(tddlHint + "delete ignore from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    public void testQuick() throws SQLException {
        String sql = null;
        Statement stmt = null;

        try {
            sql = String.format(tddlHint + "delete quick from %s", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
            Assert.fail("expected an ERR_FORBID_EXECUTE_DML_ALL");
        } catch (SQLException e) {
            is("Forbid executeSuccess DELETE ALL or UPDATE ALL sql");
            assertDataExists();
        }

        sql = String.format("delete quick from %s", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format("delete quick from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        sql = String.format(tddlHint + "delete quick from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);

        if (ConfigUtil.isSingleTable(baseOneTableName)) {
            sql = String.format("delete quick from %s limit 3", baseOneTableName);
            stmt = tddlConnection.createStatement();
            stmt.execute(sql);
        }

        sql = String.format(tddlHint + "delete quick from %s where pk = 1", baseOneTableName);
        stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }

    @Test
    @Ignore("do not support subquery")
    public void testSubquery() throws SQLException {
        String sql = String
            .format(tddlHint + "delete from %s where pk in (select distinct pk from %s where varchar_test = 'aaa')",
                baseOneTableName, baseTwoTableName);
        Statement stmt = tddlConnection.createStatement();
        stmt.execute(sql);
    }
}
