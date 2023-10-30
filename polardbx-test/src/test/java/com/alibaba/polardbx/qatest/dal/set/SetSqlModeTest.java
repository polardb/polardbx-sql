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

package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

/**
 * Sqlmode设置测试
 *
 * @author chenhui
 * @since 5.1.0
 */


public class SetSqlModeTest extends CrudBasedLockTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public SetSqlModeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void testSet_STRICT_TRANS_TABLES() throws Exception {

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // name字段值超长，strict模式应该报错
        setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
        String testVarcharValue = RandomUtils.getStringBetween(256, 266);
        sql = String.format("insert into %s (pk,varchar_test) values(1, '%s')", baseOneTableName, testVarcharValue);
        executeErrorAssert(tddlConnection, sql, null, "Data too long for column");

        setSqlMode(" ", tddlConnection);
        sql = String.format("insert into %s (pk,varchar_test) values(2, '%s')", baseOneTableName, testVarcharValue);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + baseOneTableName;
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null,
            tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("varchar_test").equals(testVarcharValue.substring(0, 255)));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(rs);
        JdbcUtil.close(tddlPs);
    }

    @Test
    public void testSetSqlMode() throws Exception {
        String setSql =
            "SET session sql_mode = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( @@sql_mode, \"STRICT_ALL_TABLES,\", \"\"), \",STRICT_ALL_TABLES\", \"\"),\"STRICT_ALL_TABLES\", \"\"), \"STRICT_TRANS_TABLES,\", \"\"), \",STRICT_TRANS_TABLES\", \"\"), \"STRICT_TRANS_TABLES\", \"\")";
        JdbcUtil.updateDataTddl(tddlConnection, setSql, null);
        String sql = "show tables";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null,
            tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        rs.next();
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);
    }
}
