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

package com.alibaba.polardbx.qatest.dml.sharding.datatype;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultNotMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderByNotPrimaryKeyAssert;

/**
 * @author fangwu
 */
public class DataTypeShardingTest extends CrudBasedLockTestCase {
    public static final String tableName = "DataTypeShardingTest";

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.sencitiveTypeTable());
    }

    public DataTypeShardingTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        String sql =
            "CREATE TABLE IF NOT EXISTS " + tableName
                + " ( id int, c2 bigint(20) DEFAULT NULL) dbpartition by uni_hash(c2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @After
    public void clearTable() {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testDataTypeSharding() throws Exception {
        // get group id
        String sql = "trace select 1 from " + tableName + " where c2='131313131331312'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String groupName = "";
        ResultSet rs = JdbcUtil.executeQuery("show trace", tddlConnection);
        while (rs.next()) {
            groupName = rs.getString("GROUP_NAME");
        }

        Assert.assertTrue(!StringUtils.isEmpty(groupName));
        String explainSql = "explain select 1 from " + tableName + " where c2=131313131331312";
        explainResultMatchAssert(explainSql, null, tddlConnection, "[\\s\\S]*" + groupName + "[\\s\\S]*");

        explainSql = "explain select 1 from " + tableName + " where c2 in(131313131331312)";
        explainResultMatchAssert(explainSql, null, tddlConnection, "[\\s\\S]*" + groupName + "[\\s\\S]*");

        explainSql = "explain select 1 from " + tableName + " where c2 in('131313131331312')";
        explainResultMatchAssert(explainSql, null, tddlConnection, "[\\s\\S]*" + groupName + "[\\s\\S]*");
    }
}
