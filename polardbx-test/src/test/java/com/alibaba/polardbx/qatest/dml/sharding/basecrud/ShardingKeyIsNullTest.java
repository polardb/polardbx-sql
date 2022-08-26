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
import com.alibaba.polardbx.qatest.data.ExecuteCHNTableName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 拆分建可以为空
 */

public class ShardingKeyIsNullTest extends CrudBasedLockTestCase {

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteCHNTableName.allBaseTypeOneTable(ExecuteCHNTableName.UPDATE_DELETE_BASE));
    }

    public ShardingKeyIsNullTest(String tableName) throws SQLException {
        this.baseOneTableName = tableName;
    }

    @Before
    public void initData() {
        String sql = "DELETE FROM " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @Test
    public void testShardKeyIsNull() {
        /*
         * !! 注意，
         *
         * 这个报错是因为拆分键是中文的原因，它的规则原本应该是简单规则（有判空处理），
         * 但因为中文的原因tddl将它识别为复杂规则(里边没有判空),
         *
         * 目前这个testCase在tddl-client之下忽略， client暂时不存在中文列的场景
         */
        String insertSql = String.format(
            "insert into %s (编号, 单号, 姓名) values(1, null, 'abc')",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            insertSql, null);

        String selectSql = String.format(
            "select * from %s where 单号 is null", baseOneTableName);
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);

        String updateSql = String.format(
            "update %s set 姓名='中文姓名' where 单号 is null",
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            updateSql, null);
        selectContentSameAssert(selectSql, null,
            mysqlConnection, tddlConnection);
    }

}
