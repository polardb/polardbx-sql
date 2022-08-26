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
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class SpecialUpdateTest1 extends AutoCrudBasedLockTestCase {

    private final String HINT =
        "/*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,COMPLEX_DML_WITH_TRX=FALSE)*/";

    public SpecialUpdateTest1() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.HASH_BY_VARCHAR
            + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
        this.baseTwoTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.HASH_BY_VARCHAR
            + ExecuteTableName.TWO + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    private void clearData(String table) {
        String sql = "delete from  " + table;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, Lists.newArrayList());
    }

    /**
     * Sharding key(varchar_test) is null
     */
    @Test
    public void updateMultiTableWithShardingKeyNull() {
        clearData(baseOneTableName);
        clearData(baseTwoTableName);

        String insertSql = "insert into " + baseOneTableName + " (pk,integer_test,varchar_test) values (?,?,?)";
        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            params.add(Lists.newArrayList(i, 10, null));
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);

        insertSql = "insert into " + baseTwoTableName + " (pk,integer_test,varchar_test) values (?,?,?)";
        params.clear();
        for (int i = 0; i < 10; i++) {
            params.add(Lists.newArrayList(i, 10, null));
        }
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);

        String sql = String.format(HINT + "update %s a, %s b set a.integer_test=20 where a.integer_test=b.integer_test",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, Lists.newArrayList(), true);

        sql = String.format("select pk, integer_test, varchar_test from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
