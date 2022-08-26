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

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author minggong
 * Test batch insert into 'TEXT' field.
 */
public class BatchInsertTextTest extends CrudBasedLockTestCase {

    public BatchInsertTextTest() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    @Before
    public void initData() throws Exception {
        clearDataOnMysqlAndTddl();
    }

    /**
     * Test inserting long strings with SPLIT mode.
     * Also insert Chinese to test if splitting Chinese character into two buffers is OK.
     */
    @Test
    public void testInsertLongText() throws Exception {

        final int MAX_TEXT_LEN = 10000;
        final int TEST_NUM = 1000;

        String sql = "insert into " + baseOneTableName + "(pk, mediumtext_test) value(?, ?)";
        char[] singleChars = {'E', '中'};

        for (char singleChar : singleChars) {
            Connection newConnection = getPolardbxConnection();

            StringBuilder sb = new StringBuilder(MAX_TEXT_LEN);
            for (int i = 0; i < MAX_TEXT_LEN; i++) {
                sb.append(singleChar);
            }
            String longestValue = sb.toString();

            List<List<Object>> params = new ArrayList<>();
            for (int i = 0; i < TEST_NUM; i++) {
                int randomSize = (int) (MAX_TEXT_LEN / 2 * (1 + Math.random()));
                String value = longestValue.substring(0, randomSize);

                List<Object> param = new ArrayList<>();
                param.add(i + 1);
                param.add(value);
                params.add(param);
            }

            String deleteSql = "delete from " + baseOneTableName;
            executeOnMysqlAndTddl(mysqlConnection, newConnection, deleteSql, null);
            setBatchInsertPolicy(BatchInsertPolicy.SPLIT);
            executeBatchOnMysqlAndTddl(mysqlConnection, newConnection, sql, params);

            randomTestData(newConnection, "mediumtext_test", TEST_NUM);
            testCount(TEST_NUM);
        }
    }

    /**
     * If the sql contains only one value, and the length of the statement exceeds SPLIT threshold,
     * make sure it's still OK.
     */
    @Test
    public void testOneStatement() throws Exception {
        StringBuilder sb = new StringBuilder(1000100);
        sb.append("insert into ");
        sb.append(baseOneTableName);
        sb.append("(pk, mediumtext_test) value(1, '");
        for (int i = 0; i < 1000000; i++) {
            sb.append('E');
        }
        sb.append("')");
        String sql = sb.toString();

        clearDataOnTddl();
        setBatchInsertPolicy(BatchInsertPolicy.SPLIT);

        JdbcUtil.executeUpdate(tddlConnection, sql);

        String selectSql = "select mediumtext_test from " + baseOneTableName + " where pk = 1";
        ResultSet rs = JdbcUtil.executeQuery(selectSql, tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getString(1).length() == 1000000);
    }

    private void setBatchInsertPolicy(BatchInsertPolicy policy) {
        String sql = String.format("set batch_insert_policy='%s'", policy.getName());
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    private void clearDataOnMysqlAndTddl() {
        String sql = "delete from " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    private void clearDataOnTddl() {
        String sql = "delete from " + baseOneTableName;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    // random check 100 values to test if it's the same in tddl and mysql.
    private void randomTestData(Connection connection, String fields, int totalNum) {
        for (int i = 0; i < 100; i++) {
            int r = (int) (Math.random() * totalNum);
            r = Math.min(Math.max(r, 1), totalNum);
            String sql = String.format("select %s from %s where %s=%d", fields, baseOneTableName, "pk", r);
            JdbcUtil.executeSuccess(connection, "set names gbk");
            selectContentSameAssert(sql, null, mysqlConnection, connection);
        }
    }

    // test total num of records.
    private void testCount(int totalNum) throws Exception {
        String sql = "select count(1) from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getLong(1) == totalNum);
    }
}
