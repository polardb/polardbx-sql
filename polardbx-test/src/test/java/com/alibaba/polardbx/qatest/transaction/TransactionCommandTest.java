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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * 事务控制指令测试
 */

public class TransactionCommandTest extends CrudBasedLockTestCase {

    public TransactionCommandTest() {
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    @Ignore
    @Test
    public void testShowTrans() throws Exception {
        tddlConnection.setAutoCommit(false);
        //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, tddlConnection);

        String sql = "update " + baseOneTableName + " set integer_test=1";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        String expectedTransId;
        try (ResultSet rs = JdbcUtil.executeQuery("select CURRENT_TRANS_ID()", tddlConnection)) {
            Assert.assertTrue(rs.next());
            expectedTransId = rs.getString(1);
        }

        boolean found = false;
        try (ResultSet rs = JdbcUtil.executeQuery("show trans", tddlConnection)) {
            List<List<Object>> allResults = JdbcUtil.getAllResult(rs);
            for (List<Object> r : allResults) {
                if (expectedTransId.equals(r.get(0))) {
                    Assert.assertEquals("BEST_EFFORT", r.get(1));
                    found = true;
                }
            }
        }
        Assert.assertTrue("should see this transaction", found);

        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    @Ignore
    @Test
    public void testPurgeTrans() throws Exception {
        tddlConnection.setAutoCommit(false);
        //JdbcUtil.setTxPolicy(ITransactionPolicy.BEST_EFFORT, tddlConnection);

        String sql = "update " + baseOneTableName + " set integer_test=1";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

        try (Connection tddlConnection2 = getPolardbxConnection()) {
            int affectedRows = JdbcUtil.updateData(tddlConnection2, "PURGE TRANS", null);
            Assert.assertTrue("should purged some transactions", affectedRows > 0);
        }

    }

}
