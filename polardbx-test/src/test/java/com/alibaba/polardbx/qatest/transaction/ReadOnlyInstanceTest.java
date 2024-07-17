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

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ReadOnlyInstanceTest extends CrudBasedLockTestCase {

    @Test
    public void testSetAutoCommitOff() throws Exception {
        if (!checkReadOnlyInstance()) {
            return;
        }

        tddlConnection.setAutoCommit(false);

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CURRENT_TRANS_POLICY()")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("ALLOW_READ", rs.getString(1));
        }
    }

    @Test
    public void testSetTrxPolicy() throws Exception {
        if (!checkReadOnlyInstance()) {
            return;
        }

        tddlConnection.setAutoCommit(false);

        Exception exception = null;
        try {
            JdbcUtil.setTxPolicy(ITransactionPolicy.XA, tddlConnection);
        } catch (Exception ex) {
            exception = ex;
        }

        Assert.assertNotNull(exception);
        Assert.assertTrue(exception.getMessage().contains("not supported"));
    }

    private boolean checkReadOnlyInstance() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show variables like 'polardbx_instance_role'")) {
            Assert.assertTrue(rs.next());
            String role = rs.getString("Value");
            return !"MASTER".equals(role);
        }
    }
}
