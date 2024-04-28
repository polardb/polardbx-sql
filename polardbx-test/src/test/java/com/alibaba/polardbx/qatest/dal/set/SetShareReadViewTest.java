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

import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

public class SetShareReadViewTest extends DirectConnectionBaseTestCase {
    private final String SET_SQL_PATTERN = "set share_read_view = %s";

    private static boolean supportShareReadView;
    private static boolean isDefaultShareReadView;

    @BeforeClass
    public static void init() throws SQLException {

        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, polardbXShardingDBName1());
            isDefaultShareReadView = JdbcUtil.isShareReadView(connection);
            supportShareReadView = JdbcUtil.supportShareReadView(connection);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    @Test
    public void testDN() throws SQLException {

        trySetOutsideTrx();
        trySetShareReadView();
    }

    @Test
    public void testIsolationLevel() throws SQLException {

        if (!supportShareReadView) {
            return;
        }
        tddlConnection.setAutoCommit(false);
        if (!isDefaultShareReadView) {
            JdbcUtil.setShareReadView(true, tddlConnection);
        }
        tddlConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        boolean shareReadView = JdbcUtil.isShareReadView(tddlConnection);
        Assert.assertFalse("share_read_view is not off", shareReadView);

        expectSetFail(true, "Share read view is only supported in repeatable-read");
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    /**
     * 尝试在事务外设置share_read_view变量
     * 预期报错
     */
    private void trySetOutsideTrx() {
        String setSql = String.format(SET_SQL_PATTERN, "off");
        executeErrorAssert(tddlConnection, setSql, null,
            "VARIABLE SHARE_READ_VIEW CAN'T BE SET TO THE VALUE OF OFF");
    }

    private void trySetShareReadView() throws SQLException {
        tddlConnection.setAutoCommit(false);
        if (supportShareReadView) {
            JdbcUtil.setShareReadView(!isDefaultShareReadView, tddlConnection);
            boolean result = JdbcUtil.isShareReadView(tddlConnection);
            Assert.assertTrue("Failed to switch on share_read_view", result != isDefaultShareReadView);
        } else {
            // expect failure if dn not support share read view
            expectSetFail(true, "Data node does not support share read view");
        }

        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    private void expectSetFail(boolean flag, String msg) {
        String setSql = String.format(SET_SQL_PATTERN, flag ? "ON" : "OFF");
        executeErrorAssert(tddlConnection, setSql, null, msg);
    }
}
