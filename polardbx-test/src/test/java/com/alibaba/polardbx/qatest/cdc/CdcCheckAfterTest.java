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

package com.alibaba.polardbx.qatest.cdc;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.cdc.Bank.ACCOUNT_COUNT;
import static com.alibaba.polardbx.qatest.cdc.Bank.ACCOUNT_INIT_AMOUNT;

/**
 * created by ziyang.lb
 **/
@Slf4j
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CdcCheckAfterTest extends CdcCheckTest {

    public static final String CHECK_CDC_COMPLETE_TOKEN = "check_cdc_complete_token";

    @Test
    public void test1_CheckCdcStatus() {
        checkStatus();
    }

    @Test
    public void test2_checkSyncComplete() throws SQLException, InterruptedException {
        sendCdcToken(CHECK_CDC_COMPLETE_TOKEN);
        waitCdcToken(CHECK_CDC_COMPLETE_TOKEN);
    }

    @Test
    public void test3_checkDataConsistency() throws SQLException {
        //正向
        doCheck(0);
        //反向
        doCheck(1);
    }

    @Test
    public void test4_checkAccount() throws SQLException {
        //转账
        checkAccounts();
    }

    @Test
    public void test5_DdlRecordCount() throws SQLException {
        int count1 = 0;
        // create if not exits，drop if exists，这两种情况如果库表已经存在或不存在时，binlog_logic_meta_history中不会记录
        // 验证两天打标数据量是否一致时，忽略掉job_id为null的情况，drop database除外
        try (Connection conn = getPolardbxConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet resultSet =
                    stmt.executeQuery("select count(*) from __cdc__.__cdc_ddl_record__ where job_id is not null "
                        + " or (job_id is null and sql_kind = 'DROP_DATABASE')");
                while (resultSet.next()) {
                    count1 = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw e;
        }

        int count2 = 0;
        try (Connection conn = getMetaConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet resultSet =
                    stmt.executeQuery(
                        "select count(*) from binlog_logic_meta_history where type = 2 and ddl_job_id is not null "
                            + "or (ddl_job_id is null and sql_kind = 'DROP_DATABASE')");
                while (resultSet.next()) {
                    count2 = resultSet.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw e;
        }

        log.info("src count is {}, target count is {}", count1, count2);
        Assert.assertEquals(count1, count2);
    }

    private void checkAccounts() throws SQLException {
        log.info("start to check accounts balance.");

        String checkSql = "select sum(balance) from drds_polarx1_qatest_app.accounts";
        Connection sourceConn = null;
        ResultSet sourceRs = null;

        Connection targetConn = null;
        ResultSet targetRs = null;
        try {
            sourceConn = srcDs.getConnection();
            sourceRs = DataSourceUtil.query(sourceConn, checkSql, 0);
            int sourceSum = 0;
            while (sourceRs.next()) {
                sourceSum = sourceRs.getInt(1);
            }

            targetConn = dstDs.getConnection();
            targetRs = DataSourceUtil.query(targetConn, checkSql, 0);
            int targetSum = 0;
            while (targetRs.next()) {
                targetSum = targetRs.getInt(1);
            }

            int totalBalance = ACCOUNT_COUNT * ACCOUNT_INIT_AMOUNT;
            Assert.assertEquals(sourceSum, totalBalance);
            Assert.assertEquals(targetSum, totalBalance);
        } catch (Exception e) {
            throw e;
        } finally {
            DataSourceUtil.closeQuery(sourceRs, null, sourceConn);
            DataSourceUtil.closeQuery(targetRs, null, targetConn);
        }
    }
}
