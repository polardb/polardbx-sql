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
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.cdc.Bank.ACCOUNT_COUNT;
import static com.alibaba.polardbx.qatest.cdc.Bank.ACCOUNT_INIT_AMOUNT;

/**
 * created by ziyang.lb
 **/
@Slf4j
public class CdcCheckAfterTest extends CdcCheckTest {

    public static final String CHECK_CDC_COMPLETE_TOKEN = "check_cdc_complete_token";

    @Test
    public void CheckCdcStatus() {
        checkStatus();
    }

    @Test
    public void checkData() throws SQLException, InterruptedException {
        sendCdcToken(CHECK_CDC_COMPLETE_TOKEN, 0);
        waitCdcToken(CHECK_CDC_COMPLETE_TOKEN, 0);
        //正向
        doCheck(0);
        //反向
        doCheck(1);
        //转账
        checkAccounts();
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
