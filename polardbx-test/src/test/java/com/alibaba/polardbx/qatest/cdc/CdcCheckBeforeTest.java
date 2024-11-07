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

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.cdc.Bank.ACCOUNT_COUNT;

/**
 * Created by ziyang.lb
 **/
@Slf4j
public class CdcCheckBeforeTest extends CdcCheckTest {

    public static final String CHECK_CDC_READY_TOKEN = "check_cdc_ready_token";

    @Test
    public void CheckCdcStatus() {
        checkStatus();
    }

    @Test
    public void checkData() throws SQLException, InterruptedException {
        sendCdcToken(CHECK_CDC_READY_TOKEN);
        waitCdcToken(CHECK_CDC_READY_TOKEN);
        doCheck(0);
    }

    @Test
    public void testTransfer() throws SQLException {
        int accountCount = ACCOUNT_COUNT;

        initAccounts(accountCount);
        Bank bank = new Bank(accountCount, srcDs);
        bank.run(10);
    }

    private void initAccounts(int accountCount) throws SQLException {
        Connection conn = null;
        try {
            log.info("start to init accounts.");
            conn = srcDs.getConnection();
            PrepareData.init(conn, accountCount, Bank.ACCOUNT_INIT_AMOUNT);
        } catch (Exception e) {
            log.error("Initial Accounts ERROR.", e);
            throw e;
        } finally {
            JdbcUtil.closeConnection(conn);
        }
        log.info("accounts initialized success.");
    }
}
