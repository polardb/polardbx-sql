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

import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

public class SetBlockEncryptionModeTest extends DirectConnectionBaseTestCase {

    final String setEncryptionModePattern = "SET block_encryption_mode = '%s';";
    final String modePattern = "aes-%d-%s";
    final int legalKeylenArr[] = {128, 192, 256};
    final String legalModeArr[] = {"ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb"};

    @Test
    public void setBlockEncryptionModeTest() throws SQLException {
        boolean supportOpenSSL = ConnectionManager.getInstance().isEnableOpenSSL();
        for (int keylen : legalKeylenArr) {
            for (String mode : legalModeArr) {
                String encryptionMode = String.format(modePattern, keylen, mode);
                boolean expectSuccess = true;
                try {
                    new BlockEncryptionMode(encryptionMode, supportOpenSSL);
                } catch (BlockEncryptionMode.NotSupportOpensslException e) {
                    expectSuccess = false;
                }
                if (expectSuccess) {
                    expectSetSuccess(encryptionMode);
                } else {
                    expectSetFail(encryptionMode);
                }
            }
        }
    }

    @Test
    public void setIllegalBlockEncryptionModeTest() {
        final int illegalKeylen = 100;
        final String illegalMode = "cbf";
        String encryptionMode = String.format(modePattern, illegalKeylen, legalModeArr[0]);
        expectSetFail(encryptionMode);

        encryptionMode = String.format(modePattern, legalKeylenArr[0], illegalMode);
        expectSetFail(encryptionMode);
    }

    private void setBlockEncryptionMode(String encryptionMode) {
        String setEncryptionModeSql = String.format(setEncryptionModePattern, encryptionMode);
        JdbcUtil.updateDataTddl(tddlConnection, setEncryptionModeSql, null);
    }

    private void expectSetSuccess(String encryptionMode) throws SQLException {
        setBlockEncryptionMode(encryptionMode);
        String sql = "show variables like 'block_encryption_mode'";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        try {
            Assert.assertTrue(rs.next());
            String value = rs.getString("Value");
            Assert.assertTrue(
                String.format("block_encryption_mode [%s] doesn't match the set value [%s]", value, encryptionMode),
                rs.getString("Value").equalsIgnoreCase(encryptionMode));
            Assert.assertFalse(rs.next());
        } finally {
            JdbcUtil.close(tddlPs);
            JdbcUtil.close(rs);
        }
    }

    private void expectSetFail(String encryptionMode) {
        String sql = String.format(setEncryptionModePattern, encryptionMode);
        executeErrorAssert(tddlConnection, sql, null,
            String.format("VARIABLE BLOCK_ENCRYPTION_MODE CAN'T BE SET TO THE VALUE OF %s", encryptionMode));
    }

}
