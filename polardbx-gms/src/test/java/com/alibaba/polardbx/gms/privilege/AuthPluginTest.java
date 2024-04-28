package com.alibaba.polardbx.gms.privilege;

import com.taobao.tddl.common.privilege.AuthPlugin;
import com.taobao.tddl.common.privilege.EncrptPassword;
import org.junit.Assert;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;

public class AuthPluginTest {

    private static final String ONE_ROUND_SHA1_PASSWORD = "7c4a8d09ca3762af61e59520943dc26494f8941b";
    private static final String TWO_ROUND_SHA1_PASSWORD = "6bb4837eb74329105ee4568dda7dc67ed2ca2ad9";

    @Test
    public void testAuthPlugin() {
        EncrptPassword polardbxPassword = new EncrptPassword(ONE_ROUND_SHA1_PASSWORD,
            AuthPlugin.POLARDBX_NATIVE_PASSWORD, true);
        EncrptPassword mysqlPassword = new EncrptPassword(TWO_ROUND_SHA1_PASSWORD,
            AuthPlugin.MYSQL_NATIVE_PASSWORD, true);
        try {
            byte[] mysqlPassFromPolardbx = polardbxPassword.getMysqlPassword();
            byte[] mysqlPassFromMysql = mysqlPassword.getMysqlPassword();
            Assert.assertArrayEquals("Expect the same result with different plugins ",
                mysqlPassFromMysql, mysqlPassFromPolardbx);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
