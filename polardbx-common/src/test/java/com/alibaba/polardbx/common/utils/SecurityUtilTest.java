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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * 测试加密工具
 *
 * @author arnkore 2016-11-24 16:28
 */
@RunWith(Enclosed.class)
public class SecurityUtilTest {
    @RunWith(Parameterized.class)
    public static class ParameterizedTest {
        private String pwd;

        private String scramble;

        public ParameterizedTest(String pwd, String scramble) {
            this.pwd = pwd;
            this.scramble = scramble;
        }

        @Parameterized.Parameters(name = "{index}:pwd={0},scramble={1}")
        public static List<String[]> getParamsData() {
            return Arrays.asList(new String[][]{
                    {"123456", "DAKJ4K1243"},
                    {"111111", "8qwerjqaf"},
                    {"111111", "13jkfjskd"},
                    {"123456", "jsakfdajskf"},
                    {"go2hell", "jdfkasjfk"},
                    {"go2hell", "ewjrkfsja8124"}
            });
        }

        @Test
        public void mysqlClientServerAuthenticateTest() throws NoSuchAlgorithmException {
            byte[] mysqlUserPassword = SecurityUtil.calcMysqlUserPassword(pwd.getBytes());
            byte[] token = SecurityUtil.scramble411(pwd.getBytes(), scramble.getBytes());
            assertTrue(SecurityUtil.verify(token, mysqlUserPassword, scramble.getBytes()));
        }
    }

    public static class NonparameterizedTest {
        @Test
        public void mysqlUserPasswordTest() throws NoSuchAlgorithmException {
            byte[] mysqlUserPassword = SecurityUtil.calcMysqlUserPassword("go2hell".getBytes());
            assertTrue("2E6558E64F7FD60426931B09CD962F2789D5F1FC".equalsIgnoreCase(SecurityUtil.byte2HexStr(mysqlUserPassword)));
        }
    }
}
