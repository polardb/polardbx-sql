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

package com.alibaba.polardbx.common.privileges;

import com.alibaba.polardbx.common.privilege.UserPasswdChecker;
import org.junit.Assert;
import org.junit.Test;

/**
 * <pre>
 * 用户名限制:
 *   长度必须大于等于2个字符, 小于等于255个字符.
 *   必须以字母或者下划线开头
 *   字符范围是: 大写字母,小写字母,数字,下划线
 * </pre>
 *
 * @author arnkore 2017-06-06 14:24
 */
public class UserCheckerTest {

    @Test
    public void testCapitalLetter() {
        Assert.assertTrue(UserPasswdChecker.verifyUsername("abc"));
        Assert.assertTrue(UserPasswdChecker.verifyUsername("_abc"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("1abc"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("!abc"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("@abc"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("&abc"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("&abc"));
    }

    @Test
    public void testLength() {
        StringBuilder appendable = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            appendable.append("a");
            String str = appendable.toString();
            if (str.length() < 2 || str.length() > 255) {
                Assert.assertFalse(str, UserPasswdChecker.verifyUsername(str));
            } else {
                Assert.assertTrue(UserPasswdChecker.verifyUsername(str));
            }
        }
    }

    @Test
    public void testValidLetter() {
        Assert.assertTrue(UserPasswdChecker.verifyUsername("abc123"));
        Assert.assertTrue(UserPasswdChecker.verifyUsername("abc_123"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("abc!"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("abc@"));
        Assert.assertFalse(UserPasswdChecker.verifyUsername("abc&"));
    }
}
