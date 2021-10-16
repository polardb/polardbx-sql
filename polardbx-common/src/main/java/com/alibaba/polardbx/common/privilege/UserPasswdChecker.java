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

package com.alibaba.polardbx.common.privilege;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class UserPasswdChecker {

    public static final int MAX_USER_COUNT = 2048;

    private static final Pattern USERNAME_CHECK_PATTERN = Pattern.compile("^[A-Za-z_][0-9A-Za-z_]{1,254}$");

    private static final Pattern PASSWORD_CHECK_PATTERN = Pattern.compile("^[0-9A-Za-z@#$%^&+=]{6,20}$");

    public static boolean verifyUsername(String username) {
        Matcher m = USERNAME_CHECK_PATTERN.matcher(username);
        return m.matches();
    }

    public static boolean verifyPassword(String password, PasswdRuleConfig config) {
        if (config == null) {
            Matcher m = PASSWORD_CHECK_PATTERN.matcher(password);
            return m.matches();
        }
        return config.verifyPassword(password);
    }
}
