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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class UserPasswdChecker {

    public static final int MAX_USER_COUNT = 2048;

    private static final Pattern USERNAME_CHECK_PATTERN = Pattern.compile("^[A-Za-z_][0-9A-Za-z_]{1,254}$");

    /**
     * <pre>
     * 用户名限制:
     *   长度必须大于等于2个字符, 小于等于20个字符.
     *   必须以字母或者下划线开头
     *   字符范围是: 大写字母,小写字母,数字,下划线
     * </pre>
     */
    public static boolean verifyUsername(String username) {
        Matcher m = USERNAME_CHECK_PATTERN.matcher(username);
        return m.matches();
    }

    public static void verifyPassword(String password, PasswdRuleConfig config) {
        if (config == null) {
            Matcher m = DynamicConfig.getInstance().getPasswordCheckPattern().matcher(password);
            if (!m.matches()) {
                if (DynamicConfig.getInstance().isDefaultPasswordCheckPattern()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD_CUSTOMIZED,
                        DynamicConfig.getInstance().getPasswordCheckPattern().pattern());
                }
            }
            return;
        }
        if (!config.verifyPassword(password)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD);
        }
    }
}
