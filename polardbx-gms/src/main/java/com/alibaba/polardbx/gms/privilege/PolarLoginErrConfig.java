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

package com.alibaba.polardbx.gms.privilege;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

/**
 * password error limit rule
 *
 * @author hongxi.chx
 */
public class PolarLoginErrConfig {

    private static class UserLoginErrConfig {

        private final int passwordMaxErrorCount;
        /**
         * 错误次数重置的时间
         */
        private final long expireSeconds;
        private final Date passwordExpireDate;

        UserLoginErrConfig(int passwordMaxErrorCount, long expireSeconds, Date passwordExpireDate) {
            this.passwordMaxErrorCount = passwordMaxErrorCount;
            this.expireSeconds = expireSeconds;
            this.passwordExpireDate = passwordExpireDate;
        }

        public UserLoginErrConfig(UserLoginErrConfig defaultConfig) {
            this.passwordMaxErrorCount = defaultConfig.passwordMaxErrorCount;
            this.expireSeconds = defaultConfig.expireSeconds;
            this.passwordExpireDate = defaultConfig.passwordExpireDate;
        }

        static UserLoginErrConfig getDefaultConfig() {
            return new UserLoginErrConfig(0, 0, null);
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(PolarLoginErrConfig.class);

    public static final String PASSWORD_MAX_ERROR_COUNT_KEY = "passwordMaxErrorCount";
    public static final String EXPIRE_SECONDS_KEY = "expireSeconds";
    public static final String PASSWORD_EXPIRE_DATE_KEY = "passwordExpireDate";

    public static final SimpleDateFormat EXPIRE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final UserLoginErrConfig DEFAULT_LOGIN_ERR_CONFIG;
    private final Map<String, UserLoginErrConfig> userLoginErrConfigMap =
        Collections.synchronizedSortedMap(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));

    public PolarLoginErrConfig() {
        this.DEFAULT_LOGIN_ERR_CONFIG = UserLoginErrConfig.getDefaultConfig();
    }

    private PolarLoginErrConfig(UserLoginErrConfig loginErrConfig) {
        this.DEFAULT_LOGIN_ERR_CONFIG = loginErrConfig;
    }

    public static PolarLoginErrConfig parse(JSONObject config) throws ParseException {
        if (config == null) {
            return new PolarLoginErrConfig();
        }
        PolarLoginErrConfig loginErrConfig;
        JSONObject defaultConfig = config.getJSONObject("DEFAULT");
        if (defaultConfig == null) {
            loginErrConfig = new PolarLoginErrConfig();
        } else {
            loginErrConfig = parseDefaultConfig(defaultConfig);
        }

        JSONObject userConfigs = config.getJSONObject("USERS");
        parseUsersConfig(userConfigs, loginErrConfig.DEFAULT_LOGIN_ERR_CONFIG, loginErrConfig.userLoginErrConfigMap);
        return loginErrConfig;
    }

    private static void parseUsersConfig(JSONObject userConfigs,
                                         UserLoginErrConfig defaultConfig,
                                         Map<String, UserLoginErrConfig> userLoginErrConfigMap) {
        try {
            if (userConfigs == null) {
                return;
            }
            for (String username : userConfigs.keySet()) {
                if (StringUtils.isEmpty(username) || PolarPrivUtil.POLAR_ROOT.equalsIgnoreCase(username)) {
                    // no login constraint for polardbx_root
                    continue;
                }
                JSONObject userConfig = userConfigs.getJSONObject(username);

                UserLoginErrConfig userLoginErrConfig;
                if (userConfig == null) {
                    userLoginErrConfig = new UserLoginErrConfig(defaultConfig);
                } else {
                    int passwordMaxErrorCount;
                    long expireSeconds;
                    Date passwordExpireDate;

                    if (userConfig.containsKey(PASSWORD_MAX_ERROR_COUNT_KEY)) {
                        passwordMaxErrorCount = getPasswordMaxErrorCount(userConfig);
                    } else {
                        passwordMaxErrorCount = defaultConfig.passwordMaxErrorCount;
                    }
                    if (userConfig.containsKey(EXPIRE_SECONDS_KEY)) {
                        expireSeconds = getExpireSeconds(userConfig);
                    } else {
                        expireSeconds = defaultConfig.expireSeconds;
                    }
                    if (userConfig.containsKey(PASSWORD_EXPIRE_DATE_KEY)) {
                        passwordExpireDate = getPasswordExpireDate(userConfig);
                    } else {
                        passwordExpireDate = defaultConfig.passwordExpireDate;
                    }
                    userLoginErrConfig =
                        new UserLoginErrConfig(passwordMaxErrorCount, expireSeconds, passwordExpireDate);
                }

                userLoginErrConfigMap.put(username, userLoginErrConfig);
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private static PolarLoginErrConfig parseDefaultConfig(JSONObject defaultConfig) {
        UserLoginErrConfig config = parseUserConfig(defaultConfig);
        return new PolarLoginErrConfig(config);
    }

    private static UserLoginErrConfig parseUserConfig(JSONObject userConfig) {
        int passwordMaxErrorCount = getPasswordMaxErrorCount(userConfig);
        long expireSeconds = getExpireSeconds(userConfig);
        Date passwordExpireDate = getPasswordExpireDate(userConfig);
        return new UserLoginErrConfig(passwordMaxErrorCount, expireSeconds, passwordExpireDate);
    }

    private static int getPasswordMaxErrorCount(JSONObject userConfig) {
        int passwordMaxErrorCount = userConfig.getIntValue(PASSWORD_MAX_ERROR_COUNT_KEY);
        if (passwordMaxErrorCount <= 0) {
            passwordMaxErrorCount = 0;
        }
        return passwordMaxErrorCount;
    }

    private static long getExpireSeconds(JSONObject userConfig) {
        long expireSeconds = userConfig.getLongValue(EXPIRE_SECONDS_KEY);
        if (expireSeconds <= 0) {
            expireSeconds = 0;
        }
        return expireSeconds;
    }

    private static Date getPasswordExpireDate(JSONObject userConfig) {
        Date passwordExpireDate = null;
        String passwordExpireDateStr = userConfig.getString(PASSWORD_EXPIRE_DATE_KEY);
        if (passwordExpireDateStr != null) {
            try {
                passwordExpireDate = EXPIRE_DATE_FORMAT.parse(passwordExpireDateStr);
            } catch (ParseException e) {
                logger.warn("Invalid user login error config: " + userConfig, e);
            }
        }
        return passwordExpireDate;
    }

    public int getPasswordMaxErrorCount(String username) {
        UserLoginErrConfig config = getUserLoginErrConfig(username);
        return config.passwordMaxErrorCount;
    }

    public long getExpireSeconds(String username) {
        UserLoginErrConfig config = getUserLoginErrConfig(username);
        return config.expireSeconds;
    }

    public Date getPasswordExpireDate(String username) {
        UserLoginErrConfig config = getUserLoginErrConfig(username);
        return config.passwordExpireDate;
    }

    private UserLoginErrConfig getUserLoginErrConfig(String username) {
        UserLoginErrConfig config = userLoginErrConfigMap.get(username);
        if (config == null) {
            config = DEFAULT_LOGIN_ERR_CONFIG;
        }
        return config;
    }

    public String getPasswordExpireDateString(String username) {
        Date passwordExpireDate = getPasswordExpireDate(username);
        if (passwordExpireDate == null) {
            return "";
        }
        return EXPIRE_DATE_FORMAT.format(passwordExpireDate);
    }
}
