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

/**
 * password error limit rule
 *
 * @author hongxi.chx
 */
public class PolarLoginErrConfig {

    private int passwordMaxErrorCount;
    private long expireSeconds;

    public PolarLoginErrConfig(int passwordMaxErrorCount, long expireSeconds) {
        this.passwordMaxErrorCount = passwordMaxErrorCount;
        this.expireSeconds = expireSeconds;
    }

    public static PolarLoginErrConfig parse(JSONObject config) {

        if (config == null) {
            return new PolarLoginErrConfig(0, 0);
        }
        int passwordMaxErrorCount = config.getIntValue("passwordMaxErrorCount");
        long expireSeconds = config.getLongValue("expireSeconds");
        if (expireSeconds <= 0 || passwordMaxErrorCount <= 0) {
            return new PolarLoginErrConfig(0, 0);
        }
        return new PolarLoginErrConfig(passwordMaxErrorCount, expireSeconds);
    }

    public int getPasswordMaxErrorCount() {
        return passwordMaxErrorCount;
    }

    public long getExpireSeconds() {
        return expireSeconds;
    }
}
