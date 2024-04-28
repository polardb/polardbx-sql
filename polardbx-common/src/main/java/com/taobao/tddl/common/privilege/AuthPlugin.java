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

package com.taobao.tddl.common.privilege;

import org.apache.commons.lang3.StringUtils;

/**
 * authentication plugin of user password
 */
public enum AuthPlugin {

    /**
     * 1-round sha-1
     */
    POLARDBX_NATIVE_PASSWORD,
    /**
     * 2-round sha-1
     */
    MYSQL_NATIVE_PASSWORD;

    public static AuthPlugin lookupByName(String pluginName) {
        if (StringUtils.isBlank(pluginName)) {
            return POLARDBX_NATIVE_PASSWORD;
        }

        switch (pluginName.toUpperCase()) {
        case "POLARDBX_NATIVE_PASSWORD":
            return POLARDBX_NATIVE_PASSWORD;
        case "MYSQL_NATIVE_PASSWORD":
            return MYSQL_NATIVE_PASSWORD;
        default:
            throw new RuntimeException("Unsupported auth_plugin: " + pluginName);
        }
    }

    public String toLowerCase() {
        return name().toLowerCase();
    }
}
