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

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.net.handler.Privileges;

import java.util.Set;

/**
 * @author fangwu
 */
public class PrivilegeUtil {

    /**
     * check user privileges
     * return null if check passed; return ErrorCode if not.
     */
    public static ErrorCode checkSchema(String schema, String user, String host, boolean trustLogin,
                                        Privileges privileges) {
        if (schema == null) {
            return null;
        }

        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }

        if (trustLogin || ConfigDataMode.isFastMock()) {
            return null;
        }

        Set<String> schemas = privileges.getUserSchemas(user, host);
        if (schemas != null && schemas.contains(schema)) {
            return null;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }
}
