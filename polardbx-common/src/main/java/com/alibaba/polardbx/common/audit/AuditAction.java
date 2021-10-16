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

package com.alibaba.polardbx.common.audit;

import java.util.HashMap;

public enum AuditAction {
    LOGIN, LOGOUT, LOGIN_ERR, CREATE_USER, DROP_USER, GRANT, REVOKE, GRANT_ROLE, REVOKE_ROLE, CREATE_ROLE, DROP_ROLE,
    SET_PASSWORD, CREATE, DROP, TRUNCATE, PURGE, ALTER, SET_DEFAULT_ROLE;

    private static HashMap<String, AuditAction> cache = new HashMap<>();

    static {
        AuditAction[] values = AuditAction.values();
        HashMap<String, AuditAction> cache = new HashMap<>();
        for (int i = 0; i < values.length; i++) {
            AuditAction value = values[i];
            cache.put(value.name(), value);
        }
        AuditAction.cache = cache;
    }

    public static AuditAction value(String name) {
        AuditAction result = AuditAction.cache.get(name);
        return result;
    }
}