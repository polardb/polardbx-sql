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

package com.alibaba.polardbx.gms.privilege.authorize;

import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;

/**
 * Entry point class for permission check.
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public class PolarAuthorizer {
    private static final Rule HEAD = chainRules(
        new AuditLogRule(),
        new InformationSchemaRule(),
        new GeneralRule()
    );

    /**
     * Entry point for checking whether an account has required permission.
     *
     * <p>
     * It only check current account's privileges. Roles are left out because it's processed by {@link
     * PolarPrivManager#checkPermission}. This method uses a rule chain for checking.
     * </p>
     *
     * @param polarAccountInfo The account with its privileges data associated
     * @param permission Permission to check
     * @return Whether this account has required permission.
     * @see PolarPrivManager#checkPermission
     */
    public static boolean hasPermission(PolarAccountInfo polarAccountInfo, Permission permission) {
        return HEAD.check(polarAccountInfo, permission);
    }

    private static Rule chainRules(Rule... rules) {
        Rule head = rules[0];
        Rule cur = head;
        for (int i = 1; i < rules.length; i++) {
            cur.setNext(rules[i]);
            cur = rules[i];
        }

        return head;
    }
}
