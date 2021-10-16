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

import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;

/**
 * If input matches, the check result is determined by itself, otherwise by next rule.
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public abstract class AbstractMatchableRule extends AbstractRule {

    /**
     * Whether input matches rule condition.
     *
     * @param accountInfo Account to check.
     * @param permission Permission to check.
     * @return Whether matches.
     * @see #check
     */
    protected abstract boolean matches(PolarAccountInfo accountInfo, Permission permission);

    /**
     * Implements check logic when matches.
     *
     * @param accountInfo Account to check.
     * @param permission Permission to check.
     * @return Whether has enough permission.
     */
    protected abstract boolean doCheckOnMatch(PolarAccountInfo accountInfo, Permission permission);

    @Override
    public boolean check(PolarAccountInfo accountInfo, Permission permission) {
        if (matches(accountInfo, permission)) {
            return doCheckOnMatch(accountInfo, permission);
        } else {
            return doCheckByNextRule(accountInfo, permission);
        }
    }

    private boolean doCheckByNextRule(PolarAccountInfo accountInfo, Permission permission) {
        return getNext().map(rule -> rule.check(accountInfo, permission))
            .orElse(false);
    }
}
