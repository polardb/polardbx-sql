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
import com.alibaba.polardbx.gms.privilege.PolarDbPriv;
import com.alibaba.polardbx.gms.privilege.PolarTbPriv;

import java.util.Optional;

/**
 * This rule implements default permission check strategy. This is usually the last rule in a rule chain.
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public class GeneralRule extends AbstractRule {
    private static boolean hasAnyPermission(PolarAccountInfo accountInfo, Permission permission) {
        switch (permission.getScope()) {
        case INSTANCE:
            return accountInfo.getInstPriv().hasUsagePriv() ||
                accountInfo.getDbPrivMap().values().stream().anyMatch(PolarDbPriv::hasUsagePriv) ||
                accountInfo.getTbPrivMap().values().stream().anyMatch(PolarTbPriv::hasUsagePriv);
        case DATABASE:
            return accountInfo.hasUsageOnDb(permission.getDatabase());
        case TABLE:
            return accountInfo.hasUsageOnTb(permission.getDatabase(), permission.getTable());
        default:
            throw new IllegalArgumentException("Unrecognized privilege scope: " + permission.getScope());
        }
    }

    @Override
    public boolean check(PolarAccountInfo accountInfo, Permission permission) {
        if (permission.isAnyPermission()) {
            return hasAnyPermission(accountInfo, permission);
        }

        if (accountInfo.getInstPriv().hasPrivilege(permission.getPrivilege())) {
            return true;
        }

        boolean hasDatabasePrivilege = Optional.ofNullable(permission.getDatabase())
            .flatMap(db -> Optional.ofNullable(accountInfo.getDbPriv(db)))
            .map(dbPrivilege -> dbPrivilege.hasPrivilege(permission.getPrivilege()))
            .orElse(false);
        if (hasDatabasePrivilege) {
            return true;
        }

        boolean hasTablePrivilege = Optional.ofNullable(permission.getTable())
            .flatMap(table -> Optional.ofNullable(accountInfo.getTbPriv(permission.getDatabase(),
                permission.getTable())))
            .map(tablePrivilege -> tablePrivilege.hasPrivilege(permission.getPrivilege()))
            .orElse(false);
        if (hasTablePrivilege) {
            return true;
        }

        return false;
    }
}
