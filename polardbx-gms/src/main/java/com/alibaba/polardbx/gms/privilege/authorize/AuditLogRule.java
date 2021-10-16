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

import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PrivilegeScope;

import java.util.Set;

/**
 * Check whether user has enough privileges to see audit log.
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public class AuditLogRule extends AbstractMatchableRule {
    private static final String TABLE_AUDIT_LOG = "polardbx_audit_log";
    private static final String TABLE_DDL_LOG = "polardbx_ddl_log";
    private static final Set<String> TABLE_SET = Sets.newHashSet(TABLE_AUDIT_LOG, TABLE_DDL_LOG);
    private static final String DATABASE_INFORMATION_SCHEMA = "information_schema";

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean matches(PolarAccountInfo accountInfo, Permission permission) {
        return PrivilegeScope.TABLE == permission.getScope() &&
            DATABASE_INFORMATION_SCHEMA.equalsIgnoreCase(permission.getDatabase()) &&
            TABLE_SET.contains(permission.getTable().toLowerCase());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doCheckOnMatch(PolarAccountInfo accountInfo, Permission permission) {
        if (PrivilegeKind.SELECT == permission.getPrivilege()) {
            return accountInfo.getInstPriv().hasPrivilege(PrivilegeKind.SHOW_AUDIT_LOG);
        } else {
            return false;
        }
    }
}
