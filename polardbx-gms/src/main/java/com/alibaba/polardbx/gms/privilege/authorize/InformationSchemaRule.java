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

import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PrivilegeScope;

/**
 * Checks whether user has access to information schema.
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public class InformationSchemaRule extends AbstractMatchableRule {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean matches(PolarAccountInfo accountInfo, Permission permission) {
        return
            ((permission.getScope() == PrivilegeScope.DATABASE) || (permission.getScope() == PrivilegeScope.TABLE))
                && PolarPrivUtil.INFORMATION_SCHEMA
                .equalsIgnoreCase(permission.getDatabase());
    }

    /**
     * Everyone has privileges to view contents of information schema, but can't change it. {@inheritDoc}
     */
    @Override
    protected boolean doCheckOnMatch(PolarAccountInfo accountInfo, Permission permission) {
        return permission.getPrivilege() == PrivilegeKind.SELECT;
    }
}
