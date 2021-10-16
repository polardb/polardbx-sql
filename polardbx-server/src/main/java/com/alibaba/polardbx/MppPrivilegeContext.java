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

package com.alibaba.polardbx;

import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;

import java.util.Map;

public class MppPrivilegeContext extends PrivilegeContext {

    @Override
    public Privileges getPrivileges() {
        if (privileges == null) {
            privileges = new CobarPrivileges();
        }
        return privileges;
    }

    @Override
    public Map<String, DbPriv> getDatabasePrivilegeMap() {
        if (databasePrivilegeMap == null) {
            databasePrivilegeMap = this.getPrivileges().getSchemaPrivs(this.getUser(), this.getHost());
        }
        return databasePrivilegeMap;
    }

    @Override
    public DbPriv getDatabasePrivilege() {
        if (databasePrivilege == null) {
            databasePrivilege = this.getDatabasePrivilegeMap().get(TStringUtil.normalizePriv(this.getSchema()));
        }
        return databasePrivilege;
    }

    @Override
    public Map<String, TbPriv> getTablePrivilegeMap() {
        if (tablePrivilegeMap == null) {
            tablePrivilegeMap = this.getPrivileges()
                .getTablePrivs(this.getUser(), this.getHost(), TStringUtil.normalizePriv(this.getSchema()));
        }
        return tablePrivilegeMap;
    }
}
