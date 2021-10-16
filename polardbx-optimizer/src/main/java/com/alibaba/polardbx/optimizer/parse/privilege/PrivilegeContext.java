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

package com.alibaba.polardbx.optimizer.parse.privilege;

import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PermissionCheckContext;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 权限上下文
 *
 * @author arnkore 2017-01-17 16:06
 */
public class PrivilegeContext {

    // 用户
    private String user;
    // 主机
    private String host;
    // 当前所属的数据库
    private String schema;
    // 当前所属数据库的权限
    protected DbPriv databasePrivilege;
    // 当前所属数据库下的表级权限
    protected Map<String, TbPriv> tablePrivilegeMap;
    // 当前全部数据库的权限
    protected Map<String, DbPriv> databasePrivilegeMap;

    private List<PrivilegeVerifyItem> privilegeVerifyItems;

    protected Privileges privileges;

    private boolean trustLogin;
    private boolean managed;

    private PolarAccountInfo polarUserInfo;
    private ActiveRoles activeRoles;

    public PrivilegeContext() {

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public DbPriv getDatabasePrivilege() {
        return databasePrivilege;
    }

    public void setDatabasePrivilege(DbPriv databasePrivilege) {
        this.databasePrivilege = databasePrivilege;
    }

    public Map<String, TbPriv> getTablePrivilegeMap() {
        return tablePrivilegeMap;
    }

    public void setTablePrivilegeMap(Map<String, TbPriv> tablePrivilegeMap) {
        this.tablePrivilegeMap = tablePrivilegeMap;
    }

    public Map<String, DbPriv> getDatabasePrivilegeMap() {
        return databasePrivilegeMap;
    }

    public void setDatabasePrivilegeMap(Map<String, DbPriv> databasePrivilegeMap) {
        this.databasePrivilegeMap = databasePrivilegeMap;
    }

    public List<PrivilegeVerifyItem> getPrivilegeVerifyItems() {
        return privilegeVerifyItems;
    }

    public void setPrivilegeVerifyItems(List<PrivilegeVerifyItem> items) {
        this.privilegeVerifyItems = items;
    }

    public void addPrivilegeVerifyItem(PrivilegeVerifyItem item) {
        if (this.privilegeVerifyItems == null) {
            this.privilegeVerifyItems = new ArrayList<>();
        }

        if (!this.containPrivilegeVerifyItem(item)) {
            this.privilegeVerifyItems.add(item);
        }
    }

    public boolean containPrivilegeVerifyItem(PrivilegeVerifyItem item) {
        if (item == null) {
            return true;
        }

        if (this.privilegeVerifyItems == null) {
            return false;
        }

        // privilege modual wont work in mock mode
        if (ConfigDataMode.isFastMock()) {
            return true;
        }

        for (PrivilegeVerifyItem existItem : this.privilegeVerifyItems) {
            if (StringUtils.equalsIgnoreCase(item.getDb(), existItem.getDb())
                && StringUtils.equalsIgnoreCase(item.getTable(), existItem.getTable())
                && item.getPrivilegePoint() == existItem.getPrivilegePoint()) {
                return true;
            }
        }

        return false;
    }

    public void setPrivileges(Privileges privileges) {
        this.privileges = privileges;
    }

    public Privileges getPrivileges() {
        return this.privileges;
    }

    public boolean isTrustLogin() {
        return trustLogin;
    }

    public void setTrustLogin(boolean trustLogin) {
        this.trustLogin = trustLogin;
    }

    public boolean isManaged() {
        return managed;
    }

    public void setManaged(boolean managed) {
        this.managed = managed;
    }

    public void setPolarUserInfo(PolarAccountInfo userInfo) {
        this.polarUserInfo = userInfo;
    }

    public PolarAccountInfo getPolarUserInfo() {
        return this.polarUserInfo;
    }

    public ActiveRoles getActiveRoles() {
        return activeRoles;
    }

    public void setActiveRoles(ActiveRoles activeRoles) {
        this.activeRoles = activeRoles;
    }

    public PermissionCheckContext toPermissionCheckContext(Permission permission) {
        return new PermissionCheckContext(getPolarUserInfo().getAccountId(), getActiveRoles(), permission);
    }
}
