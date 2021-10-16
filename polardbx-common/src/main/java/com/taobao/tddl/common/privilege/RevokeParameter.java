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

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.base.Joiner;


public class RevokeParameter implements Serializable {

    private static final long serialVersionUID = -3603094430166749112L;

    private static final char MULTI_USER_SEP = ',';

    private SortedSet<PrivilegePoint> privs;

    private PrivilegeLevel privilegeLevel;

    private String instanceId;

    private String database;

    private String table;

    private List<GrantedUser> grantedUsers;

    private String revokerUser;

    private String revokerHost;

    private boolean removeGrantOption;

    private boolean isRevokeAll;

    @Override
    public String toString() {
        StringBuilder appendable = new StringBuilder();
        if (isRevokeAll) {
            appendable.append("REVOKE ALL PRIVILEGES, GRANT OPTION FROM ");
            appendable.append(genFromPart());
        } else {
            appendable.append("REVOKE ");
            appendable.append(genGrantPartString());
            appendable.append(" ON ");
            appendable.append(genOnPartString());
            appendable.append(" FROM ");
            appendable.append(genFromPart());
        }
        return appendable.toString();
    }

    public String genGrantPartString() {

        if (PrivilegeUtil.isAllPrivs(privs, privilegeLevel)) {
            return "ALL PRIVILEGES";
        }

        StringBuilder appendable = new StringBuilder();
        appendable.append(Joiner.on(",").join(privs).trim().toString());
        if (removeGrantOption) {
            appendable.append(",GRANT OPTION");
        }

        return appendable.toString();
    }

    public String genOnPartString() {
        if (PrivilegeLevel.GLOBAL == privilegeLevel) {
            return "*.*";
        } else if (PrivilegeLevel.DATABASE == privilegeLevel) {
            return database + ".*";
        } else if (PrivilegeLevel.TABLE == privilegeLevel) {
            return database == null ? table : database + "." + table;
        } else {
            throw new IllegalArgumentException("Unsupported privilege level!");
        }
    }

    public String genFromPart() {
        StringBuilder appendable = new StringBuilder();
        Iterator<GrantedUser> grantedUserIter = grantedUsers.iterator();
        while (grantedUserIter.hasNext()) {
            GrantedUser grantedUser = grantedUserIter.next();
            appendable.append(grantedUser.toString());

            if (grantedUserIter.hasNext()) {
                appendable.append(MULTI_USER_SEP);
            }
        }

        return appendable.toString();
    }

    public RevokeParameter deepCopy() {
        RevokeParameter revokeParameter = new RevokeParameter();
        revokeParameter.setPrivilegeLevel(getPrivilegeLevel());
        revokeParameter.setRevokerUser(getRevokerUser());
        revokeParameter.setPrivs(getPrivs());
        revokeParameter.setGrantedUsers(getGrantedUsers());
        revokeParameter.setRevokeAll(isRevokeAll);
        revokeParameter.setInstanceId(getInstanceId());
        revokeParameter.setRemoveGrantOption(isRemoveGrantOption());
        revokeParameter.setRevokerHost(getRevokerHost());
        revokeParameter.setDatabase(getDatabase());
        revokeParameter.setTable(getTable());
        return revokeParameter;
    }

    public SortedSet<PrivilegePoint> getPrivs() {
        return privs;
    }

    public void setPrivs(SortedSet<PrivilegePoint> privs) {
        this.privs = privs;
    }

    public PrivilegeLevel getPrivilegeLevel() {
        return privilegeLevel;
    }

    public void setPrivilegeLevel(PrivilegeLevel privilegeLevel) {
        this.privilegeLevel = privilegeLevel;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = TStringUtil.normalizePriv(table);
    }

    public List<GrantedUser> getGrantedUsers() {
        return Collections.unmodifiableList(grantedUsers);
    }

    public void setGrantedUsers(List<GrantedUser> grantedUsers) {
        this.grantedUsers = grantedUsers;
    }

    public String getRevokerUser() {
        return revokerUser;
    }

    public void setRevokerUser(String revokerUser) {
        this.revokerUser = revokerUser;
    }

    public String getRevokerHost() {
        return revokerHost;
    }

    public void setRevokerHost(String revokerHost) {
        this.revokerHost = revokerHost;
    }

    public boolean isRemoveGrantOption() {
        return removeGrantOption;
    }

    public void setRemoveGrantOption(boolean removeGrantOption) {
        this.removeGrantOption = removeGrantOption;
    }

    public boolean isRevokeAll() {
        return isRevokeAll;
    }

    public void setRevokeAll(boolean revokeAll) {
        isRevokeAll = revokeAll;
    }
}
