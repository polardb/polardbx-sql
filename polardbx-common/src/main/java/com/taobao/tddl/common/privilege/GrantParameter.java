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
import org.apache.commons.collections.CollectionUtils;

import com.google.common.base.Joiner;

public class GrantParameter implements Serializable {

    private static final long serialVersionUID = -863541216900877837L;

    private static final char MULTI_USER_SEP = ',';

    private SortedSet<PrivilegePoint> privs;

    private PrivilegeLevel privilegeLevel;

    private String instanceId;

    private String database;

    private String table;

    private List<GrantedUser> grantedUsers;

    private String grantorUser;

    private String grantorHost;

    private boolean withGrantOption;

    private boolean tracePassword = false;

    @Override
    public String toString() {
        StringBuilder appendable = new StringBuilder();
        appendable.append("GRANT ");
        appendable.append(genGrantPartString());
        appendable.append(" ON " + genOnPartString());
        appendable.append(" TO " + genToPartString());

        if (withGrantOption) {
            appendable.append(" WITH GRANT OPTION");
        }

        return appendable.toString();
    }

    public String genGrantPartString() {

        if (CollectionUtils.isEmpty(privs)) {
            return "USAGE";
        }

        if (PrivilegeUtil.isAllPrivs(privs, privilegeLevel)) {
            return "ALL PRIVILEGES";
        }

        return Joiner.on(",").join(privs).trim().toString();
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

    public String genToPartString() {
        StringBuilder appendable = new StringBuilder();
        Iterator<GrantedUser> grantedUserIter = grantedUsers.iterator();
        while (grantedUserIter.hasNext()) {
            GrantedUser grantedUser = grantedUserIter.next();
            appendable.append(grantedUser.toString());
            EncrptPassword encrptPassword = grantedUser.getEncrptPassword();
            if (tracePassword && encrptPassword != null && encrptPassword.getPassword() != null) {
                appendable.append(" IDENTIFIED BY '" + encrptPassword.getPassword() + "'");
            }

            if (grantedUserIter.hasNext()) {
                appendable.append(MULTI_USER_SEP);
            }
        }
        return appendable.toString();
    }

    public GrantParameter deepCopy() {
        GrantParameter grantParameter = new GrantParameter();
        grantParameter.setWithGrantOption(this.isWithGrantOption());
        grantParameter.setPrivilegeLevel(this.getPrivilegeLevel());
        grantParameter.setGrantorHost(this.getGrantorHost());
        grantParameter.setPrivs(this.getPrivs());
        grantParameter.setGrantedUsers(this.getGrantedUsers());
        grantParameter.setInstanceId(this.getInstanceId());
        grantParameter.setGrantorUser(this.getGrantorUser());
        grantParameter.setTracePassword(this.isTracePassword());
        grantParameter.setDatabase(this.getDatabase());
        grantParameter.setTable(this.getTable());
        return grantParameter;
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

    public String getGrantorUser() {
        return grantorUser;
    }

    public void setGrantorUser(String grantorUser) {
        this.grantorUser = grantorUser;
    }

    public String getGrantorHost() {
        return grantorHost;
    }

    public void setGrantorHost(String grantorHost) {
        this.grantorHost = grantorHost;
    }

    public boolean isWithGrantOption() {
        return withGrantOption;
    }

    public void setWithGrantOption(boolean withGrantOption) {
        this.withGrantOption = withGrantOption;
    }

    public boolean isTracePassword() {
        return tracePassword;
    }

    public void setTracePassword(boolean tracePassword) {
        this.tracePassword = tracePassword;
    }
}
