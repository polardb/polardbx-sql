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

import com.alibaba.polardbx.common.privilege.Host;
import com.alibaba.polardbx.common.privilege.PrivilegeUtil;

import java.io.Serializable;

public class GrantedUser implements Serializable, Cloneable {
    private static final long serialVersionUID = 983518205339287788L;

    public static final String GRANTOR_SEP = "@";

    private String instanceId;

    private String database;

    private String user;

    private String host;

    private EncrptPassword encrptPassword;

    private String grantor;

    public GrantedUser() {
    }

    public GrantedUser(String user, String host) {
        this.user = user;
        this.host = host;
    }

    public GrantedUser(String user, String host, EncrptPassword encrptPassword) {
        this.user = user;
        this.host = host;
        this.encrptPassword = encrptPassword;
    }

    public GrantedUser(String instanceId, String database, String user, String host) {
        this.instanceId = instanceId;
        this.database = database;
        this.user = user;
        this.host = host;
    }

    public GrantedUser(String instanceId, String database, String user, String host, EncrptPassword encrptPassword) {
        this.instanceId = instanceId;
        this.database = database;
        this.user = user;
        this.host = host;
        this.encrptPassword = encrptPassword;
    }

    @Override
    public String toString() {
        String parsedHost = PrivilegeUtil.parseHost(host);

        StringBuilder appendable = new StringBuilder();
        appendable.append(user);
        appendable.append(GRANTOR_SEP);

        if (Host.containsWildcardChars(parsedHost)) {
            appendable.append("'");
        }
        appendable.append(PrivilegeUtil.parseHost(host));
        if (Host.containsWildcardChars(parsedHost)) {
            appendable.append("'");
        }

        return appendable.toString();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
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

    public EncrptPassword getEncrptPassword() {
        return encrptPassword;
    }

    public void setEncrptPassword(EncrptPassword encrptPassword) {
        this.encrptPassword = encrptPassword;
    }

    public String getGrantor() {
        return grantor;
    }

    public void setGrantor(String grantor) {
        this.grantor = grantor;
    }
}
