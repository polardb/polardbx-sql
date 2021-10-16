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

package com.alibaba.polardbx.common.model.privilege;

import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.GrantedUser;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DbUserInfo extends Privs {

    String userName;

    String dbName;

    String host;

    @Builder
    public DbUserInfo(boolean insertPriv, boolean updatePriv, boolean deletePriv, boolean selectPriv,
                      boolean createPriv,
                      boolean dropPriv, boolean alterPriv, boolean indexPriv, boolean grantPriv, String userName,
                      String dbName, String host) {
        super(insertPriv, updatePriv, deletePriv, selectPriv, createPriv, dropPriv, alterPriv, indexPriv, grantPriv);
        this.userName = userName;
        this.dbName = dbName;
        this.host = host;
    }

    @Override
    public GrantParameter toGrantParameter(String instId) {
        GrantParameter result = super.toGrantParameter(instId);
        GrantedUser grantedUser = new GrantedUser(instId, dbName, userName, host);
        result.setDatabase(dbName);
        result.setGrantedUsers(Lists.newArrayList(grantedUser));
        return result;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
