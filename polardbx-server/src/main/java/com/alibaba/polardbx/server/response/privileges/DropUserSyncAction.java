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

package com.alibaba.polardbx.server.response.privileges;

import com.alibaba.polardbx.AuthorizeConfig;
import com.alibaba.polardbx.CobarServer;
import com.taobao.tddl.common.privilege.GrantedUser;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;

import java.util.List;

/**
 * 执行DROP USER语句时需要移除下列缓存
 * BaseAppLoader.drdsUsers
 *
 * @author arnkore 2016-11-05 12:57
 */
public class DropUserSyncAction extends AbstractAuthorizeSyncAction implements ISyncAction {
    private List<GrantedUser> grantedUsers;

    public DropUserSyncAction() {
    }

    public DropUserSyncAction(List<GrantedUser> grantedUsers) {
        this.grantedUsers = grantedUsers;
    }

    public List<GrantedUser> getGrantedUsers() {
        return grantedUsers;
    }

    public void setGrantedUsers(List<GrantedUser> grantedUsers) {
        this.grantedUsers = grantedUsers;
    }

    @Override
    public ResultCursor sync() {
        AuthorizeConfig ac = CobarServer.getInstance().getConfig().getAuthorizeConfig();
        for (GrantedUser user : grantedUsers) {
            ac.removeUser(user.getUser(), user.getHost());
        }

        return null;
    }
}
