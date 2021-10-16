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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;

/**
 * @author arnkore 2017-01-05 15:52
 */
public class SetPasswordSyncAction extends AbstractAuthorizeSyncAction implements ISyncAction {
    private static final Logger logger = LoggerFactory.getLogger(RevokeSyncAction.class);

    private GrantedUser grantedUser;

    public SetPasswordSyncAction() {
    }

    public SetPasswordSyncAction(GrantedUser grantedUser) {
        this.grantedUser = grantedUser;
    }

    @Override
    public ResultCursor sync() {
        AuthorizeConfig ac = CobarServer.getInstance().getConfig().getAuthorizeConfig();
        try {
            ac.updatePassword(grantedUser);
            logger.info("Synchronize privilege cache of set password statement success!");
        } catch (Exception e) {
            logger.error("Synchronize privilege cache of set password statement fail!", e);
            throw GeneralUtil.nestedException(e);
        }

        return null;
    }

    public GrantedUser getGrantedUser() {
        return grantedUser;
    }

    public void setGrantedUser(GrantedUser grantedUser) {
        this.grantedUser = grantedUser;
    }
}
