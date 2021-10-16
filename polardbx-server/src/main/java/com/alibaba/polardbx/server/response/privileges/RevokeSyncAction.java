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
import com.taobao.tddl.common.privilege.PrivilegeLevel;
import com.taobao.tddl.common.privilege.RevokeParameter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;

import java.text.MessageFormat;

/**
 * revoke同步Action
 *
 * @author arnkore 2016-10-25 17:13
 */
public class RevokeSyncAction extends AbstractAuthorizeSyncAction implements ISyncAction {
    private static final Logger logger = LoggerFactory.getLogger(RevokeSyncAction.class);

    private RevokeParameter revokeParameter;

    public RevokeSyncAction() {
    }

    public RevokeSyncAction(RevokeParameter revokeParameter) {
        this.revokeParameter = revokeParameter;
    }

    public RevokeParameter getRevokeParameter() {
        return revokeParameter;
    }

    public void setRevokeParameter(RevokeParameter revokeParameter) {
        this.revokeParameter = revokeParameter;
    }

    @Override
    public ResultCursor sync() {
        AuthorizeConfig ac = CobarServer.getInstance().getConfig().getAuthorizeConfig();

        try {
            if (revokeParameter.getPrivilegeLevel() == PrivilegeLevel.TABLE) {
                ac.removeTbPriv(revokeParameter);
            } else if (revokeParameter.getPrivilegeLevel() == PrivilegeLevel.DATABASE) {
                ac.removeDbPriv(revokeParameter);
            }

            logger.info(MessageFormat.format("Synchronize privilege cache of revoke statement: ''{0}'' success!",
                revokeParameter.toString()));
        } catch (Exception e) {
            logger.error(MessageFormat
                .format("Synchronize privilege cache of revoke statement: ''{0}'' fail! The reason is ''{1}''",
                    revokeParameter.toString(), e.getMessage()), e);
            throw GeneralUtil.nestedException(e);
        }

        return null;
    }
}
