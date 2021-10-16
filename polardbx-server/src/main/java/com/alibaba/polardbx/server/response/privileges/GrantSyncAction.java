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
import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.PrivilegeLevel;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;

import java.text.MessageFormat;

/**
 * 同步本机数据库级和表级权限缓存, 同步Action来自Grant授权语句.
 *
 * @author arnkore 2016-10-25 17:30
 */
public class GrantSyncAction extends AbstractAuthorizeSyncAction implements ISyncAction {
    private static final Logger logger = LoggerFactory.getLogger(GrantSyncAction.class);

    private GrantParameter grantParameter;

    public GrantSyncAction() {
    }

    public GrantSyncAction(GrantParameter grantParameter) {
        this.grantParameter = grantParameter;
    }

    public GrantParameter getGrantParameter() {
        return grantParameter;
    }

    public void setGrantParameter(GrantParameter grantParameter) {
        this.grantParameter = grantParameter;
    }

    @Override
    public ResultCursor sync() {
        AuthorizeConfig ac = CobarServer.getInstance().getConfig().getAuthorizeConfig();
        try {
            if (grantParameter.getPrivilegeLevel() == PrivilegeLevel.TABLE) {
                ac.addTbPriv(grantParameter);
            } else if (grantParameter.getPrivilegeLevel() == PrivilegeLevel.DATABASE) {
                ac.addDbPriv(grantParameter);
            }

            logger.info(MessageFormat
                .format("Synchronize privilege cache of grant statement: ''{0}'' success in this node!",
                    grantParameter.toString()));
        } catch (Exception e) {
            logger.error(MessageFormat
                .format("Synchronize privilege cache of grant statement: ''{0}'' fail! The reason is ''{1}''",
                    grantParameter.toString(), e.getMessage()), e);
            throw GeneralUtil.nestedException(e);
        }

        return null;
    }
}
