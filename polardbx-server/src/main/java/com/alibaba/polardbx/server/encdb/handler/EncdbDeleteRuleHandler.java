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

package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;

/**
 * @author pangzhaoxing
 */
public class EncdbDeleteRuleHandler implements EncdbHandler {
    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) throws Exception {
        EncdbMsgProcessor.checkUserPrivileges(serverConnection, true);
        String ruleName = request.getString(MsgKeyConstants.NAME);
        EncdbRuleManager.getInstance().deleteEncRule(ruleName);
        return EMPTY;
    }

}
