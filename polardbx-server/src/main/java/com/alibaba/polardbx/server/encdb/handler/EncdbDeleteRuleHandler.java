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
