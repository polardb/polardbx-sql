package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.enums.TeeType;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbServer;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;

/**
 * @author pangzhaoxing
 */
public class EncdbServerInfoGetHandler implements EncdbHandler {
    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) {
        String version = request.getString(MsgKeyConstants.VERSION);
        JSONObject response = new JSONObject();
        response.put(MsgKeyConstants.MEKID, EncdbKeyManager.getInstance().getMekId());
        JSONObject serverInfo = new JSONObject();
        serverInfo.put(MsgKeyConstants.VERSION, EncdbServer.ENCDB_VERSION);
        serverInfo.put(MsgKeyConstants.TEE_TYPE, TeeType.MOCK);
        serverInfo.put(MsgKeyConstants.CIPHER_SUITE, EncdbServer.getInstance().getCipherSuite().toString());
        serverInfo.put(MsgKeyConstants.PUBLIC_KEY, EncdbServer.getInstance().getPemPublicKey());
        serverInfo.put(MsgKeyConstants.PUBLIC_KEY_HASH, EncdbServer.getInstance().getPublicKeyHash());
        serverInfo.put(MsgKeyConstants.MRENCLAVE, EncdbServer.getInstance().getEnclaveId());
        response.put(MsgKeyConstants.SERVER_INFO, serverInfo);
        return response;
    }

}
