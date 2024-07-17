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
