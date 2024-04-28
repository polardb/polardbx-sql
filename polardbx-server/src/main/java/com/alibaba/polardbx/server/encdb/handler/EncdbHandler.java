package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author pangzhaoxing
 */
public interface EncdbHandler {

    JSONObject EMPTY = new JSONObject();

    JSONObject handle(JSONObject request, ServerConnection serverConnection) throws Exception;

}
