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

package com.alibaba.polardbx.manager.handler;

import org.apache.commons.lang.StringUtils;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;

/**
 * @author xianmao.hexm
 */
public final class SetConfigHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        String var = stmt.substring(offset + 1).trim();
        String[] vars = StringUtils.split(var, "=");
        if (vars.length != 2) {
            c.writeErrMessage(ErrorCode.ER_YES, "unknow set : " + var);
        }

        String key = StringUtils.trim(vars[0]);
        String value = StringUtils.trim(vars[1]);
        if (StringUtils.equalsIgnoreCase(value, "null")) {
            value = null;
        }

        CobarServer.getInstance().getConfig().getSystem().setConfig(key, value);
        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }
}
