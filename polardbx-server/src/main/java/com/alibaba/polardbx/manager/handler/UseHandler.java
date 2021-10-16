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

import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;

/**
 * @author xianmao.hexm
 */
public final class UseHandler {

    public static void handle(String sql, ManagerConnection c, int offset) {
        String schema = sql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
            if (schema.charAt(0) == '`' && schema.charAt(length - 1) == '`') {
                schema = schema.substring(1, length - 1);
            }
        }

        c.setSchema(schema);
        c.updateMDC();
        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }

}
