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

package com.alibaba.polardbx.net.packet;

import com.alibaba.polardbx.net.util.MySQLMessage;

/**
 * Created by chuanqin on 19/5/10.
 */
public class AuthSwitchResponsePacket extends MySQLPacket {

    public byte[] password = new byte[0];
    // Only for the authentication method of "mysql_native_password "
    public static final int totalPacketLength = 24;
    public static final int emptyPacketLength = 4;

    public static final int refPacketId = 3;

    public static final int sslRefPacketId = 4;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        // read payload. For mysql_native_password auth method, password length
        // is 20.
        if (packetLength == 20) {
            password = mm.readBytes(20);
        }
    }

    @Override
    protected String packetInfo() {
        return "AuthMethodSwitchRequestPacket";
    }
}
