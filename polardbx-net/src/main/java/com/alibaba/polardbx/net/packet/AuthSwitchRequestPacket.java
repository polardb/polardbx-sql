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

import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.BufferUtil;

/**
 * Created by chuanqin on 19/5/8.
 */
public class AuthSwitchRequestPacket extends MySQLPacket {

    public String pluginName;
    public byte[] seed;

    public AuthSwitchRequestPacket(String pluginName, byte[] seed, byte packetId) {
        this.pluginName = pluginName;
        this.seed = seed;
        this.packetId = packetId;
    }

    @Override
    protected String packetInfo() {
        return "AuthMethodSwitchRequestPacket";
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) {
        proxy.packetBegin();
        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);
        proxy.write((byte) 0xfe);
        if (pluginName == null) {
            proxy.writeWithNull(new byte[0]);
        } else {
            proxy.writeWithNull(pluginName.getBytes());
        }
        proxy.writeWithNull(seed);
        // proxy.write((byte) 0);
        proxy.packetEnd();
        return proxy;
    }

    protected int getPacketLength() {
        int size = 1;//
        size += (pluginName == null) ? 1 : pluginName.length() + 1;
        size += (seed == null) ? 1 : BufferUtil.getLength(seed);
        return size;
    }
}
