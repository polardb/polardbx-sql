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

package com.alibaba.polardbx.server.session;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * 由前后端参与的一次执行会话过程
 *
 * @author xianmao.hexm
 */
public final class ServerSession {

    private final ServerConnection source;

    public ServerSession(ServerConnection source) {
        this.source = source;

    }

    public ServerConnection getSource() {
        return source;
    }

    /**
     * 撤销执行中的会话
     */
    public void cancel(FrontendConnection sponsor) {
        // TODO terminate session
        source.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "Query execution was interrupted");
        if (sponsor != null) {
            OkPacket packet = new OkPacket();
            packet.packetId = 1;
            packet.affectedRows = 0;
            packet.serverStatus = 2;
            packet.write(PacketOutputProxyFactory.getInstance().createProxy(sponsor));
        }
    }

}
