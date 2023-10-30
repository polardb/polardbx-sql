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

/**
 * (created at 2011-11-22)
 */
package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.NodeStatusManager;

/**
 * @author QIU Shuo
 */
public class Online {

    private static final OkPacket ok = new OkPacket();

    static {
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
    }

    public static void execute(String stmt, ManagerConnection mc) {
        try {
            CobarServer.getInstance().forceOnline();
            NodeStatusManager nodeStatusManager = ServiceProvider.getInstance().getServer().getStatusManager();
            if (nodeStatusManager != null) {
                nodeStatusManager.init();
            }
            EventLogger.log(EventType.ONLINE, "CN is online");
            ok.write(PacketOutputProxyFactory.getInstance().createProxy(mc));
        } catch (Exception ex) {
            mc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, ex.getMessage());
        }
    }

}
