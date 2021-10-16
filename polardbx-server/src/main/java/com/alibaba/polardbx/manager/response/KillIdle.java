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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * 干掉没有正在执行sql的连接
 *
 * @author mengshi.sunmengshi 2014年11月25日 下午1:58:44
 * @since 5.1.14
 */
public final class KillIdle {

    private static final Logger logger = LoggerFactory.getLogger(KillIdle.class);

    public static void response(String stmt, int offset, ManagerConnection mc) {
        int count = 0;
        for (NIOProcessor processor : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection conn : processor.getFrontends().values()) {
                if (conn instanceof ServerConnection) {
                    if (((ServerConnection) conn).isStatementExecuting().get()) {
                        continue;
                    } else {
                        conn.close();
                        StringBuilder s = new StringBuilder();
                        logger.info(s.append(conn).append("killed by manager").toString());
                        count++;
                    }
                }
            }
        }

        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = count;
        packet.serverStatus = 2;
        packet.write(PacketOutputProxyFactory.getInstance().createProxy(mc));
    }

}
