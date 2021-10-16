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

package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.manager.handler.KillQueryHandler;
import com.alibaba.polardbx.manager.handler.LockTransHandler;
import com.alibaba.polardbx.manager.handler.ManagerSelectHandler;
import com.alibaba.polardbx.manager.handler.SetConfigHandler;
import com.alibaba.polardbx.manager.handler.ShowHandler;
import com.alibaba.polardbx.manager.handler.SyncHandler;
import com.alibaba.polardbx.manager.parser.ManagerParse;
import com.alibaba.polardbx.manager.response.KillConnection;
import com.alibaba.polardbx.manager.response.KillIdle;
import com.alibaba.polardbx.manager.response.Offline;
import com.alibaba.polardbx.manager.response.Online;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.QueryHandler;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.handler.DbStatusHandler;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public class ManagerQueryHandler implements QueryHandler {

    private static final Logger     logger = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private final ManagerConnection source;

    public ManagerQueryHandler(ManagerConnection source){
        this.source = source;
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder().append(sql).toString());
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SELECT:
                ManagerSelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.SET:
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
                break;
            case ManagerParse.SET_CONFIG:
                SetConfigHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> 8, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(sql, c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(sql, c);
                break;
            case ManagerParse.KILL_QUERY:
                KillQueryHandler.handle(sql, rs >>> 8, c);
                break;
            case ManagerParse.KILL_IDLE:
                KillIdle.response(sql, rs >>> 8, c);
                break;
            case ManagerParse.SYNC:
                SyncHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.SET_READ_ONLY:
                DbStatusHandler.handle(sql, c);
                break;
            case ManagerParse.LOCK_TRANS:
                LockTransHandler.lock(sql, c);
                break;
            case ManagerParse.UNLOCK_TRANS:
                LockTransHandler.unlock(sql, c);
                break;
            default: {
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            }
        }
    }
}
