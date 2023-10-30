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

package com.alibaba.polardbx.net.handler;

import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * 前端命令处理器
 *
 * @author xianmao.hexm
 */
public class FrontendCommandHandler implements NIOHandler {

    private static final Logger logger = LoggerFactory.getLogger(FrontendCommandHandler.class);

    protected final FrontendConnection source;
    protected final CommandCount commandCount;

    public FrontendCommandHandler(FrontendConnection source) {
        this.source = source;
        this.commandCount = source.getProcessor().getCommands();
    }

    /**
     * @param data requests on one connection must be processed sequentially
     */
    @Override
    synchronized public void handle(byte[] data) {
        source.buildMDC();
        source.setPacketId((byte) (0 & 0xff));
        source.setLastActiveTime(System.nanoTime());
        source.setSqlBeginTimestamp(System.currentTimeMillis());

        switch (data[4]) {
        case Commands.COM_INIT_DB:
            commandCount.doInitDB();
            source.initDB(data);
            break;
        case Commands.COM_PING:
            commandCount.doPing();
            source.ping();
            break;
        case Commands.COM_QUIT:
            commandCount.doQuit();
            source.close();
            break;
        case Commands.COM_QUERY:
            commandCount.doQuery();
            source.query(data);
            break;
        case Commands.COM_PROCESS_KILL:
            commandCount.doKill();
            source.close();
            break;
        case Commands.COM_STMT_PREPARE:
            commandCount.doStmtPrepare();
            source.stmtPrepare(data);
            break;
        case Commands.COM_STMT_EXECUTE:
            commandCount.doStmtExecute();
            source.stmtExecute(data);
            break;
        case Commands.COM_STMT_CLOSE:
            commandCount.doStmtClose();
            source.stmtClose(data);
            break;
        case Commands.COM_STMT_SEND_LONG_DATA:
            commandCount.doStmtSendLongData();
            source.stmtSendLongData(data);
            break;
        case Commands.COM_STMT_RESET:
            commandCount.doStmtReset();
            source.stmtReset(data);
            break;
        case Commands.COM_FIELD_LIST:
            commandCount.doFieldList();
            source.fieldList(data);
            break;
        case Commands.COM_SET_OPTION:
            commandCount.doSetOption();
            source.setOption(data);
            break;
        case Commands.COM_STMT_FETCH:
            commandCount.doStmtFetch();
            source.stmtFetch(data);
            break;
        case Commands.COM_REFRESH:
            PacketOutputProxyFactory.getInstance().createProxy(source).writeArrayAsPacket(OkPacket.OK);
            break;
        case Commands.COM_CHANGE_USER:
            PacketOutputProxyFactory.getInstance().createProxy(source).writeArrayAsPacket(OkPacket.OK);
            break;
        case Commands.COM_RESET_CONNECTION:
            PacketOutputProxyFactory.getInstance().createProxy(source).writeArrayAsPacket(OkPacket.OK);
            break;
        case Commands.COM_REGISTER_SLAVE:
            source.setRegisterSlave(true);
            PacketOutputProxyFactory.getInstance().createProxy(source).writeArrayAsPacket(OkPacket.OK);
            break;
        case Commands.COM_BINLOG_DUMP:
            source.binlogDump(data);
            break;
        default:
            logger.error("Unknown command: " + data[4]);
            commandCount.doOther();
            source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }
}
