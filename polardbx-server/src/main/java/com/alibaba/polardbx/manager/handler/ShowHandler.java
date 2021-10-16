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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.manager.parser.ManagerParseShow;
import com.alibaba.polardbx.manager.response.*;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseShow.parse(stmt, offset);
        switch (rs & 0xff) {
        case ManagerParseShow.COMMAND:
            ShowCommand.execute(c);
            break;
        case ManagerParseShow.COLLATION:
            ShowCollation.execute(c);
            break;
        case ManagerParseShow.CONNECTION:
            ShowConnection.execute(c);
            break;
        case ManagerParseShow.CONNECTION_SQL:
            ShowConnectionSQL.execute(c);
            break;
        case ManagerParseShow.DATABASE:
            ShowDatabase.execute(c);
            break;
        case ManagerParseShow.DATASOURCE:
            ShowDataSource.execute(c, null);
            break;
        case ManagerParseShow.HELP:
            ShowHelp.execute(c);
            break;
        case ManagerParseShow.PROCESSOR:
            ShowProcessor.execute(c);
            break;
        case ManagerParseShow.STATS:
            ShowStats.execute(c);
            break;
        case ManagerParseShow.SERVER:
            ShowServer.execute(c);
            break;
        case ManagerParseShow.SLOW:
            ShowSQLSlow.execute(c);
            break;
        case ManagerParseShow.CCL_STATS:
            ShowCclStats.execute(c);
            break;
        case ManagerParseShow.THREADPOOL:
            ShowThreadPool.execute(c);
            break;
        case ManagerParseShow.MEMORYPOOL:
            ShowMemoryPool.execute(c);
            break;
        case ManagerParseShow.TIME_CURRENT:
            ShowTime.execute(c, ManagerParseShow.TIME_CURRENT);
            break;
        case ManagerParseShow.TIME_STARTUP:
            ShowTime.execute(c, ManagerParseShow.TIME_STARTUP);
            break;
        case ManagerParseShow.VARIABLES:
            ShowVariables.execute(c);
            break;
        case ManagerParseShow.VERSION:
            ShowVersion.execute(c);
            break;
        case ManagerParseShow.CONFIG:
            ShowConfig.execute(c);
            break;
        case ManagerParseShow.STC:
            ShowStc.execute(c);
            break;
        case ManagerParseShow.HTC:
            ShowHtc.execute(c);
            break;
        case ManagerParseShow.LEADER:
            ShowLeader.execute(c);
            break;
        case ManagerParseShow.TRANS_LOCK:
            LockTransHandler.show(c);
            break;
        default:
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}
