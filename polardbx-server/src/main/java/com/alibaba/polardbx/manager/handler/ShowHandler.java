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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.manager.parser.ManagerParseShow;
import com.alibaba.polardbx.manager.response.ShowCclStats;
import com.alibaba.polardbx.manager.response.ShowCollation;
import com.alibaba.polardbx.manager.response.ShowCommand;
import com.alibaba.polardbx.manager.response.ShowConfig;
import com.alibaba.polardbx.manager.response.ShowConnection;
import com.alibaba.polardbx.manager.response.ShowConnectionSQL;
import com.alibaba.polardbx.manager.response.ShowDataSource;
import com.alibaba.polardbx.manager.response.ShowDatabase;
import com.alibaba.polardbx.manager.response.ShowDiscardLog;
import com.alibaba.polardbx.manager.response.ShowHelp;
import com.alibaba.polardbx.manager.response.ShowHtc;
import com.alibaba.polardbx.manager.response.ShowLeader;
import com.alibaba.polardbx.manager.response.ShowMemoryPool;
import com.alibaba.polardbx.manager.response.ShowProcessor;
import com.alibaba.polardbx.manager.response.ShowSQLSlow;
import com.alibaba.polardbx.manager.response.ShowServer;
import com.alibaba.polardbx.manager.response.ShowServerExecutor;
import com.alibaba.polardbx.manager.response.ShowSqlLog;
import com.alibaba.polardbx.manager.response.ShowStats;
import com.alibaba.polardbx.manager.response.ShowStc;
import com.alibaba.polardbx.manager.response.ShowThreadPool;
import com.alibaba.polardbx.manager.response.ShowTime;
import com.alibaba.polardbx.manager.response.ShowTransStats;
import com.alibaba.polardbx.manager.response.ShowVariables;
import com.alibaba.polardbx.manager.response.ShowVersion;

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
        case ManagerParseShow.SERVER_EXECUTOR:
            ShowServerExecutor.execute(c);
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
        case ManagerParseShow.SQL_LOG:
            ShowSqlLog.execute(c);
            break;
        case ManagerParseShow.DISCARD_COUNT:
            ShowDiscardLog.execute(c);
            break;
        case ManagerParseShow.TRANS_STATS:
            ShowTransStats.execute(c);
            break;
        default:
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}
