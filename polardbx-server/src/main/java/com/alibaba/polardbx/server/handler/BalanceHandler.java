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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.ParseUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

public class BalanceHandler {
    public static void handle(ByteString stmt, ServerConnection c) {
        int percent = 0;
        int i = 0;
        outer:
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case 'B':
                case 'b':
                    break outer;
                default:
                    c.handleError(ErrorCode.ERR_HANDLE_DATA,
                        new SQLSyntaxErrorException("sql is not a supported statement, " +
                            "maybe you have an error in your SQL syntax "), stmt.toString(), false);
                    return;
            }
        }
        i += "balance".length();
outer : for (; i < stmt.length(); ++i) {
            char ch = stmt.charAt(i);
            switch (ch) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    break outer;
                default:
                    c.handleError(ErrorCode.ERR_HANDLE_DATA,
                        new SQLSyntaxErrorException("sql is not a supported statement," +
                            " maybe you have an error in your SQL syntax "), stmt.toString(), false);
                    return;
            }
        }

        while (i < stmt.length() && stmt.charAt(i) >= '0' && stmt.charAt(i) <= '9') {
            percent = percent * 10 + (int) stmt.charAt(i) - '0';
            i++;
            if (percent > 100) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, new SQLSyntaxErrorException("balance num too big"),
                    stmt.toString(), false);
                return;
            }
        }

        for (; i < stmt.length(); ++i) {
            char ch = stmt.charAt(i);
            switch (ch) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                case ';':
                    continue;
                default:
                    c.handleError(ErrorCode.ERR_HANDLE_DATA,
                        new SQLSyntaxErrorException("sql is not a supported statement," +
                            " maybe you have an error in your SQL syntax "), stmt.toString(), false);
                    return;
            }
        }

        int allSize = 0;
        int needTagCount = 0;
        int alreadyTagCount = 0;

        List<ServerConnection> toTagList = new ArrayList<>();

        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection) {
                    if (fc.isNeedReconnect()) {
                        alreadyTagCount++;
                        allSize++;
                    } else if (fc != c) {
                        toTagList.add((ServerConnection) fc);
                        allSize++;
                    }
                }
            }
        }

        needTagCount = allSize * percent / 100 - alreadyTagCount;

        if (needTagCount > 0) {
            for (ServerConnection fc : toTagList) {
                if (needTagCount > 0) {
                    needTagCount--;
                    fc.setNeedReconnect(true);
                }
            }
        }
        PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
    }
}
