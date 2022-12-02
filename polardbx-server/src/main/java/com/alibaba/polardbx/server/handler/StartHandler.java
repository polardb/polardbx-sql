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

import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.IsolationUtil;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

/**
 * @author xianmao.hexm
 */
public final class StartHandler {

    public static void handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore,
                              boolean inProcedureCall) {
        Throwable sqlEx = null;
        try {
            SQLStartTransactionStatement startStmt =
                (SQLStartTransactionStatement) (FastsqlUtils.parseSql(stmt).get(0));
            IsolationLevel level = IsolationUtil.convertFastSql(startStmt.getIsolationLevel());
            if (level == null && startStmt.isConsistentSnapshot()) {
                level = IsolationLevel.REPEATABLE_READ;
            }
            c.begin(startStmt.isReadOnly(), level);
            if (!inProcedureCall) {
                PacketOutputProxyFactory.getInstance().createProxy(c)
                    .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
            }
        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            LogUtils.recordSql(c, stmt, sqlEx);
        }
    }
}
