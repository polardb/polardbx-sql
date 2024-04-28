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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.optimizer.planmanager.StatementMap;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MysqlDeallocatePrepareStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.sql.SQLSyntaxErrorException;

/**
 * Created by simiao.zw on 2014/7/29.
 */
public final class DeallocateHandler {

    public static boolean handle(ByteString stmt, ServerConnection c, boolean hasMore, boolean inProcedureCall) {

        try {
            parseAndRemove(stmt, c);

            if (!inProcedureCall) {
                // return OK
                PacketOutputProxyFactory.getInstance().createProxy(c)
                    .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
            }
            return true;
        } catch (SQLSyntaxErrorException e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "Parse '" + stmt + "'");
            return false;
        }
    }

    private static void parseAndRemove(ByteString sql, ServerConnection c) throws SQLSyntaxErrorException {
        try {
            MysqlDeallocatePrepareStatement statement =
                (MysqlDeallocatePrepareStatement) FastsqlUtils.parseSql(sql).get(0);
            String name = statement.getStatementName().getSimpleName();
            c.removePreparedCache(name, false);
        } catch (Exception e) {
            throw new SQLSyntaxErrorException("deallocate prepare statement error", e);
        }

    }
}
