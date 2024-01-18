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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * 仅做兼容debezium
 * 不做操作直接返回
 */
public final class FlushTable {

    public static boolean response(ByteString stmt, ServerConnection c,
                                   int offset, boolean hasMore) {
        List<SQLStatement> stmtList = FastsqlUtils.parseSql(stmt);
        if (stmtList.size() != 1) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "dose not support multi statement in flush tables");
            return false;
        }
        if (!(stmtList.get(0) instanceof MySqlFlushStatement)) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "illegal flush statement");
            return false;
        }
        MySqlFlushStatement flushStmt = (MySqlFlushStatement) stmtList.get(0);
        if (!isFlushTableWithReadLock(flushStmt) && !isFlushPrivileges(flushStmt)) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "unsupported flush statement");
            return false;
        }
        // do nothing
        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        return true;
    }

    private static boolean isFlushTableWithReadLock(MySqlFlushStatement flushStmt) {
        if (flushStmt != null) {
            return flushStmt.isTableOption() && CollectionUtils.isEmpty(flushStmt.getTables()) &&
                flushStmt.isWithReadLock();
        }
        return false;
    }

    private static boolean isFlushPrivileges(MySqlFlushStatement flushStmt) {
        if (flushStmt != null) {
            return flushStmt.isPrivileges();
        }
        return false;
    }
}
