package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.ErrorCode;
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

    public static void response(ByteString stmt, ServerConnection c,
                                int offset, boolean hasMore) {
        List<SQLStatement> stmtList = FastsqlUtils.parseSql(stmt);
        if (stmtList.size() != 1) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "dose not support multi statement in flush tables");
            return;
        }
        if (!(stmtList.get(0) instanceof MySqlFlushStatement)) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "illegal flush statement");
            return;
        }
        MySqlFlushStatement flushStmt = (MySqlFlushStatement) stmtList.get(0);
        if (!isFlushTableWithReadLock(flushStmt) && !isFlushPrivileges(flushStmt)) {
            c.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "unsupported flush statement");
            return;
        }
        // do nothing
        PacketOutputProxyFactory.getInstance().createProxy(c)
            .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
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
