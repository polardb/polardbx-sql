package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseAlterSystem;
import com.alibaba.polardbx.server.util.LogUtils;

public class AlterSystemHandler {
    public static boolean handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        int rs = ServerParseAlterSystem.parse(stmt, offset);
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (rs & 0xff) {
            case ServerParseAlterSystem.COMPATIBILITY_LEVEL:
                return AlterSystemCompatibility.execute(c, stmt);
            default:
                recordSql = false;
                return c.execute(stmt, hasMore);
            }
        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, stmt, sqlEx);
            }
        }
    }
}
