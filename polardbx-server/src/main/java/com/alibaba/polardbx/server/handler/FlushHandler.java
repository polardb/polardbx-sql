package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseFlush;
import com.alibaba.polardbx.server.response.FlushTable;

public final class FlushHandler {
    public static void handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        FlushTable.response(stmt, c, offset, hasMore);
    }
}
