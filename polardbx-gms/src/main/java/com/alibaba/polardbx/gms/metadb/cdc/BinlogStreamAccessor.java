package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinlogStreamAccessor extends AbstractAccessor {
    private static final String BINLOG_STREAM_TABLE = "binlog_x_stream";
    private static final String LIST_BINLOG_STREAM_TARGET =
        "select group_name, stream_name, latest_cursor, endpoint from `" + BINLOG_STREAM_TABLE + "`";

    private static final String SELECT_BINLOG_STREAM_TARGET =
        "select group_name, stream_name, latest_cursor, endpoint from `" + BINLOG_STREAM_TABLE
            + "` where `stream_name` = ?";

    public List<BinlogStreamRecord> listAllStream() {
        try {
            return MetaDbUtil.query(LIST_BINLOG_STREAM_TARGET, BinlogStreamRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + LIST_BINLOG_STREAM_TARGET + "'",
                e);
            return null;
        }
    }

    public List<BinlogStreamRecord> getStream(String streamName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, streamName);
            return MetaDbUtil.query(SELECT_BINLOG_STREAM_TARGET, params, BinlogStreamRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to query the system table '" + SELECT_BINLOG_STREAM_TARGET + "'",
                e);
            return null;
        }
    }
}
