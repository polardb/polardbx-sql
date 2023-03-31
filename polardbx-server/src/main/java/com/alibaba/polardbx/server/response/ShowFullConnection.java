package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.IntegerUtil;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.List;
import java.util.Map;

/**
 * connections command with extra info, like partition hint
 */
public final class ShowFullConnection {

    private static final int FIELD_COUNT = 14;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ALIVE_TIME(s)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LAST_ACTIVE(ms)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHANNELS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TRX", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NEED_RECONNECT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PARTITION_HINT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        byte packetId = eof.packetId;
        String charset = c.getCharset();

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + c.getSchema() + "'");
            return false;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_HANDLE_DATA, e);
                return false;
            }
        }

        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new ShowConnectionSyncAction(
            c.getUser(), c.getSchema()), c.getSchema());
        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }

            for (Map<String, Object> conn : rs) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("ID")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("HOST")), charset));
                row.add(IntegerUtil.toBytes(DataTypes.IntegerType.convertFrom(conn.get("PORT"))));
                row.add(IntegerUtil.toBytes(DataTypes.IntegerType.convertFrom(conn.get("LOCAL_PORT"))));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("SCHEMA")), charset));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("CHARSET")), charset));
                row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(conn.get("NET_IN"))));
                row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(conn.get("NET_OUT"))));
                row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(conn.get("ALIVE_TIME(S)"))));
                row.add(LongUtil.toBytes(DataTypes.LongType.convertFrom(conn.get("LAST_ACTIVE"))));
                row.add(IntegerUtil.toBytes(DataTypes.IntegerType.convertFrom(conn.get("CHANNELS"))));
                row.add(IntegerUtil.toBytes(DataTypes.IntegerType.convertFrom(conn.get("TRX"))));
                row.add(IntegerUtil.toBytes(DataTypes.IntegerType.convertFrom(conn.get("NEED_RECONNECT"))));
                row.add(StringUtil.encode(DataTypes.StringType.convertFrom(conn.get("PARTITION_HINT")), charset));
                row.packetId = ++packetId;
                proxy = row.write(proxy);

            }
        }
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

}
