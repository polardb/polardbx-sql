package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.units.DataSize.succinctBytes;

public class ShowColumnarStatus {
    private static final int FIELD_COUNT = 11;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;
    private static final int COLUMNAR_STATUS_FIELDS = 11;

    /**
     * tso 条件匹配 “ tso = 21213”， tso忽略大小写， =前后忽略空格
     */
    private static final Pattern tsoPattern = Pattern.compile("(?i)\\bTSO\\b\\s*=\\s*(\\d+)");

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TSO", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("SCHEMA_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("INDEX_NAME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ROWS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("CSV_FILES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ORC_FILES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("DEL_FILES", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("FILES_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;
    }

    /**
     * show columnar status; 库级别
     * show full columnar status; 实例级别
     */
    public static boolean execute(ServerConnection c, ByteString stmt, int offset, boolean instance) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : FIELDS) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        String schema = c.getSchema();

        //粗略解析假如条件带了tso = xxx;
        String whereStr = stmt.substring(offset);
        Long tso = null;
        Matcher matcher = tsoPattern.matcher(whereStr);
        if (matcher.find()) {
            tso = Long.parseLong(matcher.group(1));
        }

        List<byte[][]> resultList = generateStatusPacket(schema, tso, instance);
        for (byte[][] results : resultList) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            for (byte[] result : results) {
                row.add(result);
            }
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

    public static List<byte[][]> generateStatusPacket(String logicalSchema, Long tso, boolean instance) {
        // force fetch the newest tso
        if (tso == null || tso == 0L) {
            tso = ColumnarTransactionUtils.getLatestShowColumnarStatusTsoFromGms();
        }

        if (tso == null) {
            tso = Long.MIN_VALUE;
        }

        List<byte[][]> resultsList = new ArrayList<>();

        //1、获取所有columnar index表
        List<ColumnarTableMappingRecord> columnarRecords = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {

            ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
            tableMappingAccessor.setConnection(metaDbConn);

            //同时显示正在创建的表，方便查看进度
            if (instance) {
                columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.PUBLIC.name()));
                columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.CREATING.name()));

            } else {
                columnarRecords.addAll(
                    tableMappingAccessor.queryBySchemaAndStatus(logicalSchema, ColumnarTableStatus.PUBLIC.name()));
                columnarRecords.addAll(
                    tableMappingAccessor.queryBySchemaAndStatus(logicalSchema, ColumnarTableStatus.CREATING.name()));
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "fail to fetch columnar index: " + logicalSchema, e);
        }

        List<ColumnarTransactionUtils.ColumnarIndexStatusRow> rows =
            ColumnarTransactionUtils.queryColumnarIndexStatus(tso, columnarRecords);

        rows.forEach(row -> {
            byte[][] results = new byte[COLUMNAR_STATUS_FIELDS][];
            results[0] = String.valueOf(row.tso).getBytes();
            results[1] = row.tableSchema.getBytes();
            results[2] = row.tableName.getBytes();
            results[3] = row.indexName.getBytes();
            results[4] = String.valueOf(row.indexId).getBytes();
            results[5] = String.valueOf(row.csvRows + row.orcRows - row.delRows).getBytes();
            results[6] = String.valueOf(row.csvFileNum).getBytes();
            results[7] = String.valueOf(row.orcFileNum).getBytes();
            results[8] = String.valueOf(row.delFileNum).getBytes();
            results[9] = succinctBytes(row.csvFileSize + row.orcFileSize + row.delFileSize).toString().getBytes();
            results[10] = row.status.getBytes();
            resultsList.add(results);
        });

        return resultsList;
    }
}
