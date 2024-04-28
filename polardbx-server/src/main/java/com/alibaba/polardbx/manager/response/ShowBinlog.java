package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.MasterStatus;
import com.alibaba.polardbx.rpc.cdc.Request;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ShowBinlog {
    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILE", Fields.FIELD_TYPE_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("POSITION", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BINLOG_DO_DB", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BINLOG_IGNORE_DB", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTED_GTID_SET", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
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

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub = CdcRpcClient.getCdcRpcClient()
            .getCdcServiceBlockingStub();
        MasterStatus masterStatus = blockingStub.showMasterStatus(
            Request.newBuilder().build());
        ArrayResultCursor result = new ArrayResultCursor("SHOW MASTER STATUS");
        result.addColumn("File", DataTypes.StringType);
        result.addColumn("Position", DataTypes.LongType);
        result.addColumn("Binlog_Do_DB", DataTypes.StringType);
        result.addColumn("Binlog_Ignore_DB", DataTypes.StringType);
        result.addColumn("Executed_Gtid_Set", DataTypes.StringType);
        result.initMeta();
        result.addRow(new Object[] {
            masterStatus.getFile(), masterStatus.getPosition(), masterStatus.getBinlogDoDB(),
            masterStatus.getBinlogIgnoreDB(), masterStatus.getExecutedGtidSet()});
        Channel channel = blockingStub.getChannel();
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdown();
        }

        List<List<Object>> values = new ArrayList<>();

        while (true) {
            Row rs = result.next();
            if (rs == null) {
                break;
            }

            final List<Object> columnValues = rs.getValues();
            // Convert inner type to JDBC types
            final List<Object> convertedColumnValues =
                columnValues.stream().map((Object cm) -> DataTypeUtil.toJavaObject(null, cm))
                    .collect(Collectors.toList());
            values.add(convertedColumnValues);
        }

        for (List<Object> columnValues : values) {
            RowDataPacket row = getRow(c.getResultSetCharset(), columnValues);
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String charset, List<Object> value) {

        RowDataPacket rowData = new RowDataPacket(FIELD_COUNT);

        rowData.add(StringUtil.encode(value.get(0).toString(), charset));
        rowData.add(StringUtil.encode(value.get(1).toString(), charset));
        rowData.add(StringUtil.encode(value.get(2).toString(), charset));
        rowData.add(StringUtil.encode(value.get(3).toString(), charset));
        rowData.add(StringUtil.encode(value.get(4).toString(), charset));

        return rowData;
    }
}
