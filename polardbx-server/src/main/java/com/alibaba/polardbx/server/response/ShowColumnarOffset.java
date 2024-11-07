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

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.FullMasterStatus;
import com.alibaba.polardbx.rpc.cdc.Request;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ShowColumnarOffset {
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("BinlogFile", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("Position", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TSO", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("TIME", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("LATENCY(ms)", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c) {
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

        List<byte[][]> resultList = buildBinlogOffset();
        if (resultList != null) {
            for (byte[][] results : resultList) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                for (byte[] result : results) {
                    row.add(result);
                }
                row.packetId = ++tmpPacketId;
                proxy = row.write(proxy);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
        return true;
    }

    private static List<byte[][]> buildBinlogOffset() {
        List<byte[][]> resultsList = new ArrayList<>();

        //先拿columnar位点，防止columnar位点 > polardbx
        List<ColumnarCheckpointsRecord> checkpointsList = new ArrayList<>();
        List<String> typeList = new ArrayList<>();
        Long columnarPurgeTso = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {

            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);

            List<ColumnarCheckpointsRecord> columnarCheckpointsRecords =
                checkpointsAccessor.queryLastByTypes(ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                    ColumnarCheckpointsAccessor.CheckPointType.DDL,
                    ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
            if (!columnarCheckpointsRecords.isEmpty()) {
                checkpointsList.add(columnarCheckpointsRecords.get(0));
                typeList.add("COLUMNAR_LATENCY");
            }
            ColumnarPurgeHistoryAccessor columnarPurgeHistoryAccessor = new ColumnarPurgeHistoryAccessor();
            columnarPurgeHistoryAccessor.setConnection(metaDbConn);

            List<ColumnarPurgeHistoryRecord> records = columnarPurgeHistoryAccessor.queryLastPurgeTso();
            if (!records.isEmpty()) {
                columnarPurgeTso = records.get(0).tso;
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "fail to fetch columnar checkpoint.", e);
        }

        ColumnarManager columnarManager = ColumnarManager.getInstance();
        // CN 版本链加载最新位点
        long latestTso = columnarManager.latestTso();
        // 列存事务下水位线
        long minTso = ColumnarTransactionUtils.getMinColumnarSnapshotTime();
        // purge 下水位线
        long purgeTso = ((DynamicColumnarManager) columnarManager).getMinTso();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);

            List<ColumnarCheckpointsRecord> columnarCheckpointsRecords =
                checkpointsAccessor.queryByTsoAndTypes(
                    latestTso,
                    ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                        ColumnarCheckpointsAccessor.CheckPointType.DDL,
                        ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
            if (!columnarCheckpointsRecords.isEmpty()) {
                checkpointsList.add(columnarCheckpointsRecords.get(0));
                typeList.add("CN_MIN_LATENCY");
            }

            columnarCheckpointsRecords =
                checkpointsAccessor.queryByTsoAndTypes(
                    minTso,
                    ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                        ColumnarCheckpointsAccessor.CheckPointType.DDL,
                        ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
            if (!columnarCheckpointsRecords.isEmpty()) {
                checkpointsList.add(columnarCheckpointsRecords.get(0));
                typeList.add("CN_MAX_LATENCY");
            }

            columnarCheckpointsRecords =
                checkpointsAccessor.queryByTsoAndTypes(
                    purgeTso,
                    ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                        ColumnarCheckpointsAccessor.CheckPointType.DDL,
                        ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
            if (!columnarCheckpointsRecords.isEmpty()) {
                checkpointsList.add(columnarCheckpointsRecords.get(0));
                typeList.add("CN_PURGE_WATERMARK");
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "fail to fetch columnar checkpoint.", e);
        }

        //binlog 位点
        CdcServiceGrpc.CdcServiceBlockingStub cdcServiceBlockingStub =
            CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        FullMasterStatus fullMasterStatus =
            cdcServiceBlockingStub.showFullMasterStatus(Request.newBuilder().setStreamName("").build());

        long cdcTso = getTsoTimestamp(fullMasterStatus.getLastTso());
        long cdcMs = cdcTso >>> 22;
        long cdcDelay = fullMasterStatus.getDelayTime();
        // 创建日期对象
        Date date = new Date(cdcMs);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String time = sdf.format(date);

        byte[][] polardbxResults = new byte[FIELD_COUNT][];
        int pos = 0;
        polardbxResults[pos++] = String.valueOf("CDC").getBytes();
        polardbxResults[pos++] = String.valueOf(fullMasterStatus.getFile()).getBytes();
        polardbxResults[pos++] = String.valueOf(fullMasterStatus.getPosition()).getBytes();
        polardbxResults[pos++] = String.valueOf(cdcTso).getBytes();
        polardbxResults[pos++] = String.valueOf(time).getBytes();
        polardbxResults[pos++] = String.valueOf(cdcDelay).getBytes();
        resultsList.add(polardbxResults);

        for (int i = 0; i < checkpointsList.size(); i++) {
            ColumnarCheckpointsRecord checkpointsRecord = checkpointsList.get(i);
            String type = typeList.get(i);
            try {
                ObjectMapper mapper = new ObjectMapper();
                SourceInfo sourceInfo = mapper.readValue(checkpointsRecord.getOffset(), SourceInfo.class);
                long columnarMs = sourceInfo.tso >>> 22;
                long latency = cdcMs - columnarMs;

                Date cDate = new Date(columnarMs);
                SimpleDateFormat cSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String cTime = cSdf.format(cDate);
                byte[][] columnarResults = new byte[FIELD_COUNT][];
                int cPos = 0;
                columnarResults[cPos++] = String.valueOf(type).getBytes();
                columnarResults[cPos++] = String.valueOf(sourceInfo.file).getBytes();
                columnarResults[cPos++] = String.valueOf(sourceInfo.pos).getBytes();
                columnarResults[cPos++] = String.valueOf(sourceInfo.tso).getBytes();
                columnarResults[cPos++] = String.valueOf(cTime).getBytes();
                columnarResults[cPos++] = String.valueOf(latency).getBytes();
                resultsList.add(columnarResults);

            } catch (Throwable t) {
                throw new RuntimeException("Parse json failed: " + checkpointsRecord.getOffset(), t);
            }

        }

        if (columnarPurgeTso != null) {
            long columnarPurgeMs = columnarPurgeTso >>> 22;
            long latency = cdcMs - columnarPurgeMs;

            Date cDate = new Date(columnarPurgeMs);
            SimpleDateFormat cSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String cTime = cSdf.format(cDate);
            byte[][] columnarResults = new byte[FIELD_COUNT][];
            int cPos = 0;
            columnarResults[cPos++] = String.valueOf("COLUMNAR_PURGE").getBytes();
            columnarResults[cPos++] = String.valueOf("").getBytes();
            columnarResults[cPos++] = String.valueOf("").getBytes();
            columnarResults[cPos++] = String.valueOf(columnarPurgeTso).getBytes();
            columnarResults[cPos++] = String.valueOf(cTime).getBytes();
            columnarResults[cPos++] = String.valueOf(latency).getBytes();
            resultsList.add(columnarResults);
        }

        Channel channel = cdcServiceBlockingStub.getChannel();
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdown();
        }

        return resultsList;
    }

    private static class SourceInfo {

        @JsonProperty("file")
        private String file;
        @JsonProperty("pos")
        private long pos = 0L;
        @JsonProperty("row")
        private int row = 0;
        @JsonProperty("server_id")
        private long server_id = 0;
        @JsonProperty("tso")
        private long tso = 0;

    }

    private static Long getTsoTimestamp(String tso) {
        return Long.valueOf(tso.substring(0, 19));
    }
}
