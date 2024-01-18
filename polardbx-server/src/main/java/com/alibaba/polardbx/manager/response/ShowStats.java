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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.stats.MatrixStatistics;

/**
 * 查看处理器状态
 *
 * @author xianmao.hexm 2010-1-25 下午01:11:00
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowStats {

    private static final int FIELD_COUNT = 36;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ACTIVE_CONNECTION", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CONNECTION_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TIME_COST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REQUEST_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("QUERY_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INSERT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DELETE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("UPDATE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("REPLACE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RUNNING_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PHYSICAL_REQUEST_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PHYSICAL_TIME_COST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ERROR_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VIOLATION_ERROR_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MULTI_DB_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TEMP_TABLE_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("JOIN_MULTI_DB_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("AGGREGATE_MULTI_DB_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HINT_COUNT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SLOW_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PHYSICAL_SLOW_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TRANS_COUNT_XA", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TRANS_COUNT_BEST_EFFORT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TRANS_COUNT_TSO", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CCL_KILL", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CCL_RUN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CCL_WAIT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CCL_WAIT_KILL", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CCL_RESCHEDULE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TP_WORKLOAD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("AP_WORKLOAD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOCAL_NUM", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CLUSTER_NUM", Fields.FIELD_TYPE_LONGLONG);
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
        for (SchemaConfig schema : CobarServer.getInstance().getConfig().getSchemas().values()) {
            if (!schema.getDataSource().isInited()) {
                continue;
            }
            if (SystemDbHelper.CDC_DB_NAME.equalsIgnoreCase(schema.getName())) {
                continue;
            }
            RowDataPacket row = getRow(schema.getDataSource(), c.getCharset());
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

    private static RowDataPacket getRow(TDataSource ds, String charset) {
        MatrixStatistics stats = ds.getStatistics();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(ds.getSchemaName(), charset));
        row.add(LongUtil.toBytes(stats.netIn));
        row.add(LongUtil.toBytes(stats.netOut));
        row.add(LongUtil.toBytes(stats.activeConnection.get()));
        row.add(LongUtil.toBytes(stats.connectionCount.get()));
        row.add(LongUtil.toBytes(stats.timeCost));
        row.add(LongUtil.toBytes(stats.request));
        row.add(LongUtil.toBytes(stats.query));
        row.add(LongUtil.toBytes(stats.insert));
        row.add(LongUtil.toBytes(stats.delete));
        row.add(LongUtil.toBytes(stats.update));
        row.add(LongUtil.toBytes(stats.replace));
        ServerThreadPool exec = CobarServer.getInstance().getServerExecutor();
        row.add(LongUtil.toBytes(exec.getTaskCountBySchemaName(ds.getSchemaName())));
        row.add(LongUtil.toBytes(stats.physicalRequest.get()));
        row.add(LongUtil.toBytes(stats.physicalTimeCost.get()));
        row.add(LongUtil.toBytes(stats.errorCount));
        row.add(LongUtil.toBytes(stats.integrityConstraintViolationErrorCount));
        row.add(LongUtil.toBytes(stats.multiDBCount));
        row.add(LongUtil.toBytes(stats.tempTableCount));
        row.add(LongUtil.toBytes(stats.joinMultiDBCount));
        row.add(LongUtil.toBytes(stats.aggregateMultiDBCount));
        row.add(LongUtil.toBytes(stats.hintCount));
        row.add(LongUtil.toBytes(stats.slowRequest));
        row.add(LongUtil.toBytes(stats.physicalSlowRequest));
        row.add(LongUtil.toBytes(stats.getTransactionStats().countXA.get()));
        row.add(LongUtil.toBytes(stats.getTransactionStats().countBestEffort.get()));
        row.add(LongUtil.toBytes(stats.getTransactionStats().countTSO.get()));
        row.add(LongUtil.toBytes(stats.cclKill));
        row.add(LongUtil.toBytes(stats.cclRun));
        row.add(LongUtil.toBytes(stats.cclWait));
        row.add(LongUtil.toBytes(stats.cclWaitKill));
        row.add(LongUtil.toBytes(stats.cclReschedule));
        row.add(LongUtil.toBytes(stats.tpLoad));
        row.add(LongUtil.toBytes(stats.apLoad));
        row.add(LongUtil.toBytes(stats.local));
        row.add(LongUtil.toBytes(stats.cluster));
        return row;
    }
}
