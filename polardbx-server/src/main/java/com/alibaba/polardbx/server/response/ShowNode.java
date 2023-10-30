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
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 查看当前链接的读写请求比例
 */
public final class ShowNode {

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MASTER_READ_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SLAVE_READ_COUNT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MASTER_READ_PERCENT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SLAVE_READ_PERCENT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        byte tmpPacketId = packetId;
        // write eof
        if (!c.isEofDeprecated()) {
            EOFPacket eof = new EOFPacket();
            eof.packetId = ++tmpPacketId;
            proxy = eof.write(proxy);
        }

        // write rows
        String charset = c.getCharset();

        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return false;
        }
        // 取得配置文件
        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
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
        int i = 0;

        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(new ShowNodeSyncAction(c.getSchema()), c.getSchema());

        Map<String, Long> masterReadCounts = new HashMap();
        Map<String, Long> slaveReadCounts = new HashMap();

        List<String> groups = new ArrayList();
        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }
            for (Map<String, Object> row : rs) {
                String groupName = String.valueOf(row.get("NAME"));

                if (!groups.contains(groupName)) {
                    groups.add(groupName);
                }

                long masterReadCount = DataTypes.LongType.convertFrom(row.get("MASTER_READ_COUNT"));
                long slaveReadCount = DataTypes.LongType.convertFrom(row.get("SLAVE_READ_COUNT"));

                if (masterReadCounts.containsKey(groupName)) {
                    masterReadCounts.put(groupName, masterReadCount + masterReadCounts.get(groupName));
                } else {
                    masterReadCounts.put(groupName, masterReadCount);
                }

                if (slaveReadCounts.containsKey(groupName)) {
                    slaveReadCounts.put(groupName, slaveReadCount + slaveReadCounts.get(groupName));
                } else {
                    slaveReadCounts.put(groupName, slaveReadCount);
                }
            }
        }
        for (String group : groups) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(LongUtil.toBytes(i++));

            row.add(StringUtil.encode(group, charset));

            long masterReadCount = masterReadCounts.get(group);
            long slaveReadCount = slaveReadCounts.get(group);

            java.text.NumberFormat percentFormat = java.text.NumberFormat.getPercentInstance();

            row.add(LongUtil.toBytes(masterReadCount));
            row.add(LongUtil.toBytes(slaveReadCount));

            if (masterReadCount + slaveReadCount != 0) {
                row.add(StringUtil.encode(percentFormat.format(masterReadCount
                        / (float) (masterReadCount + slaveReadCount)),
                    charset));
                row.add(StringUtil.encode(percentFormat.format(slaveReadCount
                        / (float) (masterReadCount + slaveReadCount)),
                    charset));
            } else {
                row.add(StringUtil.encode(percentFormat.format(0.0), charset));
                row.add(StringUtil.encode(percentFormat.format(0.0), charset));
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
}
