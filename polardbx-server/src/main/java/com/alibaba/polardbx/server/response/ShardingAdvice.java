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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.whatIf.ShardingWhatIf;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.sharding.advisor.ShardResultForOutput;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.List;
import java.util.Map;

/**
 * @author shengyu
 */
public class ShardingAdvice {


    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("TABLE ADVICE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("ESTIMATED CHANGES", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("MOST BENEFICIAL SQLs", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        fields[i] = PacketUtil.getField("MOST HARMFUL SQLs", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void response(ServerConnection c, boolean hasMore,
                                ShardResultForOutput result, ShardingWhatIf shardingWhatIf) {


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

        List<String> summary = shardingWhatIf.summarize();
        // write rows
        byte packetId = eof.packetId;

        if (result.getSqls().size() == 0) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(c.getSchema(), c.getCharset()));
            row.add(StringUtil.encode("No valid sql cache found in current schema!", c.getCharset()));
            for (int i = 0; i < 3; i++) {
                row.add(StringUtil.encode("", c.getCharset()));
            }
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        } else if (summary == null){
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(c.getSchema(), c.getCharset()));
            row.add(StringUtil.encode("No better sharding plan found for current workload!", c.getCharset()));
            for (int i = 0; i < 3; i++) {
                row.add(StringUtil.encode("", c.getCharset()));
            }
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        } else {
            for (Map.Entry<String, StringBuilder> entry : result.display().entrySet()) {
                RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                row.add(StringUtil.encode(entry.getKey(), c.getCharset()));
                row.add(StringUtil.encode(entry.getValue().toString(), c.getCharset()));
                for (String info : summary) {
                    row.add(StringUtil.encode(info, c.getCharset()));
                }
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

        // post write
        proxy.packetEnd();
    }
}
