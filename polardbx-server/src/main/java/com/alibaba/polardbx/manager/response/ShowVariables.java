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

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowVariables {

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("VARIABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
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
        for (Map.Entry<String, String> e : variables.entrySet()) {
            RowDataPacket row = getRow(e.getKey(), e.getValue(), c.getResultSetCharset());
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String name, String value, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        row.add(StringUtil.encode(value, charset));
        return row;
    }

    private static final Map<String, String> variables = new HashMap<String, String>();

    static {
        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("collation_connection", "utf8_general_ci");
        variables.put("collation_server", "utf8_general_ci");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "8192");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("lower_case_table_names", "1");
        variables.put("tx_isolation", "READ-COMMITTED");
        variables.put("wait_timeout", "172800");
        variables.put("performance_schema", "OFF");
    }

}
