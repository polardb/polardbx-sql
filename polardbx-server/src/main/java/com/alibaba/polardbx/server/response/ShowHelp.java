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
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;

import java.util.ArrayList;
import java.util.List;

/**
 * 打印help信息
 */
public final class ShowHelp {

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("STATEMENT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXAMPLE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ServerConnection c) {
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
        for (HelpData data : datas) {
            RowDataPacket row = getRow(data.key, data.desc, data.example, c.getCharset());
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(String stmt, String desc, String example, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(stmt, charset));
        row.add(StringUtil.encode(desc, charset));
        row.add(StringUtil.encode(example, charset));
        return row;
    }

    private static final List<HelpData> datas = new ArrayList<HelpData>();

    static {
        // show
        datas.add(new HelpData("show rule", "Report all table rule", ""));
        datas.add(new HelpData("show rule from TABLE", "Report table rule", "show rule from user"));
        datas.add(new HelpData("show full rule from TABLE", "Report table full rule", "show full rule from user"));
        datas
            .add(new HelpData("show topology from TABLE", "Report table physical topology", "show topology from user"));
        datas.add(new HelpData("show partitions from TABLE",
            "Report table dbPartition or tbPartition columns",
            "show partitions from user"));
        datas.add(new HelpData("show broadcasts", "Report all broadcast tables", ""));
        datas.add(new HelpData("show datasources", "Report all partition db threadPool info", ""));
        datas.add(new HelpData("show node", "Report master/slave read status", ""));
        datas.add(new HelpData("show slow", "Report top 100 slow sql", ""));
        datas.add(new HelpData("show physical_slow", "Report top 100 physical slow sql", ""));
        datas.add(new HelpData("clear slow", "Clear slow data", ""));
        datas.add(new HelpData("trace SQL",
            "Start trace sql, use show trace to print profiling data",
            "trace select count(*) from user; show trace"));
        datas.add(new HelpData("show trace", "Report sql execute profiling info", ""));
        datas.add(new HelpData("explain SQL", "Report sql plan info", "explain select count(*) from user"));
        datas.add(new HelpData("explain detail SQL",
            "Report sql detail plan info",
            "explain detail select count(*) from user"));
        datas.add(new HelpData("explain execute SQL",
            "Report sql on physical db plan info",
            "explain execute select count(*) from user"));
        datas.add(new HelpData("show sequences", "Report all sequences status", ""));
        datas.add(new HelpData("create sequence NAME [start with COUNT]",
            "Create sequence",
            "create sequence test start with 0"));
        datas.add(new HelpData("alter sequence NAME [start with COUNT]",
            "Alter sequence",
            "alter sequence test start with 100000"));
        datas.add(new HelpData("drop sequence NAME", "Drop sequence", "drop sequence test"));
        datas.add(new HelpData("show db status", "Report size of each physical database ", "show db status"));
        datas.add(new HelpData("show full db status", "Report size of each physical table, support like ",
            "show full db status like user"));
        datas.add(new HelpData("show ds", "Report simple partition db info ", "show ds"));
        datas.add(new HelpData("show connection", "Report all connections info ", "show connection"));
        //deprecated
        //datas.add(new HelpData("show statistic", "Report all rows of tables ", "show statistic"));
        datas.add(new HelpData("show trans", "Report trans info ", "show trans/show trans limit 2"));
        datas.add(new HelpData("show full trans", "Report all trans info ", "show full trans/show full trans limit 2"));
        datas.add(new HelpData("show stats", "Report all requst stats ", "show stats"));
        datas.add(new HelpData("show stc", "Report all requst stats by partition", "show stc"));
        datas.add(new HelpData("show htc", "Report the CPU/LOAD/MEM/NET/GC stats", "show htc"));
        //ccl
        datas.add(new HelpData("show ccl_rules", "Report the ccl rule info", "show ccl_rules"));
        datas.add(new HelpData("clear ccl_rules", "Clear the ccl rules", "clear ccl_rules"));
        datas.add(new HelpData("show ccl_triggers", "Report the ccl trigger info", "show ccl_triggers"));
        datas.add(new HelpData("clear ccl_triggers", "Clear the ccl triggers", "clear ccl_triggers"));
        //deprecated
        //datas.add(new HelpData("show git_commit", "Report the release info ", "show git_commit"));
    }

    public static class HelpData {

        String key;
        String desc;
        String example;

        public HelpData(String key, String desc, String example) {
            this.key = key;
            this.desc = desc;
            this.example = example;
        }

    }
}
