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
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.AlterSystemCompatibilityHandler;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yaozhili
 */
public final class ShowCompatibilityLevel {

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    private static final String versionRegex
        = "SHOW\\s+COMPATIBILITY_LEVEL\\s+(\\d+)\\s*";
    private static final Pattern versionPattern = Pattern.compile(versionRegex, Pattern.CASE_INSENSITIVE);
    private static final int INPUT_MAX_LEN = 128;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CAUSE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SOLUTION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c, ByteString stmt) {

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
        List<AlterSystemCompatibilityHandler.Result> results = getRisks(stmt);
        for (AlterSystemCompatibilityHandler.Result result : results) {
            RowDataPacket row =
                getRow(result.status.toString(), result.cause, result.solution, c.getResultSetCharset());
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
        return true;
    }

    private static List<AlterSystemCompatibilityHandler.Result> getRisks(ByteString stmt) {
        String input = stmt.toString().toUpperCase();
        if (input.length() > INPUT_MAX_LEN) {
            return Collections.singletonList(new AlterSystemCompatibilityHandler.Result(
                AlterSystemCompatibilityHandler.Result.Status.ERROR,
                "Input too long.",
                "Example: SHOW COMPATIBILITY_LEVEL 1000"
            ));
        }
        Matcher matcher = versionPattern.matcher(input);
        Long level = null;
        if (matcher.find()) {
            try {
                level = Long.parseLong(matcher.group(1));
            } catch (IndexOutOfBoundsException e) {
                // ignore.
            }
        }
        if (null == level) {
            return Collections.singletonList(new AlterSystemCompatibilityHandler.Result(
                AlterSystemCompatibilityHandler.Result.Status.ERROR,
                "Invalid level format.",
                "Example: SHOW COMPATIBILITY_LEVEL 1000"));
        }

        List<AlterSystemCompatibilityHandler.Result> results =
            AlterSystemCompatibilityHandler.getRisks().getIfPresent(level);
        if (null == results) {
            results = Collections.singletonList(
                new AlterSystemCompatibilityHandler.Result(
                    AlterSystemCompatibilityHandler.Result.Status.ERROR,
                    "No results found.",
                    "Execute ALTER SYSTEM SET COMPATIBILITY_LEVEL = " + level +
                        " to start a compatibility alter task first"));
        }
        return results;
    }

    private static RowDataPacket getRow(String status, String cause, String solution, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(status, charset));
        row.add(StringUtil.encode(cause, charset));
        row.add(StringUtil.encode(solution, charset));
        return row;
    }
}
