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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext.ErrorMessage;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * show warnings实现
 *
 * @author agapple 2015年3月27日 下午4:47:46
 * @since 5.1.19
 */
public final class ShowWarnings {

    public static final String LEVEL_WARNING = "Warning";
    public static final String LEVEL_ERROR = "Error";

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    private static final String cmd = "Show Warnings";

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("Level", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("Code", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("Message", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
    }

    public static boolean execute(ServerConnection c, boolean hasMore) {
        String db = c.getSchema();
        if (db == null) {
            c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return false;
        }

        SchemaConfig schema = c.getSchemaConfig();
        if (schema == null) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        TConnection conn = c.getTddlConnection();
        if (conn == null || conn.getExecutionContext() == null) {
            return c.execute(cmd, hasMore);
        }

        Map<String, Object> extraDatas = conn.getExecutionContext().getExtraDatas();
        if (extraDatas == null) {
            return c.execute(cmd, hasMore);
        }

        List<ErrorMessage> messagesWarning =
            (List<ErrorMessage>) extraDatas.getOrDefault(ExecutionContext.WARNING_MESSAGE, Lists.newLinkedList());
        List<ErrorMessage> messagesLastFailed =
            (List<ErrorMessage>) extraDatas.getOrDefault(ExecutionContext.LAST_FAILED_MESSAGE, Lists.newLinkedList());
        // Failed messages are displayed in `show errors`

        if (messagesWarning.isEmpty() && messagesLastFailed.isEmpty()) {
            return c.execute(cmd, hasMore);
        }

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
        for (ErrorMessage msg : messagesWarning) {
            RowDataPacket row = getRow(LEVEL_WARNING, msg, c.getResultSetCharset());
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }
        for (ErrorMessage msg : messagesLastFailed) {
            RowDataPacket row = getRow(LEVEL_ERROR, msg, c.getResultSetCharset());
            row.packetId = ++tmpPacketId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();

        // clear warnings, but keep last failed message
        conn.getExecutionContext().clearMessage(ExecutionContext.WARNING_MESSAGE);
        return true;
    }

    private static RowDataPacket getRow(String level, ErrorMessage msg, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(level, charset));
        row.add(LongUtil.toBytes(msg.getCode()));
        String messageText;
        if (TStringUtil.isEmpty(msg.getGroupName())) {
            messageText = msg.getMessage();
        } else {
            messageText = "From " + msg.getGroupName() + " , " + msg.getMessage();
        }
        row.add(StringUtil.encode(messageText, charset));
        return row;
    }

}
