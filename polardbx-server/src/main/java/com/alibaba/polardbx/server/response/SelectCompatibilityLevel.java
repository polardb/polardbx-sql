package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import com.alibaba.polardbx.server.util.ParseUtil;
import com.alibaba.polardbx.server.util.StringUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectCompatibilityLevel {
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;
    private static final String versionRegex = "SELECT\\s+COMPATIBILITY_LEVEL\\s*\\((.*)\\)";
    private static final Pattern versionPattern = Pattern.compile(versionRegex, Pattern.CASE_INSENSITIVE);
    private static final String versionRegex2 = ".*(\\d+\\.\\d+\\.\\d+-\\d+).*";
    private static final Pattern versionPattern2 = Pattern.compile(versionRegex2, Pattern.CASE_INSENSITIVE);
    private static final int INPUT_MAX_LEN = 128;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("COMPATIBILITY LEVEL", Fields.FIELD_TYPE_VAR_STRING);
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
        RowDataPacket row = getRow(mapToCompatibilityLevel(stmt), c.getResultSetCharset());
        row.packetId = ++tmpPacketId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
        return true;
    }

    private static String mapToCompatibilityLevel(ByteString stmt) {
        String input = stmt.toString().toUpperCase();
        if (input.length() > INPUT_MAX_LEN) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Wrong format, example: select compatibility_level(5.4.18-1234)");
        }
        Matcher matcher = versionPattern.matcher(input);
        String version = null;
        if (matcher.find()) {
            try {
                version = matcher.group(1);
            } catch (IndexOutOfBoundsException e) {
                // ignore.
                version = null;
            }
        }
        if (null == version) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Wrong format, example: select compatibility_level(5.4.18-1234)");
        }
        version = ParseUtil.removeQuotation(version);
        matcher = versionPattern2.matcher(version);
        if (matcher.find()) {
            try {
                version = matcher.group(1);
            } catch (IndexOutOfBoundsException e) {
                // ignore.
                version = null;
            }
        }
        if (null == version) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                "Wrong format, example: select compatibility_level(5.4.18-1234)");
        }
        return AlterSystemCompatibilityHandler.versionToCompatibilityLevel(version).toString();
    }

    private static RowDataPacket getRow(String level, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(level, charset));
        return row;
    }
}
