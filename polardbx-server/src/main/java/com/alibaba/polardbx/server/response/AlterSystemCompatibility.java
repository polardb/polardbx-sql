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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yaozhili
 */
public final class AlterSystemCompatibility {

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    private static final String versionRegex
        = "ALTER\\s+SYSTEM\\s+SET\\s+COMPATIBILITY_LEVEL\\s*=\\s*(\\d+)\\s*";
    public static final Pattern versionPattern = Pattern.compile(versionRegex, Pattern.CASE_INSENSITIVE);
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
        AlterSystemCompatibilityHandler.Result result;
        String input = stmt.toString().toUpperCase();
        if (input.length() > INPUT_MAX_LEN) {
            result = new AlterSystemCompatibilityHandler.Result(
                AlterSystemCompatibilityHandler.Result.Status.ERROR,
                "Input too long.",
                "Example: ALTER SYSTEM SET COMPATIBILITY_LEVEL = 1000"
            );
        } else if (!c.isSuperUserOrAllPrivileges()) {
            result = new AlterSystemCompatibilityHandler.Result(
                AlterSystemCompatibilityHandler.Result.Status.ERROR,
                "Only super user can change COMPATIBILITY_LEVEL",
                "-"
            );
        } else {
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
                result = new AlterSystemCompatibilityHandler.Result(
                    AlterSystemCompatibilityHandler.Result.Status.ERROR,
                    "Invalid level format.",
                    "Example: ALTER SYSTEM SET COMPATIBILITY_LEVEL = 1000");
            } else {
                result = AlterSystemCompatibilityHandler.addCompatibilityTask(level);
            }
        }

        RowDataPacket row = getRow(result.status.toString(), result.cause, result.solution, c.getResultSetCharset());
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

    private static RowDataPacket getRow(String status, String cause, String solution, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(status, charset));
        row.add(StringUtil.encode(cause, charset));
        row.add(StringUtil.encode(solution, charset));
        return row;
    }

}
