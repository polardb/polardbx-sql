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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcVersionUtil;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

public class SelectPolardbVersion {

    private static final Logger logger = LoggerFactory.getLogger(SelectPolardbVersion.class);

    private static final int FIELD_COUNT = 3;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final byte packetId = FIELD_COUNT + 1;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("NODE_TYPE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BUILD_NUMBER", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
    }

    public static boolean response(ServerConnection c, boolean hasMore) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        proxy = header.write(proxy);
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
        String charset = c.getCharset();
        addCnVersion(proxy, ++tmpPacketId, charset);
        addDnVersion(proxy, ++tmpPacketId, charset);
        addCdcVersion(proxy, ++tmpPacketId, charset);
        addGmsVersion(proxy, ++tmpPacketId, charset);

        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }

    private static void addGmsVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String nodeType = "GMS";
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addVersionWithBuildNumber(row, nodeType, MetaDbUtil.getGmsPolardbVersion(), charset);
        row.packetId = packetId;
        row.write(proxy);
    }

    private static void addCdcVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String nodeType = "CDC";
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addVersionWithBuildNumber(row, nodeType, CdcVersionUtil.getVersion(), charset);
        row.packetId = packetId;
        row.write(proxy);
    }

    private static void addDnVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String nodeType = "DN";
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addVersionWithBuildNumber(row, nodeType, ExecUtils.getDnPolardbVersion(), charset);
        row.packetId = packetId;
        row.write(proxy);
    }

    private static void addCnVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String nodeType = "CN";
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addVersionWithBuildNumber(row, nodeType, Version.getVersion(), charset);
        row.packetId = packetId;
        row.write(proxy);
    }

    /**
     * @param version format: {MajorVersion}-{BuildNumber}
     */
    private static void addVersionWithBuildNumber(RowDataPacket row, String nodeType,
                                                  String version, String charset) {
        String majorVersion = version;
        String buildNumber = "";
        boolean isLegalVersionFormat = false;
        try {
            if (!StringUtils.isBlank(version)) {
                String[] strs = StringUtils.split(version, "-");
                if (strs != null && strs.length == 2) {
                    majorVersion = strs[0];
                    buildNumber = strs[1];
                    isLegalVersionFormat = true;
                }
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }

        if (!isLegalVersionFormat) {
            logger.warn("Failed to parse " + nodeType + " version: " + version);
        }
        row.add(StringUtil.encode(nodeType, charset));
        row.add(StringUtil.encode(majorVersion, charset));
        row.add(StringUtil.encode(buildNumber, charset));
    }
}
