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
import com.alibaba.polardbx.executor.ddl.job.task.columnar.ColumnarVersionUtil;
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
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * 如果组件不存在，则不输出该行
 * 如果组件存在但是版本号解析错误，则版本号返回NULL
 */
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

        fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RELEASE_DATE", Fields.FIELD_TYPE_VAR_STRING);
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
        String charset = c.getResultSetCharset();
        ++tmpPacketId;
        tmpPacketId = addProduct(proxy, tmpPacketId, charset);
        tmpPacketId = addCnVersion(proxy, tmpPacketId, charset);
        tmpPacketId = addDnVersion(proxy, tmpPacketId, charset);
        tmpPacketId = addCdcVersion(proxy, tmpPacketId, charset);
        tmpPacketId = addGmsVersion(proxy, tmpPacketId, charset);
        tmpPacketId = addColumnarVersion(proxy, tmpPacketId, charset);

        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = tmpPacketId;
        if (hasMore) {
            lastEof.status |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        proxy = lastEof.write(proxy);

        proxy.packetEnd();
        return true;
    }

    private static byte addProduct(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String type = "Product";
        final String productVersion = "PolarDB V2.0";
        final String productReleaseDate = "Distributed Edition";

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addToRow(row, type, productVersion, productReleaseDate, charset);
        row.packetId = packetId;
        row.write(proxy);

        return ++packetId;
    }

    static byte addGmsVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        String version = null;
        try {
            version = MetaDbUtil.getGmsPolardbVersion();
        } catch (Exception e) {
            logger.warn("Failed to get GMS version", e);
        }
        if (version != null) {
            final String nodeType = "GMS";
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            addVersionWithReleaseDate(row, nodeType, version, charset);
            row.packetId = packetId;
            row.write(proxy);
            return ++packetId;
        }
        return packetId;
    }

    private static byte addColumnarVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        String version = null;
        try {
            version = ColumnarVersionUtil.getVersion();
        } catch (Exception e) {
            logger.warn("Failed to get Columnar version", e);
        }
        if (version != null) {
            final String nodeType = "Columnar";
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            addVersionWithReleaseDate(row, nodeType, version, charset);
            row.packetId = packetId;
            row.write(proxy);
            return ++packetId;
        }
        return packetId;
    }

    private static byte addCdcVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        String version = null;
        try {
            version = CdcVersionUtil.getVersion();
        } catch (Exception e) {
            logger.warn("Failed to get CDC version", e);
        }
        if (version != null) {
            final String nodeType = "CDC";
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            addVersionWithReleaseDate(row, nodeType, version, charset);
            row.packetId = packetId;
            row.write(proxy);
            return ++packetId;
        }
        return packetId;
    }

    /**
     * may not exist when this is a COLUMNAR_SLAVE
     */
    static byte addDnVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        String version = null;
        try {
            version = ExecUtils.getDnPolardbVersion();
        } catch (Exception e) {
            logger.warn("Failed to get DN version", e);
        }
        if (version != null) {
            final String nodeType = "DN";
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            addVersionWithReleaseDate(row, nodeType, version, charset);
            row.packetId = packetId;
            row.write(proxy);
            return ++packetId;
        }
        return packetId;
    }

    private static byte addCnVersion(IPacketOutputProxy proxy, byte packetId, String charset) {
        final String nodeType = "CN";
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        addVersionWithReleaseDate(row, nodeType, Version.getVersion(), charset);
        row.packetId = packetId;
        row.write(proxy);
        return ++packetId;
    }

    /**
     * @param version format: {Version}-{ReleaseDate}
     */
    static void addVersionWithReleaseDate(RowDataPacket row, String type,
                                          String version, String charset) {
        String majorVersion = version;
        String releaseDate = null;
        boolean isLegalVersionFormat = false;
        try {
            if (!StringUtils.isBlank(version)) {
                String[] strs = StringUtils.split(version, "-");
                if (strs != null && strs.length == 2) {
                    majorVersion = strs[0];
                    releaseDate = strs[1];
                    if (releaseDate != null) {
                        // might be {ReleaseDate}_{BuildNumber}
                        int buildNumberIdx = releaseDate.indexOf("_");
                        if (buildNumberIdx != -1) {
                            releaseDate = releaseDate.substring(0, buildNumberIdx);
                        }
                    }
                    isLegalVersionFormat = true;
                }
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }

        if (!isLegalVersionFormat) {
            logger.warn("Failed to parse " + type + " version: " + version);
        }
        addToRowWithProductVersion(row, type, majorVersion, releaseDate, charset);
    }

    private static void addToRow(RowDataPacket row, String type,
                                 String version, String releaseDate,
                                 String charset) {
        row.add(StringUtil.encode(type, charset));
        row.add(StringUtil.encode(version, charset));
        row.add(StringUtil.encode(releaseDate, charset));
    }

    private static void addToRowWithProductVersion(RowDataPacket row, String type,
                                                   String version, String releaseDate,
                                                   String charset) {
        row.add(StringUtil.encode(type, charset));
        row.add(StringUtil.encode(getFullProductionVersion(version), charset));
        row.add(StringUtil.encode(releaseDate, charset));
    }

    public static String getFullProductionVersion(String version) {
        if (version == null) {
            return null;
        }
        return Version.PRODUCT_VERSION + "." + version;
    }
}
