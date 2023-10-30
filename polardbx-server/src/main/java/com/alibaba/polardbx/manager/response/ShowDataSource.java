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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MysqlResultSetPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author 梦实 2016年11月8日 下午2:04:25
 * @since 5.0.0
 */
public final class ShowDataSource {

    private static final int FIELD_COUNT = 17;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final Logger logger = LoggerFactory.getLogger(ShowDataSource.class);

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("GROUP", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("URL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INIT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MIN", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("IDLE_TIMEOUT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_WAIT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("POOLING_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ATOM", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("READ_WEIGHT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("WRITE_WEIGHT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, String name) {
        Map<String, SchemaConfig> schemas = CobarServer.getInstance().getConfig().getSchemas();
        MysqlResultSetPacket packet = new MysqlResultSetPacket();
        String charset = c.getCharset();
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

        try {
            for (SchemaConfig schema : schemas.values()) {
                TDataSource ds = schema.getDataSource();
                if (!ds.isInited()) {
                    continue;
                }

                int index = 0;

                List<TGroupDataSource> groups = ds.getGroupDataSources();

                for (TGroupDataSource groupDs : groups) {

                    final Map<String, DataSourceWrapper> dataSourceWrapperMap = groupDs.getConfigManager()
                        .getDataSourceWrapperMap();

                    for (Entry<String, DataSourceWrapper> atomEntry : dataSourceWrapperMap.entrySet()) {
                        final String dbKey = atomEntry.getKey();
                        final DataSourceWrapper dataSourceWrapper = atomEntry.getValue();
                        final Weight weightVal = dataSourceWrapper.getWeight();
                        int w = -1;
                        int r = -1;
                        if (weightVal != null) {
                            w = weightVal.w;
                            r = weightVal.r;
                        }

                        final TAtomDataSource atom = dataSourceWrapper.getWrappedDataSource();
                        final DataSource rawDS = atom.getDataSource();
                        if (rawDS instanceof XDataSource) {
                            final XDataSource x = (XDataSource) rawDS;

                            // TODO: not show.
                        } else if (rawDS instanceof DruidDataSource) {
                            DruidDataSource d = (DruidDataSource) rawDS;

                            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                            row.add(LongUtil.toBytes(index++));
                            row.add(StringUtil.encode(ds.getAppName(), charset));
                            row.add(StringUtil.encode(d.getName(), charset));
                            row.add(StringUtil.encode(groupDs.getDbGroupKey(), charset));
                            row.add(StringUtil.encode(d.getUrl(), charset));
                            row.add(StringUtil.encode(d.getUsername(), charset));
                            row.add(StringUtil.encode(d.getDbType(), charset));
                            row.add(StringUtil.encode(String.valueOf(d.getInitialSize()), charset));
                            row.add(StringUtil.encode(String.valueOf(d.getMinIdle()), charset));
                            row.add(StringUtil.encode(String.valueOf(d.getMaxActive()), charset));
                            row.add(
                                StringUtil.encode(String.valueOf(d.getTimeBetweenEvictionRunsMillis() / (1000 * 60)),
                                    charset));
                            row.add(StringUtil.encode(String.valueOf(d.getMaxWait()), charset));
                            row.add(StringUtil.encode(String.valueOf(d.getActiveCount()), charset));
                            row.add(StringUtil.encode(String.valueOf(d.getPoolingCount()), charset));
                            row.add(StringUtil.encode(dbKey, charset));
                            row.add(StringUtil.encode(String.valueOf(r), charset));
                            row.add(StringUtil.encode(String.valueOf(w), charset));
                            row.packetId = ++packetId;
                            proxy = row.write(proxy);
                        }
                    } // end of for
                } // end of for
            } // end of for
        } catch (Exception ex) {
            logger.error("", ex);

            c.handleError(ErrorCode.ERR_HANDLE_DATA, ex);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    public static TAtomDataSource getAtomDatasource(DataSource s) {
        if (s instanceof TAtomDataSource) {
            return (TAtomDataSource) s;
        }

        if (s instanceof DataSourceWrapper) {
            return getAtomDatasource(((DataSourceWrapper) s).getWrappedDataSource());
        }

        throw new IllegalAccessError();
    }
}
