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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.stats.MatrixStatistics;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * 查询当前系统配置
 *
 * @author agapple 2014年12月12日 上午12:20:45
 * @since 5.1.17
 */
public final class ShowStc {

    private static final int FIELD_COUNT = 33;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DBNAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MYSQLADDR", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("APPNAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("GROUPNAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ATOMNAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("READCOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("WRITECOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTALCOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("READTIMECOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("WRITETIMECOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTALTIMECOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CONNERRCOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQLERRCOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQLLENGTH", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ROWS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PHY_ACTIVE_CONNECTIONS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PHY_POOLING_CONNECTIONS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        // For X-Protocol.
        fields[i] = PacketUtil.getField("QUERY_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("UPDATE_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TSO_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_RESPOND_TIME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TOTAL_PHYSICAL_TIME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_PLAN_REQUEST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_SQL_REQUEST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_PLAN_MISS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CACHE_SQL_MISS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("POOL_DIGEST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SEND_MSG_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SEND_FLUSH_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SEND_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RECV_MSG_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RECV_NET_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RECV_SIZE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        Map<String, SchemaConfig> schemas = CobarServer.getInstance().getConfig().getSchemas();
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

        List<List<Object>> list = MatrixStatistics.getStcInfo();
        for (List<Object> obs : list) {
            RowDataPacket row = getRow(obs, c.getCharset(), schemas);
            row.packetId = ++packetId;
            proxy = row.write(proxy);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getRow(List<Object> obs, String charset, Map<String, SchemaConfig> schemas) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        for (Object obj : obs) {
            row.add(StringUtil.encode(obj.toString(), charset));
        }

        boolean poolCntSet = false;
        try {
            // Add connection pool info.
            final String schema = obs.get(0).toString();
            final String groupName = obs.get(3).toString();
            final String atomName = obs.get(4).toString();
            TDataSource ds = schemas.containsKey(schema) ? schemas.get(schema).getDataSource() : null;
            // Not found? Try scan use appname.
            if (null == ds) {
                final String appName = obs.get(2).toString();
                for (SchemaConfig config : schemas.values()) {
                    if (config.getDataSource().getAppName().equalsIgnoreCase(appName)) {
                        ds = config.getDataSource();
                        break;
                    }
                }
            }
            if (ds != null && ds.isInited()) {
                for (TGroupDataSource groupDataSource : ds.getGroupDataSources()) {
                    if (!groupDataSource.getDbGroupKey().equalsIgnoreCase(groupName)) {
                        continue;
                    }
                    DataSourceWrapper dataSourceWrapper =
                        groupDataSource.getConfigManager().getDataSourceWrapperMap().get(atomName);
                    if (dataSourceWrapper != null) {
                        final DataSource rawDS = dataSourceWrapper.getWrappedDataSource().getDataSource();
                        if (rawDS instanceof XDataSource) {
                            final XDataSource x = (XDataSource) rawDS;
                            final XClientPool.XStatus s = x.getStatus();
                            row.add(StringUtil.encode(Integer.toString(s.workingSession), charset));
                            row.add(StringUtil.encode(Integer.toString(s.idleSession), charset));
                            row.add(StringUtil.encode(Long.toString(x.getQueryCount().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getUpdateCount().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getTsoCount().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getTotalRespondTime().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getTotalPhysicalTime().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getCachePlanQuery().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getCacheSqlQuery().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getCachePlanMiss().get()), charset));
                            row.add(StringUtil.encode(Long.toString(x.getCacheSqlMiss().get()), charset));
                            row.add(StringUtil.encode(x.getDigest(), charset));
                            final XClientPool p = x.getClientPool();
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getSendMsgCount().get()), charset));
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getSendFlushCount().get()), charset));
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getSendSize().get()), charset));
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getRecvMsgCount().get()), charset));
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getRecvNetCount().get()), charset));
                            row.add(StringUtil.encode(
                                Long.toString(p.getPerfCollection().getRecvSize().get()), charset));
                            poolCntSet = true;
                        }
                    }
                    break;
                }
            }
        } catch (Throwable ignore) {
        } finally {
            if (!poolCntSet) {
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
            }
        }
        return row;
    }

}
