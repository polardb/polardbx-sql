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

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author fangwu
 */
public class ShowParametric {

    private static int FIELD_COUNT = 14;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("DATABASE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BASELINE_ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PARAMETER_SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PLAN_ID", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PLAN_INFO", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ACCEPTED", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SELECTIVITY_SPACE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INFLATION_NARROW", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PARAMS", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHOOSE_TIME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RECENTLY_CHOOSE_RATE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXPECTED_ROWS", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MAX_ROWS_FEEDBACK", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("MIN_ROWS_FEEDBACK", Fields.FIELD_TYPE_DOUBLE);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void response(ServerConnection c) {
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

        String schemaName = c.getSchema();
        if (OptimizerContext.getContext(schemaName) != null) {
            Map<String, BaselineInfo> baselineInfoMap = PlanManager.getInstance().getBaselineMap(schemaName);
            if (baselineInfoMap != null) {
                for (BaselineInfo baselineInfo : baselineInfoMap.values()) {
                    if (baselineInfo == null) {
                        continue;
                    }
                    for (Point point : baselineInfo.getPointSet()) {
                        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
                        row.add(StringUtil.encode(schemaName, c.getCharset()));
                        row.add(StringUtil.encode(baselineInfo.getId() + "", c.getCharset()));
                        row.add(StringUtil.encode(baselineInfo.getParameterSql().replaceAll("\n", " ").trim(),
                            c.getCharset()));
                        long planId = point.getPlanId();
                        row.add(StringUtil.encode(planId + "", c.getCharset()));
                        /**
                         * plan info
                         */
                        if (planId == -1) {
                            continue;
                        } else {
                            PlanInfo planInfo = baselineInfo.getPlan((int) planId);
                            if (planInfo == null || !planInfo.inited()) {
                                row.add(null);
                                row.add(null);
                            } else {

                                RelNode plan = planInfo.getPlan(null, null);
                                String planExplain = null;
                                if (plan != null) {
                                    planExplain = "\n" + RelOptUtil.dumpPlan("",
                                        plan,
                                        SqlExplainFormat.TEXT,
                                        SqlExplainLevel.NO_ATTRIBUTES);
                                }
                                row.add(StringUtil.encode(planExplain, c.getCharset()));
                                row.add(StringUtil.encode(planInfo.isAccepted() ? "TRUE" : "FALSE", c.getCharset()));
                            }
                        }
                        NumberFormat numberFormat = NumberFormat.getPercentInstance();
                        row.add(StringUtil.encode(point.getSelectivityMap().toString(), c.getCharset()));
                        row.add(StringUtil.encode(point.getInflationNarrow() + "", c.getCharset()));
                        row.add(StringUtil.encode(point.getParams().toString(), c.getCharset()));
                        row.add(StringUtil.encode(point.getChooseTime() + "", c.getCharset()));
                        row.add(StringUtil
                            .encode(numberFormat.format(point.getLastRecentlyChooseRate()) + "", c.getCharset()));
                        row.add(StringUtil.encode(point.getRowcountExpected() + "", c.getCharset()));
                        row.add(StringUtil.encode(point.getMaxRowcountExpected() + "", c.getCharset()));
                        row.add(StringUtil.encode(point.getMinRowcountExpected() + "", c.getCharset()));
                        row.packetId = ++packetId;
                        proxy = row.write(proxy);
                    }
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // post write
        proxy.packetEnd();
    }

    public static TreeSet<String> getSchemas(PrivilegeContext pc) {
        return getSchemasInternal(pc.getUser(),
            pc.getHost(),
            pc.getSchema(),
            pc.getPrivileges(),
            pc.isTrustLogin(),
            pc.isManaged());
    }

    private static TreeSet<String> getSchemas(ServerConnection c) {
        return getSchemasInternal(c.getUser(),
            c.getHost(),
            c.getSchema(),
            c.getPrivileges(),
            c.isTrustLogin(),
            c.isManaged());
    }

    private static TreeSet<String> getSchemasInternal(String user, String host, String schema, Privileges privileges,
                                                      boolean isTrustLogin, boolean isManaged) {
        TreeSet<String> schemaSet = new TreeSet<>();
        CobarConfig conf = CobarServer.getInstance().getConfig();

        boolean isTrustedIp = privileges.isTrustedIp(host, user);
        // 没有指定数据库，同时为trustedIp的时候需要显示所有的数据库.
        if (schema == null && isTrustedIp) {
            Map<String, SchemaConfig> schemaConfigMap = conf.getSchemas();
            if (schemaConfigMap != null) {
                schemaSet.addAll(schemaConfigMap.keySet());
            }
        } else {
            Set<String> _userSchemas = privileges.getUserSchemas(user, host);
            if (_userSchemas != null) {
                schemaSet.addAll(_userSchemas);
            }
        }

        schemaSet.remove(SystemDbHelper.DEFAULT_DB_NAME);
        schemaSet.remove(SystemDbHelper.CDC_DB_NAME);
        return schemaSet;
    }

}
