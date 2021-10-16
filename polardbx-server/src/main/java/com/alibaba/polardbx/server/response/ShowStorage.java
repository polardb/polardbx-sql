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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 查看线程池状态
 *
 * @author chenghui.lch
 */
public final class ShowStorage {

    private static final int FIELD_COUNT = 8;
    private static final int EXTRA_FIELD_COUNT = 1;

    private static final ResultSetHeaderPacket HEADER_PACKET = PacketUtil.getHeader(FIELD_COUNT);
    private static final ResultSetHeaderPacket EXTRA_HEADER_PACKET =
        PacketUtil.getHeader(FIELD_COUNT + EXTRA_FIELD_COUNT);

    private static final FieldPacket[] FIELD_PACKETS = new FieldPacket[FIELD_COUNT];
    private static final FieldPacket[] EXTRA_PACKETS = new FieldPacket[EXTRA_FIELD_COUNT];
    private static final EOFPacket EOF_PACKET = new EOFPacket();
    private static final EOFPacket EXTRA_EOF_PACKET = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER_PACKET.packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("STORAGE_INST_ID", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("LEADER_NODE", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("IS_HEALTHY", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("INST_KIND", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("DB_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("GROUP_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        FIELD_PACKETS[i] = PacketUtil.getField("DELETABLE", Fields.FIELD_TYPE_VAR_STRING);
        FIELD_PACKETS[i++].packetId = ++packetId;

        EOF_PACKET.packetId = ++packetId;

        i = 0;
        EXTRA_PACKETS[i] = PacketUtil.getField("REPLICAS", Fields.FIELD_TYPE_VAR_STRING);
        EXTRA_PACKETS[i++].packetId = packetId;

        EXTRA_EOF_PACKET.packetId = ++packetId;
    }

    public static void execute(ServerConnection c) {
        execute(c, false);
    }

    public static void execute(ServerConnection c, boolean showReplicas) {
        ByteBufferHolder buffer = c.allocate();
        String charset = c.getCharset();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        executeInternal(proxy, charset, showReplicas);
    }

    public static void executeInternal(IPacketOutputProxy proxy, String charset, boolean showReplicas) {

        proxy.packetBegin();

        // write header
        if (!showReplicas) {
            proxy = HEADER_PACKET.write(proxy);
        } else {
            proxy = EXTRA_HEADER_PACKET.write(proxy);
        }

        // write fields
        for (FieldPacket field : FIELD_PACKETS) {
            proxy = field.write(proxy);
        }
        byte packetId;
        if (showReplicas) {
            for (FieldPacket field : EXTRA_PACKETS) {
                proxy = field.write(proxy);
            }
            proxy = EXTRA_EOF_PACKET.write(proxy);
            packetId = EXTRA_EOF_PACKET.packetId;
        } else {
            proxy = EOF_PACKET.write(proxy);
            packetId = EOF_PACKET.packetId;
        }

        List<Map<String, String>> storageInfos = getStorageInfoFromMetaDb(showReplicas);
        for (Map<String, String> storageInfoMap : storageInfos) {
            RowDataPacket row = getRow(storageInfoMap, charset, showReplicas);
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

    private static List<Map<String, String>> getStorageInfoFromMetaDb(boolean showReplicas) {

        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

        List<Map<String, String>> storageInstInfoMaps = new ArrayList<>();

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            for (Map.Entry<String, StorageInstHaContext> haCtxItem : storageInstHaCtxCache.entrySet()) {
                String storageInstId = haCtxItem.getKey();
                StorageInstHaContext ctx = haCtxItem.getValue();
                String leaderNode = ctx.getCurrAvailableNodeAddr();
                boolean isLeaderHealthy = ctx.isCurrAvailableNodeAddrHealthy();
                int instKind = ctx.getStorageKind();
                String instKindStr = getInstKind(instKind);
                List<Integer> replicaStatus = ctx.getStorageInfo().stream()
                    .map(x -> x.status)
                    .distinct()
                    .collect(Collectors.toList());
                String statusStr = replicaStatus.size() == 1 ?
                    Integer.toString(replicaStatus.get(0)) :
                    StringUtils.join(replicaStatus, ",");

                Pair<Integer, Integer> dbCntAndGrpCnt =
                    groupDetailInfoAccessor.getDbCountAndGroupCountByStorageInstId(storageInstId);

                boolean deletable = instKind != StorageInfoRecord.INST_KIND_META_DB &&
                    !DbTopologyManager.singleGroupStorageInstList.contains(storageInstId);

                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("storageInstId", storageInstId);
                storageInstInfoMap.put("leaderNode", leaderNode);
                storageInstInfoMap.put("isHealthy", String.valueOf(isLeaderHealthy));
                storageInstInfoMap.put("instKind", instKindStr);
                storageInstInfoMap.put("dbCnt", String.valueOf(dbCntAndGrpCnt.getKey()));
                storageInstInfoMap.put("groupCnt", String.valueOf(dbCntAndGrpCnt.getValue()));
                storageInstInfoMap.put("status", statusStr);
                storageInstInfoMap.put("deletable", BooleanUtils.toStringTrueFalse(deletable));

                if (showReplicas) {
                    storageInstInfoMap.put("replicas", haCtxItem.getValue().getReplicaString());
                }
                storageInstInfoMaps.add(storageInstInfoMap);
            }

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }

        return storageInstInfoMaps;
    }

    private static RowDataPacket getRow(Map<String, String> storageInstInfoMap, String charset, boolean showReplicas) {
        int fieldCount = !showReplicas ? FIELD_COUNT : FIELD_COUNT + EXTRA_FIELD_COUNT;
        RowDataPacket row = new RowDataPacket(fieldCount);
        row.add(StringUtil.encode(storageInstInfoMap.get("storageInstId"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("leaderNode"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("isHealthy"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("instKind"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("dbCnt"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("groupCnt"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("status"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("deletable"), charset));
        row.add(StringUtil.encode(storageInstInfoMap.get("replicas"), charset));
        return row;
    }

    public static String getInstKind(int instKind) {
        switch (instKind) {
        case StorageInfoRecord.INST_KIND_MASTER:
            return "MASTER";
        case StorageInfoRecord.INST_KIND_SLAVE:
            return "SLAVE";
        case StorageInfoRecord.INST_KIND_META_DB:
            return "META_DB";
        default:
            return "NA";
        }
    }

}
