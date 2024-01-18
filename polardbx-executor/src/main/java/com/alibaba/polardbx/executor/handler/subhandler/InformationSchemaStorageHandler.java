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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.ha.impl.StorageNodeHaInfo;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.node.StorageStatusManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFullStorage;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStorage;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStorageReplicas;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class InformationSchemaStorageHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaStorageHandler.class);

    public InformationSchemaStorageHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return (virtualView instanceof InformationSchemaStorage)
            || (virtualView instanceof InformationSchemaStorageReplicas)
            || (virtualView instanceof InformationSchemaFullStorage);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        boolean showStorageReplicas = virtualView instanceof InformationSchemaStorageReplicas;
        boolean isFull = virtualView instanceof InformationSchemaFullStorage;
        if (isFull) {
            List<Map<String, String>> storageInfos = getStorageInfoFromStorageHaManager();
            for (Map<String, String> storageInfoItem : storageInfos) {
                Object[] row = getFullRow(storageInfoItem);
                cursor.addRow(row);
            }
            return cursor;
        }
        List<Map<String, String>> storageInfos = getStorageInfoFromMetaDb(showStorageReplicas);
        for (Map<String, String> storageInfoItem : storageInfos) {
            Object[] row = getRow(storageInfoItem, showStorageReplicas);
            cursor.addRow(row);
        }
        return cursor;
    }

    protected static class StorageInstCtxSorter implements Comparator<StorageInstHaContext> {
        public StorageInstCtxSorter() {
        }

        @Override
        public int compare(StorageInstHaContext o1, StorageInstHaContext o2) {
            String o1SortKey =
                String.format("%s#%s#%s", o1.getStorageMasterInstId(), o1.getStorageKind(), o1.getStorageInstId());
            String o2SortKey =
                String.format("%s#%s#%s", o2.getStorageMasterInstId(), o2.getStorageKind(), o2.getStorageInstId());
            return o1SortKey.compareTo(o2SortKey);
        }
    }

    private List<Map<String, String>> getStorageInfoFromMetaDb(boolean showReplicas) {

        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

        List<Map<String, String>> storageInstInfoMaps = new ArrayList<>();

        TreeSet<StorageInstHaContext> dnInfos = new TreeSet<>(new StorageInstCtxSorter());
        dnInfos.addAll(storageInstHaCtxCache.values());

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            Set<String> nonDeletableStorage = DbTopologyManager.getNonDeletableStorageInst(metaDbConn);
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            dnInfos.stream().forEach(dnInfo -> {
                String storageInstId = dnInfo.getStorageInstId();
                String storageMasterInstId = dnInfo.getStorageMasterInstId();
                StorageInstHaContext ctx = dnInfo;
                String leaderNode = ctx.getCurrAvailableNodeAddr();
                boolean isLeaderHealthy = ctx.isCurrAvailableNodeAddrHealthy();
                int instKind = ctx.getStorageKind();
                String instKindStr = StorageInfoRecord.getInstKind(instKind);
                List<Integer> replicaStatus = ctx.getStorageInfo().stream()
                    .map(x -> x.status)
                    .distinct()
                    .collect(Collectors.toList());
                String statusStr = replicaStatus.size() == 1 ?
                    Integer.toString(replicaStatus.get(0)) :
                    StringUtils.join(replicaStatus, ",");

                Pair<Integer, Integer> dbCntAndGrpCnt =
                    groupDetailInfoAccessor.getDbCountAndGroupCountByStorageInstId(storageInstId);

                // If contains single group or default group, the instance
                boolean deletable =
                    DbTopologyManager.checkStorageInstDeletable(nonDeletableStorage, storageInstId, instKind);

                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("storageInstId", storageInstId);
                storageInstInfoMap.put("storageRwInstId", storageMasterInstId);
                storageInstInfoMap.put("leaderNode", leaderNode);
                storageInstInfoMap.put("isHealthy", String.valueOf(isLeaderHealthy));
                storageInstInfoMap.put("instKind", instKindStr);
                storageInstInfoMap.put("dbCnt", String.valueOf(dbCntAndGrpCnt.getKey()));
                storageInstInfoMap.put("groupCnt", String.valueOf(dbCntAndGrpCnt.getValue()));
                storageInstInfoMap.put("status", statusStr);
                storageInstInfoMap.put("deletable", BooleanUtils.toStringTrueFalse(deletable));

                if (showReplicas) {
                    storageInstInfoMap.put("replicas", dnInfo.getReplicaString());
                }
                storageInstInfoMaps.add(storageInstInfoMap);
            });

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }

        return storageInstInfoMaps;
    }

    private Object[] getRow(Map<String, String> storageInstInfoMap, boolean showReplicas) {
        Object[] rowVal = null;
        List<Object> row = new ArrayList<>();
        String storageId = storageInstInfoMap.get("storageInstId");
        row.add(storageId);
        row.add(storageInstInfoMap.get("leaderNode"));
        row.add(storageInstInfoMap.get("isHealthy"));
        row.add(storageInstInfoMap.get("instKind"));
        row.add(storageInstInfoMap.get("dbCnt"));
        row.add(storageInstInfoMap.get("groupCnt"));
        row.add(storageInstInfoMap.get("status"));
        row.add(storageInstInfoMap.get("deletable"));
        StorageStatus storageStatus = StorageStatusManager.getInstance().getStorageStatus().get(storageId);
        if (storageStatus != null) {
            row.add(storageStatus.getDelaySecond());
            row.add(storageStatus.getActiveSession());
        } else {
            row.add("null");
            row.add("null");
        }
        if (showReplicas) {
            row.add(storageInstInfoMap.get("replicas"));
            String storageRwInstId = storageInstInfoMap.get("storageRwInstId");
            row.add(storageRwInstId);
        }
        rowVal = row.stream().toArray();
        return rowVal;
    }

    protected List<Map<String, String>> getStorageInfoFromStorageHaManager() {

        Map<String, StorageInstHaContext> storageInstHaCtxCache =
            StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

        List<Map<String, String>> storageInstInfoMaps = new ArrayList<>();

        TreeSet<StorageInstHaContext> dnInfos = new TreeSet<>(new StorageInstCtxSorter());
        dnInfos.addAll(storageInstHaCtxCache.values());

        dnInfos.stream().forEach(dnInfo -> {
            StorageInstHaContext ctx = dnInfo;

            String dnId = ctx.getStorageInstId();
            String rwDnId = ctx.getStorageMasterInstId();
            String instKindStr = StorageInfoRecord.getInstKind(ctx.getStorageKind());

            /**
             * Leader Area
             */
            String leaderNode = ctx.getCurrAvailableNodeAddr();
            if (leaderNode != null) {
                String leaderUser = ctx.getUser();
                String leaderPasswdEnc = getPasswdEncWithStar(ctx.getEncPasswd());
                String isLeaderHealthy = String.valueOf(ctx.isCurrAvailableNodeAddrHealthy());
                String leaderXport = String.valueOf(ctx.getCurrXport());
                String leaderAddrVipFlag = String.valueOf(ctx.isCurrIsVip());
                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("dn", dnId);
                storageInstInfoMap.put("rwDn", rwDnId);
                storageInstInfoMap.put("kind", instKindStr);
                storageInstInfoMap.put("node", leaderNode);
                storageInstInfoMap.put("user", leaderUser);
                storageInstInfoMap.put("passwdEnc", leaderPasswdEnc);
                storageInstInfoMap.put("xport", leaderXport);
                storageInstInfoMap.put("role", StorageRole.LEADER.getRole());
                storageInstInfoMap.put("isHealthy", isLeaderHealthy);
                storageInstInfoMap.put("isVip", leaderAddrVipFlag);
                storageInstInfoMap.put("info_from", StorageHaManager.AREA_TYPE_HA_SWITCHER);
                storageInstInfoMaps.add(storageInstInfoMap);
            }

            /**
             * Memory Area
             */
            Map<String, StorageNodeHaInfo> haInfoInMemory = ctx.getAllStorageNodeHaInfoMap();
            for (Map.Entry<String, StorageNodeHaInfo> haInfoItem : haInfoInMemory.entrySet()) {
                String addr = haInfoItem.getKey();
                StorageNodeHaInfo nodeInfo = haInfoItem.getValue();
                String roleStr = nodeInfo.getRole().getRole();
                String nodeUser = nodeInfo.getUser();
                String nodePasswdEnc = getPasswdEncWithStar(nodeInfo.getEncPasswd());
                String isNodeHealthy = String.valueOf(nodeInfo.isHealthy());
                String nodeXport = String.valueOf(nodeInfo.getXPort());
                String nodeAddrVipFlag = String.valueOf(nodeInfo.isVip());

                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("dn", dnId);
                storageInstInfoMap.put("rwDn", rwDnId);
                storageInstInfoMap.put("kind", instKindStr);
                storageInstInfoMap.put("node", addr);
                storageInstInfoMap.put("user", nodeUser);
                storageInstInfoMap.put("passwdEnc", nodePasswdEnc);
                storageInstInfoMap.put("xport", nodeXport);
                storageInstInfoMap.put("role", roleStr);
                storageInstInfoMap.put("isHealthy", isNodeHealthy);
                storageInstInfoMap.put("isVip", nodeAddrVipFlag);
                storageInstInfoMap.put("info_from", StorageHaManager.AREA_TYPE_HA_CHECKER);
                storageInstInfoMaps.add(storageInstInfoMap);
            }

            /**
             * MetaDB Area
             */
            Map<String, StorageInfoRecord> nodeInfoInMetaDb = ctx.getStorageNodeInfos();
            if (ctx.getStorageVipAddr() != null) {
                String vipAddr = ctx.getCurrAvailableNodeAddr();
                String vipUser = ctx.getStorageVipUser();
                String vipPasswdEnc = ctx.getStorageVipEncPasswd();
                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("dn", dnId);
                storageInstInfoMap.put("rwDn", rwDnId);
                storageInstInfoMap.put("kind", instKindStr);
                storageInstInfoMap.put("node", vipAddr);
                storageInstInfoMap.put("user", vipUser);
                storageInstInfoMap.put("passwdEnc", getPasswdEncWithStar(vipPasswdEnc));
                storageInstInfoMap.put("xport", "-1");
                storageInstInfoMap.put("role", "");
                storageInstInfoMap.put("isHealthy", "");
                storageInstInfoMap.put("isVip", "true");
                storageInstInfoMap.put("info_from", StorageHaManager.AREA_TYPE_META_DB);
                storageInstInfoMaps.add(storageInstInfoMap);
            }
            for (Map.Entry<String, StorageInfoRecord> nodeRecItem : nodeInfoInMetaDb.entrySet()) {
                String addr = nodeRecItem.getKey();
                StorageInfoRecord nodeInfo = nodeRecItem.getValue();
                String nodeRecUser = nodeInfo.user;
                String nodeRecPasswdEnc = getPasswdEncWithStar(nodeInfo.passwdEnc);
                String nodeXport = String.valueOf(nodeInfo.xport);
                if (nodeInfo.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                    continue;
                }
                Map<String, String> storageInstInfoMap = new HashMap<>();
                storageInstInfoMap.put("dn", dnId);
                storageInstInfoMap.put("rwDn", rwDnId);
                storageInstInfoMap.put("kind", instKindStr);
                storageInstInfoMap.put("node", addr);
                storageInstInfoMap.put("user", nodeRecUser);
                storageInstInfoMap.put("passwdEnc", nodeRecPasswdEnc);
                storageInstInfoMap.put("xport", nodeXport);
                storageInstInfoMap.put("role", "");
                storageInstInfoMap.put("isHealthy", "");
                storageInstInfoMap.put("isVip", "false");
                storageInstInfoMap.put("info_from", StorageHaManager.AREA_TYPE_META_DB);
                storageInstInfoMaps.add(storageInstInfoMap);
            }
        });

        return storageInstInfoMaps;

    }

    private Object[] getFullRow(Map<String, String> storageInstInfoMap) {
        Object[] rowVal = null;
        List<Object> row = new ArrayList<>();
        row.add(storageInstInfoMap.get("dn"));
        row.add(storageInstInfoMap.get("rwDn"));
        row.add(storageInstInfoMap.get("kind"));
        row.add(storageInstInfoMap.get("node"));
        row.add(storageInstInfoMap.get("user"));
        row.add(storageInstInfoMap.get("passwdEnc"));
        row.add(storageInstInfoMap.get("xport"));
        row.add(storageInstInfoMap.get("role"));
        row.add(storageInstInfoMap.get("isHealthy"));
        row.add(storageInstInfoMap.get("isVip"));
        row.add(storageInstInfoMap.get("info_from"));
        rowVal = row.stream().toArray();
        return rowVal;
    }

    private String getPasswdEncWithStar(String passwdEnc) {
        StringBuilder passwdSb = new StringBuilder("");
        passwdSb.append(passwdEnc.substring(0, 5));
        passwdSb.append("******");
        return passwdSb.toString();
    }
}
