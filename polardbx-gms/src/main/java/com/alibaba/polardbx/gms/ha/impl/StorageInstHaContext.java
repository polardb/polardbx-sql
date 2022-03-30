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

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import lombok.val;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The ha info of storage inst
 *
 * @author chenghui.lch
 */
public class StorageInstHaContext {

    public enum StorageHaStatus {
        NORMAL, SWITCHING
    }

    protected String instId;

    protected String storageInstId;

    protected String storageMasterInstId;

    protected boolean isMasterMode = false;

    protected String user;

    protected String encPasswd;

    /**
     * the type of storage (such as xcluster/mysql/polardb)
     */
    protected int storageType;

    /**
     * the storage inst kind (such master inst / slave inst / metadb inst)
     */
    protected int storageKind;

    /**
     * the ha status for the storage inst
     * <pre>
     *      the ha status will be updated during running  StorageHaManager.StorageHaSwitchTask
     * </pre>
     */
    protected volatile StorageHaStatus haStatus = StorageHaStatus.NORMAL;

    /**
     * The vip addr of storage, sometime it will be null
     */
    protected volatile String storageVipAddr;

    /**
     * For PolarDB-X master, currAvailableNodeAddr is leader
     * <p>
     * For PolarDB-X slave, currAvailableNodeAddr is learner
     * <pre>
     *      The available addr is checked and refreshed by StorageHaManager.CheckStorageHaTask at intervals of 5 seconds
     * </pre>
     */
    protected volatile String currAvailableNodeAddr;

    /**
     * label if the currAvailableNodeAddr is healthy
     */
    protected volatile boolean isCurrAvailableNodeAddrHealthy = true;

    /**
     * HaInfos(included role info & healthy info) for all nodes of a storage inst
     * They are the dynamic healthy info for all node in paxos-group
     * <pre>
     *     key: addr of storage node
     *     val: role info & healthy of storage node.
     *
     *     The HA information is checked and refreshed by StorageHaManager.CheckStorageHaTask at intervals of 5 seconds
     * </pre>
     */
    protected volatile Map<String, StorageNodeHaInfo> allStorageNodeHaInfoMap;

    /**
     * The storage nodes that loaded from metaDB that its vip info is excluded for x-cluster storage type
     * <pre>
     *     When the storage_info of metaDB is modified, it will be refreshed by the callback of ConfigListener.
     *     So, maybe the addr list of storageNodeInfos are different from allStorageNodeHaInfoMap if xcluster has
     *     been added new paxos-group node or removed old paxos-group.
     * </pre>
     */
    protected volatile Map<String, StorageInfoRecord> storageNodeInfos;

    /**
     * <pre>
     *     All TGroupDataSource need to get read lock of storage to do initialization, and
     *     HA switch task need to get write lock to do switch task for current storage.
     *     This lock is used to avoid the TGroupDataSource initialization during its storage is doing ha switch task.
     * </pre>
     */
    protected final ReentrantReadWriteLock haLock = new ReentrantReadWriteLock();

    protected static StorageInstHaContext buildStorageInstHaContext(String instId,
                                                                    String storageInstId,
                                                                    String storageMasterInstId,
                                                                    String user,
                                                                    String encPasswd,
                                                                    int storageType,
                                                                    int instKind,
                                                                    String vipAddr,
                                                                    Map<String, StorageNodeHaInfo> storageNodeHaInfoMap,
                                                                    Map<String, StorageInfoRecord> addrStorageNodeMap) {

        StorageInstHaContext storageInstHaContext = new StorageInstHaContext();

        storageInstHaContext.instId = instId;
        storageInstHaContext.storageInstId = storageInstId;
        storageInstHaContext.storageMasterInstId = storageMasterInstId;
        storageInstHaContext.user = user;
        storageInstHaContext.encPasswd = encPasswd;
        storageInstHaContext.storageType = storageType;
        storageInstHaContext.storageKind = instKind;
        storageInstHaContext.storageVipAddr = vipAddr;
        storageInstHaContext.storageNodeInfos = addrStorageNodeMap;

        boolean isMasterMode = storageInstHaContext.storageKind != StorageInfoRecord.INST_KIND_SLAVE;
        storageInstHaContext.isMasterMode = isMasterMode;
        if (isMasterMode) {
            // Find leader for master mode
            String leaderAddr = null;
            for (Map.Entry<String, StorageNodeHaInfo> nodeRoleItem : storageNodeHaInfoMap.entrySet()) {
                String nodeAddr = nodeRoleItem.getKey();
                StorageNodeHaInfo nodeRole = nodeRoleItem.getValue();
                if (StorageRole.LEADER == nodeRole.getRole()) {
                    leaderAddr = nodeAddr;
                    break;
                }
            }
            storageInstHaContext.currAvailableNodeAddr = leaderAddr;
        } else {
            // Find healthy learner for slave mode
            String healthyLearnerAddr = null;
            for (Map.Entry<String, StorageNodeHaInfo> nodeRoleItem : storageNodeHaInfoMap.entrySet()) {
                String nodeAddr = nodeRoleItem.getKey();
                StorageNodeHaInfo nodeRole = nodeRoleItem.getValue();
                if (StorageRole.LEARNER == nodeRole.getRole() && nodeRole.isHealthy) {
                    healthyLearnerAddr = nodeAddr;
                    break;
                }
            }
            storageInstHaContext.currAvailableNodeAddr = healthyLearnerAddr;
            if (healthyLearnerAddr == null) {
                storageInstHaContext.isCurrAvailableNodeAddrHealthy = false;
            }
        }
        storageInstHaContext.allStorageNodeHaInfoMap = storageNodeHaInfoMap;
        return storageInstHaContext;
    }

    public String getStorageInstId() {
        return storageInstId;
    }

    public boolean isMasterMode() {
        return isMasterMode;
    }

    public boolean isMetaDb() {
        return this.storageKind == StorageInfoRecord.INST_KIND_META_DB;
    }

    public boolean isDNMaster() {
        return this.storageKind == StorageInfoRecord.INST_KIND_MASTER;
    }

    public String getUser() {
        return user;
    }

    public String getEncPasswd() {
        return encPasswd;
    }

    public String getCurrAvailableNodeAddr() {
        return currAvailableNodeAddr;
    }

    public int getStorageKind() {
        return storageKind;
    }

    public String getInstId() {
        return instId;
    }

    public boolean isCurrAvailableNodeAddrHealthy() {
        return isCurrAvailableNodeAddrHealthy;
    }

    public String getStorageMasterInstId() {
        return storageMasterInstId;
    }

    public Map<String, StorageNodeHaInfo> getAllStorageNodeHaInfoMap() {
        return allStorageNodeHaInfoMap;
    }

    public StorageInfoRecord getLeaderNode() {
        for (Map.Entry<String, StorageNodeHaInfo> node : allStorageNodeHaInfoMap.entrySet()) {
            String instId = node.getKey();
            StorageNodeHaInfo info = node.getValue();
            if (info.getRole() == StorageRole.LEADER) {
                return storageNodeInfos.get(instId);
            }
        }
        return null;
    }

    public List<StorageInfoRecord> getReplicaByZone(String zone) {
        Objects.requireNonNull(zone);

        return storageNodeInfos.values().stream()
            .filter(x -> zone.equals(x.getAzoneId()))
            .collect(Collectors.toList());
    }

    public String getReplicaString() {
        List<String> replicaList = new ArrayList<>();
        List<StorageNodeHaInfo> replicas = allStorageNodeHaInfoMap.values().stream()
            .sorted(Comparator.comparing(StorageNodeHaInfo::getRole)).collect(Collectors.toList());
        for (val replica : replicas) {
            StorageInfoRecord dnNodeRec = storageNodeInfos.get(replica.getAddr());
            String zone = "unknown";
            if (dnNodeRec != null) {
                zone = dnNodeRec.getAzoneId();
            }
            String str = String.format("%s/%s/%s", replica.getRole(), replica.getAddr(), zone);
            replicaList.add(str);
        }
        return StringUtils.join(replicaList, ",");
    }

    public StorageInfoRecord getNodeInfoByAddress(String address) {
        return storageNodeInfos.get(address);
    }

    public StorageNodeHaInfo getNodeHaInfoByAddress(String address) {
        return allStorageNodeHaInfoMap.get(address);
    }

    public boolean hasReplica(String address) {
        return allStorageNodeHaInfoMap.containsKey(address);
    }

    public Collection<StorageInfoRecord> getStorageInfo() {
        return this.storageNodeInfos.values();
    }

    public boolean isAllReplicaReady() {
        return getStorageInfo().stream().allMatch(StorageInfoRecord::isStatusReady);
    }

    public boolean containsReplicaAtAzone(String primaryZone) {
        Objects.requireNonNull(primaryZone);

        for (val replica : storageNodeInfos.values()) {
            if (primaryZone.equals(replica.getAzoneId())) {
                return true;
            }
        }
        return false;
    }


    public String getStorageVipAddr() {
        return storageVipAddr;
    }

    public void setStorageVipAddr(String storageVipAddr) {
        this.storageVipAddr = storageVipAddr;
    }


    public ReentrantReadWriteLock getHaLock() {
        return haLock;
    }

    @Override
    public String toString() {
        return "StorageInstHaContext{" +
            "instId='" + instId + '\'' +
            ", storageInstId='" + storageInstId + '\'' +
            ", storageMasterInstId='" + storageMasterInstId + '\'' +
            ", haStatus=" + haStatus +
            ", storageNodeInfos=" + storageNodeInfos +
            '}';
    }
}
