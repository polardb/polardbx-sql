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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.map.MultiKeyMap;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupStorageInfoManager {

    private String schemaName;
    /**
     * record the mapping between PartitionId and DNId.
     * tableGroupId -> partitionId -> dnId
     */
    private Map<Long, Map<Long, String>> partitionsToDnMapping = new ConcurrentHashMap<>();

    public GroupStorageInfoManager(String schemaName) {
        this.schemaName = schemaName;
    }

    public Map<Long, String> getPartitionDNs(Long trgId) {
        return partitionsToDnMapping.get(trgId);
    }

    public String getPartitionDN(Long trgId, Long parId) {
        Map<Long, String> ret = partitionsToDnMapping.get(trgId);
        if (ret != null) {
            return ret.get(parId);
        }
        return null;
    }

    public void clear() {
        synchronized (partitionsToDnMapping) {
            partitionsToDnMapping.clear();
        }
    }

    public void unregisterTableGroupId(Long... tableGroupIds) {
        synchronized (partitionsToDnMapping) {
            for (Long tableGroupId : tableGroupIds) {
                partitionsToDnMapping.remove(tableGroupId);
            }
        }
    }

    public void registerTableGroupId(Collection<TableGroupConfig> tableGroupConfigInfoCache) {
        registerTableGroupId(new Function<Connection, List<PartitionGroupRecord>>() {
            @Override
            public List<PartitionGroupRecord> apply(Connection connection) {
                return tableGroupConfigInfoCache.stream().flatMap(
                    t -> t.getPartitionGroupRecords().stream()).collect(Collectors.toList());
            }
        });
    }

    public void registerTableGroupId(List<Long> tableGroupIds) {
        registerTableGroupId(new Function<Connection, List<PartitionGroupRecord>>() {
            @Override
            public List<PartitionGroupRecord> apply(Connection connection) {
                if (tableGroupIds.size() > 0) {
                    PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
                    partitionGroupAccessor.setConnection(connection);
                    return partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupIds);
                }
                return ImmutableList.of();
            }
        });
    }

    private void registerTableGroupId(Function<Connection, List<PartitionGroupRecord>> function) {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            List<PartitionGroupRecord> partitionGroupRecords = function.apply(metaDbConn);
            //groupName -> (tgId -> partitionId)
            HashMultimap<String, Pair<Long, Long>> groupPartitionMapping = HashMultimap.create();
            for (PartitionGroupRecord record : partitionGroupRecords) {
                String grpKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(record.getPhy_db());
                groupPartitionMapping.put(grpKey, new Pair(record.tg_id, record.id));
            }
            Map<String, String> groupStorageMapping = new ConcurrentHashMap<>();
            if (groupPartitionMapping.size() > 0) {
                GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                groupDetailInfoAccessor.setConnection(metaDbConn);
                //groupName -> storageId
                groupStorageMapping =
                    groupDetailInfoAccessor.getStorageInstIdMappingByDbNameAndGroupName(
                        ServerInstIdManager.getInstance().getMasterInstId(), schemaName,
                        groupPartitionMapping.keySet());
            }

            synchronized (partitionsToDnMapping) {
                for (Map.Entry<String, Pair<Long, Long>> entry : groupPartitionMapping.entries()) {
                    Long tgId = entry.getValue().getKey();
                    Long partitionId = entry.getValue().getValue();
                    String storageId = groupStorageMapping.get(entry.getKey());

                    Map<Long, String> partitionStorageIdMapping =
                        partitionsToDnMapping.computeIfAbsent(tgId, aLong -> new ConcurrentHashMap<>());
                    partitionStorageIdMapping.put(partitionId, storageId);
                }
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }
}
