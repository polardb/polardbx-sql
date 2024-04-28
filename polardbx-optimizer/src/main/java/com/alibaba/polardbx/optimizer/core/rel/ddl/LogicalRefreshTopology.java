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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshDbTopologyPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshTopologyPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public class LogicalRefreshTopology extends BaseDdlOperation {

    private RefreshTopologyPreparedData preparedData;

    public LogicalRefreshTopology(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public static LogicalRefreshTopology create(DDL ddl) {
        return new LogicalRefreshTopology(ddl);
    }

    public void preparedData(ExecutionContext ec) {
        Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> info = prepareInfo(ec);
        assert info != null;

        preparedData = new RefreshTopologyPreparedData();
        Map<String, RefreshDbTopologyPreparedData> dbPreparedDataMap = new HashMap<>();
        for (Map.Entry<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbEntry :
            info.entrySet()) {
            List<GroupDetailInfoExRecord> groupDetailInfoExRecords = new ArrayList<>();
            Map<String, List<Pair<String, String>>> instGroupDbInfo = dbEntry.getValue().getValue();
            for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
                for (Pair<String, String> pair : entry.getValue()) {
                    GroupDetailInfoExRecord groupDetailInfoExRecord = new GroupDetailInfoExRecord();
                    groupDetailInfoExRecord.storageInstId = entry.getKey();
                    groupDetailInfoExRecord.phyDbName = pair.getValue();
                    groupDetailInfoExRecord.groupName = pair.getKey();
                    groupDetailInfoExRecord.dbName = schemaName;
                    groupDetailInfoExRecords.add(groupDetailInfoExRecord);
                }
            }
            TableGroupConfig tableGroupConfig = dbEntry.getValue().getKey();

            RefreshDbTopologyPreparedData refreshDbTopologyPreparedData = new RefreshDbTopologyPreparedData();
            refreshDbTopologyPreparedData.setSchemaName(dbEntry.getKey());
            refreshDbTopologyPreparedData.setInstGroupDbInfo(instGroupDbInfo);
            if (tableGroupConfig != null && tableGroupConfig.getAllTables().size() > 0) {
                TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();

                String tableName = tableGroupConfig.getTables().get(0);
                TableMeta tableMeta = ec.getSchemaManager(tableGroupRecord.getSchema()).getTable(tableName);

                List<String> partNames = new ArrayList<>();
                List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
                PartitionInfoUtil.getPartitionName(tableMeta.getPartitionInfo(), partNames, subPartNamePairs);

                List<String> newPartitions =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs,
                        groupDetailInfoExRecords.size(),
                        new TreeSet<>(String::compareToIgnoreCase), false);
                refreshDbTopologyPreparedData.setTableGroupName(tableGroupConfig.getTableGroupRecord().tg_name);
                refreshDbTopologyPreparedData.setNewPartitionNames(newPartitions);
                refreshDbTopologyPreparedData.setTargetGroupDetailInfoExRecords(groupDetailInfoExRecords);
                refreshDbTopologyPreparedData.prepareInvisiblePartitionGroup();
            }
            dbPreparedDataMap.put(dbEntry.getKey(), refreshDbTopologyPreparedData);
        }
        preparedData.setAllRefreshTopologyPreparedData(dbPreparedDataMap);
        preparedData.setDbTableGroupAndInstGroupInfo(info);
    }

    private Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> prepareInfo(
        ExecutionContext executionContext) {

        Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo =
            new HashMap<>();

        List<DbInfoRecord> newPartDbInfoRecords =
            DbTopologyManager.getNewPartDbInfoFromMetaDb(executionContext.getSchemaName());
        if (CollectionUtils.isEmpty(newPartDbInfoRecords)) {
            return dbTableGroupAndInstGroupInfo;
        }

        for (DbInfoRecord dbInfoRecord : newPartDbInfoRecords) {
            String schemaName = dbInfoRecord.dbName;
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " not found");
            TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getBroadcastTableGroupConfig();

            // if truely locality here.
            String dbLocality = LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality();
            LocalityDesc dbLocalityDesc = LocalityInfoUtils.parse(dbLocality);
            if (StoragePoolManager.getInstance().isTriggered() && dbLocalityDesc.holdEmptyDnList()) {
                dbLocalityDesc = StoragePoolManager.getInstance().getDefaultLocalityDesc();
            }

            Map<String, List<Pair<String, String>>> instGroupDbInfo =
                DbTopologyManager.generateDbAndGroupNewConfigInfo(schemaName, dbLocalityDesc);
            if (GeneralUtil.isNotEmpty(instGroupDbInfo)) {
                final boolean shareStorageMode =
                    executionContext.getParamManager().getBoolean(ConnectionParams.SHARE_STORAGE_MODE);
                //for local debug
                if (shareStorageMode) {
                    Map<String, List<Pair<String, String>>> copyInstGroupDbInfo = new HashMap<>();
                    for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
                        for (Pair<String, String> pair : entry.getValue()) {
                            copyInstGroupDbInfo.computeIfAbsent(entry.getKey(), o -> new ArrayList<>()).add(Pair.of(
                                GroupInfoUtil.buildGroupNameFromPhysicalDb(pair.getValue() + "S"),
                                pair.getValue() + "S"));
                        }
                    }
                    instGroupDbInfo = copyInstGroupDbInfo;
                }
                dbTableGroupAndInstGroupInfo.put(dbInfoRecord.dbName, new Pair<>(tableGroupConfig, instGroupDbInfo));
            }
        }
        return dbTableGroupAndInstGroupInfo;
    }

    public RefreshTopologyPreparedData getPreparedData() {
        return preparedData;
    }

}
