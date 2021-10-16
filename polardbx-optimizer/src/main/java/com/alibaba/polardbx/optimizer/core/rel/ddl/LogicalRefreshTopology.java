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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshDbTopologyPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshTopologyPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalRefreshTopology extends BaseDdlOperation {

    private RefreshTopologyPreparedData preparedData;

    public LogicalRefreshTopology(DDL ddl) {
        super(ddl);
    }

    public static LogicalRefreshTopology create(DDL ddl) {
        return new LogicalRefreshTopology(ddl);
    }

    public void preparedData(
        Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo) {
        assert dbTableGroupAndInstGroupInfo != null;
        preparedData = new RefreshTopologyPreparedData();
        Map<String, RefreshDbTopologyPreparedData> dbPreparedDataMap = new HashMap<>();
        for (Map.Entry<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbEntry : dbTableGroupAndInstGroupInfo
            .entrySet()) {
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
                List<String> newPartitions =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, groupDetailInfoExRecords.size());
                refreshDbTopologyPreparedData.setTableGroupName(tableGroupConfig.getTableGroupRecord().tg_name);
                refreshDbTopologyPreparedData.setNewPartitionNames(newPartitions);
                refreshDbTopologyPreparedData.setTargetGroupDetailInfoExRecords(groupDetailInfoExRecords);
                refreshDbTopologyPreparedData.prepareInvisiblePartitionGroup();
            }
            dbPreparedDataMap.put(dbEntry.getKey(), refreshDbTopologyPreparedData);
        }
        preparedData.setAllRefreshTopologyPreparedData(dbPreparedDataMap);
        preparedData.setDbTableGroupAndInstGroupInfo(dbTableGroupAndInstGroupInfo);
    }

    public RefreshTopologyPreparedData getPreparedData() {
        return preparedData;
    }

}
