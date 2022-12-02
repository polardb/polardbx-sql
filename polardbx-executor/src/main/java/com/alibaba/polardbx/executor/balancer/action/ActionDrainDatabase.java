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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Action that move group between storage nodes
 *
 * @author moyi
 * @since 2021/05
 */
public class ActionDrainDatabase implements BalanceAction, Comparable<ActionDrainDatabase> {

    private String schema;
    private String drainNode;
    private String sql;
    private BalanceStats stats;

    public ActionDrainDatabase(String schema, String drainNode, String sql, BalanceStats stats) {
        this.schema = schema;
        this.drainNode = drainNode;
        this.sql = sql;
        this.stats = stats;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return "DrainDatabase";
    }

    @Override
    public String getStep() {
        return sql;
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        long totalRows = 0L;
        long totalSize = 0L;
        try {
            List<DbGroupInfoRecord> groupDetailInfoRecordList = DbTopologyManager.getAllDbGroupInfoRecordByInstId(schema, drainNode);
            List<String> groupNames = groupDetailInfoRecordList.stream().map(e->e.groupName).collect(Collectors.toList());

            if(DbInfoManager.getInstance().isNewPartitionDb(schema)){
                Set<String> drainingPhyDb = new HashSet<>();
                for(DbGroupInfoRecord groupInfo: groupDetailInfoRecordList){
                    drainingPhyDb.add(groupInfo.phyDbName);
                }
                for(PartitionStat partitionStat: stats.getPartitionStats()){
                    String phyDb = partitionStat.getPartitionGroupRecord().getPhy_db();
                    if(drainingPhyDb.contains(phyDb)){
                        totalRows += partitionStat.getPartitionRows();
                        totalSize += partitionStat.getPartitionDiskSize();
                    }
                }
            }else {
                for (GroupStats.GroupsOfStorage groupsOfStorage: GeneralUtil.emptyIfNull(stats.getGroups())){
                    if(groupsOfStorage==null || groupsOfStorage.getGroupDataSizeMap()==null){
                        continue;
                    }
                    for(Map.Entry<String, Pair<Long, Long>> entry: groupsOfStorage.groupDataSizeMap.entrySet()){
                        if(groupNames.contains(entry.getKey())){
                            totalRows += entry.getValue().getKey();
                            totalSize += entry.getValue().getValue();
                        }
                    }
                }
            }
        }catch (Exception e){
            EventLogger.log(EventType.DDL_WARN, "calculate rebalance rows error. " + e.getMessage());
        }

        return ActionUtils.convertToDelegatorJob(schema, sql, CostEstimableDdlTask.createCostInfo(totalRows, totalSize));
    }

    public String getSql() {
        return sql;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionDrainDatabase)) {
            return false;
        }
        ActionDrainDatabase drainDatabase = (ActionDrainDatabase) o;
        return Objects.equals(sql, drainDatabase.getSql()) && Objects
            .equals(schema, drainDatabase.getSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, sql);
    }

    @Override
    public String toString() {
        return "ActionDrainDatabase{" +
            "schemaName=" + schema +
            ", sql=" + sql +
            '}';
    }

    @Override
    public int compareTo(ActionDrainDatabase o) {
        int res = schema.compareTo(o.schema);
        if (res != 0) {
            return res;
        }
        return  o.getSql().compareTo(sql);
    }
}
