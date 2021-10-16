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

package com.alibaba.polardbx.executor.ddl.job.converter;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.gms.util.TableMetaUtil;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.gms.TddlRuleGmsConfig;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DdlJobDataConverter {

    /**
     * NOTE: In most case you should ues DdlPhyPlanBuilder.genPhysicalPlan instead
     */
    public static PhysicalPlanData convertToPhysicalPlanData(Map<String, List<List<String>>> tableTopology,
                                                             List<PhyDdlTableOperation> physicalPlans) {
        return convertToPhysicalPlanData(tableTopology, physicalPlans, false, false);
    }

    /**
     * NOTE: In most case you should ues DdlPhyPlanBuilder.genPhysicalPlan instead
     * TODO Is the parameter isGsi necessary ?
     */
    public static PhysicalPlanData convertToPhysicalPlanData(Map<String, List<List<String>>> tableTopology,
                                                             List<PhyDdlTableOperation> physicalPlans,
                                                             boolean isGsi, boolean isAutoPartition) {
        PhysicalPlanData data = new PhysicalPlanData();

        PhyDdlTableOperation physicalPlan = physicalPlans.get(0);

        String schemaName = physicalPlan.getSchemaName();

        data.setSchemaName(schemaName);
        data.setLogicalTableName(physicalPlan.getLogicalTableName());
        data.setNewLogicalTableName(physicalPlan.getNewLogicalTableName());

        data.setDefaultDbIndex(physicalPlan.getDbIndex());
        data.setDefaultPhyTableName(Util.last(Util.last(physicalPlan.getTableNames())));

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPartDb) {
            TablesExtRecord tablesExtRecord = TableMetaUtil.convertToTablesExtRecord(physicalPlan.getTableRule(),
                schemaName, physicalPlan.getLogicalTableName(), false, isAutoPartition);

            if (physicalPlan.getNewLogicalTableName() != null && !physicalPlan.getNewLogicalTableName().isEmpty()) {
                tablesExtRecord.newTableName = physicalPlan.getNewLogicalTableName();
            }

            data.setTablesExtRecord(tablesExtRecord);
        } else {
            PartitionInfo partitionInfo = physicalPlan.getPartitionInfo();
            if (partitionInfo != null) {
                TableGroupConfig tableGroupConfig = buildTableGroupConfig(partitionInfo);
                data.setTableGroupConfig(tableGroupConfig);
            }
        }
        data.setTableTopology(tableTopology);

        data.setKind(physicalPlan.getKind());

        data.setSqlTemplate(physicalPlan.getNativeSql());

        List<Map<Integer, ParameterContext>> paramsList = new ArrayList<>();
        for (PhyDdlTableOperation phyDdlTableOperation : physicalPlans) {
            paramsList.add(phyDdlTableOperation.getParam());
        }
        data.setParamsList(paramsList);

        data.setExplain(physicalPlan.isExplain());
        data.setPartitioned(physicalPlan.isPartitioned());
        data.setWithHint(physicalPlan.isHint());

        if (physicalPlan.getNativeSqlNode() instanceof SqlCreateTable) {
            data.setIfNotExists(physicalPlan.isIfNotExists());
        } else if (physicalPlan.getNativeSqlNode() instanceof SqlDropTable) {
            data.setIfExists(((SqlDropTable) physicalPlan.getNativeSqlNode()).isIfExists());
        }

        if (physicalPlan.getNativeSqlNode() instanceof SqlCreateTable) {
            data.setTemporary(((SqlCreateTable) physicalPlan.getNativeSqlNode()).isTemporary());
        }

        data.setSequence(physicalPlan.getSequence());

        if (physicalPlan.getNativeSqlNode() instanceof SqlCreateTable) {
            data.setCreateTablePhysicalSql(((SqlCreateTable) physicalPlan.getNativeSqlNode()).getSourceSql());
        }

        return data;
    }

    public static List<RelNode> convertToPhysicalPlans(PhysicalPlanData data, ExecutionContext executionContext) {
        List<RelNode> physicalPlans = new ArrayList<>();

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(data.getSchemaName());
        TableRule tableRule = isNewPartDb ? null : buildTableRule(data.getTablesExtRecord());

        int index = 0;
        for (Map.Entry<String, List<List<String>>> topology : data.getTableTopology().entrySet()) {
            String groupName = topology.getKey();
            for (List<String> phyTableNames : topology.getValue()) {
                PhyDdlTableOperation phyDdlTableOperation =
                    PhyDdlTableOperation.create(data.getSchemaName(), data.getLogicalTableName(), executionContext);

                phyDdlTableOperation.setSchemaName(data.getSchemaName());
                phyDdlTableOperation.setDbIndex(groupName);

                phyDdlTableOperation.setLogicalTableName(data.getLogicalTableName());
                if (data.getNewLogicalTableName() != null) {
                    phyDdlTableOperation.setRenameLogicalTableName(data.getNewLogicalTableName());
                }

                phyDdlTableOperation.setTableNames(ImmutableList.of(phyTableNames));

                phyDdlTableOperation.setKind(data.getKind());

                phyDdlTableOperation.setSqlTemplate(data.getSqlTemplate());
                phyDdlTableOperation.setParam(data.getParamsList().get(index++));

                phyDdlTableOperation.setNativeSqlNode(null);

                phyDdlTableOperation.setDbType(DbType.MYSQL);

                phyDdlTableOperation.setTableRule(tableRule);

                phyDdlTableOperation.setExplain(data.isExplain());
                phyDdlTableOperation.setPartitioned(data.isPartitioned());
                phyDdlTableOperation.setSequence(data.getSequence());
                phyDdlTableOperation.setIfNotExists(data.isIfNotExists());

                physicalPlans.add(phyDdlTableOperation);
            }
        }

        return physicalPlans;
    }

    public static TableRule buildTableRule(TablesExtRecord tablesExtRecord) {
        TddlRuleGmsConfig tddlRuleGmsConfig = new TddlRuleGmsConfig();
        return tddlRuleGmsConfig.initTableRule(tablesExtRecord);
    }

    public static TableGroupConfig buildTableGroupConfig(PartitionInfo partitionInfo) {
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(partitionInfo);
        List<TablePartitionRecord> partRecList = PartitionInfoUtil.prepareRecordForAllPartitions(partitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, partitionInfo,
                partitionInfo.getPartitionBy().getPartitions());
        TableGroupRecord tableGroupRecord = null;
        List<PartitionGroupRecord> partitionGroupRecords = null;

        // need to create a new table group and related partition groups
        if (partitionInfo.getTableGroupId() < 0) {
            // TODO(moyi) auto flag?
            tableGroupRecord = PartitionInfoUtil.prepareRecordForTableGroup(partitionInfo);
        }

        partitionGroupRecords =
            PartitionInfoUtil.prepareRecordForPartitionGroups(partitionInfo.getPartitionBy().getPartitions());

        TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
        tablePartRecordInfoContext.setLogTbRec(logTableRec);
        tablePartRecordInfoContext.setPartitionRecList(partRecList);
        tablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
        List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
        tablePartRecordInfoContexts.add(tablePartRecordInfoContext);

        TableGroupConfig tableGroupConfig = new TableGroupConfig();
        tableGroupConfig.setTableGroupRecord(tableGroupRecord);
        tableGroupConfig.setPartitionGroupRecords(partitionGroupRecords);
        tableGroupConfig.setTables(tablePartRecordInfoContexts);
        return tableGroupConfig;
    }
}
