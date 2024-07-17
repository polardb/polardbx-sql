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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.gms.util.TableMetaUtil;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.gms.TddlRuleGmsConfig;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper.genHashCodeForPhyTableDDL;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils.buildPhyDbTableNameFromGroupNameAndPhyTableName;

public class DdlJobDataConverter {

    /**
     * NOTE: In most case you should ues DdlPhyPlanBuilder.genPhysicalPlan instead
     */
    public static PhysicalPlanData convertToPhysicalPlanData(Map<String, List<List<String>>> tableTopology,
                                                             List<PhyDdlTableOperation> physicalPlans,
                                                             ExecutionContext ec) {
        return convertToPhysicalPlanData(tableTopology, physicalPlans, false, false, ec);
    }

    public static PhysicalPlanData convertToPhysicalPlanData(Map<String, List<List<String>>> tableTopology,
                                                             List<PhyDdlTableOperation> physicalPlans,
                                                             boolean isGsi, boolean isAutoPartition,
                                                             ExecutionContext ec) {
        return convertToPhysicalPlanData(tableTopology, physicalPlans, isGsi, isAutoPartition, false, false, ec);
    }

    /**
     * NOTE: In most case you should ues DdlPhyPlanBuilder.genPhysicalPlan instead
     * TODO Is the parameter isGsi necessary ?
     */
    public static PhysicalPlanData convertToPhysicalPlanData(Map<String, List<List<String>>> tableTopology,
                                                             List<PhyDdlTableOperation> physicalPlans,
                                                             boolean isGsi, boolean isAutoPartition, boolean isOSS,
                                                             boolean pushDownFk, ExecutionContext ec) {
        PhysicalPlanData data = new PhysicalPlanData();

        PhyDdlTableOperation physicalPlan = physicalPlans.get(0);

        String schemaName = physicalPlan.getSchemaName();

        data.setSchemaName(schemaName);
        data.setLogicalTableName(physicalPlan.getLogicalTableName());
        data.setNewLogicalTableName(physicalPlan.getNewLogicalTableName());

        data.setDefaultDbIndex(physicalPlan.getDbIndex());
        if (!pushDownFk) {
            data.setDefaultPhyTableName(Util.last(Util.last(physicalPlan.getTableNames())));
        } else {
            data.setDefaultPhyTableName(Util.last(physicalPlan.getTableNames()).get(0));
        }

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
            if (partitionInfo != null || isOSS) {
                TableGroupDetailConfig tableGroupConfig = buildTableGroupConfig(partitionInfo, isOSS);
                data.setTableGroupConfig(tableGroupConfig);
                Map<String, List<PhysicalPartitionInfo>> physicalPartitionTopology =
                    physicalPlan.getPartitionInfo().getPhysicalPartitionTopology(null, false);
                data.setPhysicalPartitionTopology(physicalPartitionTopology);
                data.setLocalityDesc(LocalityInfoUtils.parse(partitionInfo.getLocality()));
            }
        }
        data.setTableTopology(tableTopology);

        data.setKind(physicalPlan.getKind());

        data.setSqlTemplate(physicalPlan.getBytesSql().toString(null));

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
        return convertToPhysicalPlans(data, executionContext, new HashSet<>());
    }

    public static Set<String> getPhysicalDoneTables(PhysicalPlanData data, ExecutionContext executionContext,
                                                    Map<String, String> hashCodeForDdlBefore) {
        Set<String> physicalDoneTables = new HashSet<>();
        for (Map.Entry<String, List<List<String>>> topology : data.getTableTopology().entrySet()) {
            String groupName = topology.getKey();
            for (List<String> phyTablesNames : topology.getValue()) {
                for (String phyTableName : phyTablesNames) {
                    String hashCodeForDdl = genHashCodeForPhyTableDDL(data.getSchemaName(), groupName,
                        SqlIdentifier.surroundWithBacktick(phyTableName), 0);
                    String fullPhyTableName = buildPhyDbTableNameFromGroupNameAndPhyTableName(groupName, phyTableName);
                    if (!hashCodeForDdlBefore.get(fullPhyTableName).equals(hashCodeForDdl)) {
                        physicalDoneTables.add(fullPhyTableName);
                    }
                }
            }
        }
        return physicalDoneTables;
    }

    public static List<RelNode> convertToPhysicalPlans(PhysicalPlanData data, ExecutionContext executionContext,
                                                       Set<String> donePhysicalTables) {
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

                List<String> fullPhyTableNames = phyTableNames.stream()
                    .map(o -> buildPhyDbTableNameFromGroupNameAndPhyTableName(groupName, o))
                    .collect(Collectors.toList());
                if (donePhysicalTables.containsAll(fullPhyTableNames)) {
                    index++;
                    continue;
                }
                phyDdlTableOperation.setTableNames(ImmutableList.of(phyTableNames));

                phyDdlTableOperation.setKind(data.getKind());

                phyDdlTableOperation.setBytesSql(BytesSql.getBytesSql(data.getSqlTemplate()));
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

    public static TableGroupDetailConfig buildTableGroupConfig(PartitionInfo partitionInfo, boolean isOSS) {
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(partitionInfo);
        List<TablePartitionRecord> partRecList = PartitionInfoUtil.prepareRecordForAllPartitions(partitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, partitionInfo,
                partitionInfo.getPartitionBy().getPartitions());
        TableGroupRecord tableGroupRecord = null;
        List<PartitionGroupRecord> partitionGroupRecords = null;

        // need to create a new table group and related partition groups
        if (partitionInfo.getTableGroupId() < 0 || isOSS) {
            // TODO(moyi) auto flag?
            tableGroupRecord = PartitionInfoUtil.prepareRecordForTableGroup(partitionInfo, isOSS);
        }

        partitionGroupRecords =
            PartitionInfoUtil.prepareRecordForPartitionGroups(partitionInfo.getPartitionBy().getPhysicalPartitions());
        partitionGroupRecords.forEach(o -> o.tg_id = partitionInfo.getTableGroupId());

        TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
        tablePartRecordInfoContext.setLogTbRec(logTableRec);
        tablePartRecordInfoContext.setPartitionRecList(partRecList);
        tablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
        tablePartRecordInfoContext.setSubPartitionRecList(
            TablePartRecordInfoContext.buildAllSubPartitionRecList(subPartRecInfos));
        List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
        tablePartRecordInfoContexts.add(tablePartRecordInfoContext);

        TableGroupDetailConfig tableGroupConfig =
            new TableGroupDetailConfig(tableGroupRecord, partitionGroupRecords, tablePartRecordInfoContexts,
                partitionInfo.getLocality());
        return tableGroupConfig;
    }
}
