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

package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableGroupItemBuilder extends DdlPhyPlanBuilder {

    protected final AlterTableGroupItemPreparedData preparedData;
    protected final ExecutionContext executionContext;
    protected Map<String, Set<String>> sourcePhyTables = new LinkedHashMap<>();
    protected Map<String, Set<String>> targetPhyTables = new LinkedHashMap<>();
    /**
     * orderedTargetTableLocations is used to store all the partition's locations of target new added phy tables
     * <p>
     * item is format as <partitionName, <phyTbl, grpKey>>
     */
    protected Map<String, Pair<String, String>> orderedTargetTableLocations =
        new TreeMap<>(String::compareToIgnoreCase);

    public AlterTableGroupItemBuilder(DDL ddl,
                                      AlterTableGroupItemPreparedData preparedData,
                                      ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    public AlterTableGroupItemBuilder build() {
        buildNewTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        // not build physical plans for cci
        if (!preparedData.isColumnarIndex()) {
            buildPhysicalPlans();
        }
        this.built = true;
        return this;
    }

    @Override
    protected void buildTableRuleAndTopology() {
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    public Map<String, Set<String>> getSourcePhyTables() {
        if (GeneralUtil.isEmpty(sourcePhyTables)) {
            PartitionInfo partitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                    .getPartitionInfo(preparedData.getTableName());
            PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
            for (String oldPartitionName : preparedData.getOldPartitionNames()) {
                boolean found = false;
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    if (partitionInfo.getPartitionBy().getSubPartitionBy() != null &&
                        GeneralUtil.isNotEmpty(partitionSpec.getSubPartitions())
                        && preparedData.isOperateOnSubPartition()) {
                        for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                            if (subPartitionSpec.getName().equalsIgnoreCase(oldPartitionName) || (subPartBy != null
                                && subPartBy.isUseSubPartTemplate() && subPartitionSpec.getTemplateName()
                                .equalsIgnoreCase(oldPartitionName))) {
                                PartitionLocation location = subPartitionSpec.getLocation();
                                sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                                    .add(location.getPhyTableName());
                                found = true;
                                break;
                            }
                        }
                    }

                    if (!found && partitionSpec.getName().equalsIgnoreCase(oldPartitionName)) {
                        if (partitionSpec.isLogical()) {
                            for (PartitionSpec subPart : partitionSpec.getSubPartitions()) {
                                PartitionLocation location = subPart.getLocation();
                                sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                                    .add(location.getPhyTableName());
                            }
                        } else {
                            PartitionLocation location = partitionSpec.getLocation();
                            sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                                .add(location.getPhyTableName());
                        }
                        found = true;
                    }
                    if (found && !(subPartBy != null && subPartBy.isUseSubPartTemplate())) {
                        break;
                    }
                }
            }
        }
        return sourcePhyTables;
    }

    public Map<String, Set<String>> getTargetPhyTables() {
        return targetPhyTables;
    }

    public Map<String, Pair<String, String>> getOrderedTargetTableLocations() {
        return orderedTargetTableLocations;
    }

    @Override
    protected void buildNewTableTopology(String schemaName, String tableName) {
        tableTopology = new HashMap<>();
        List<PartitionGroupRecord> invisiblePartitionGroups = preparedData.getInvisiblePartitionGroups();
        int i = 0;
        for (String newPhyTableName : preparedData.getNewPhyTables()) {
            PartitionGroupRecord partitionGroupRecord = invisiblePartitionGroups.get(i++);
            //TODO need review by taokun
            String groupName = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.getPhy_db());
            List<String> phyTables = new ArrayList<>();
            phyTables.add(newPhyTableName);
            tableTopology.computeIfAbsent(groupName, o -> new ArrayList<>())
                .add(phyTables);
            targetPhyTables.computeIfAbsent(groupName, o -> new HashSet<>())
                .add(newPhyTableName);
            orderedTargetTableLocations.put(partitionGroupRecord.partition_name, Pair.of(newPhyTableName, groupName));
            buildAlterPartitionReferenceTableTopology(schemaName, tableName);
        }
    }

    @Override
    protected void buildSqlTemplate() {
        PartitionLocation location = preparedData.getDefaultPartitionSpec().getLocation();
        String createTableStr = AlterTableGroupUtils
            .fetchCreateTableDefinition(relDdl, executionContext, location.getGroupKey(), location.getPhyTableName(),
                preparedData.getSchemaName());
        sqlTemplate = AlterTableGroupUtils.getSqlTemplate(preparedData.getSchemaName(), preparedData.getTableName(),
            createTableStr, executionContext);
    }

    public void buildAlterPartitionReferenceTableTopology(String schemaName, String tableName) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        for (Map.Entry<String, ForeignKeyData> fk : tableMeta.getForeignKeys().entrySet()) {
            ForeignKeyData data = fk.getValue();
            if (!data.isPushDown()) {
                continue;
            }
            final PartitionInfo refPartInfo =
                OptimizerContext.getContext(data.refSchema).getPartitionInfoManager()
                    .getPartitionInfo(data.refTableName);
            final Map<String, List<List<String>>> refTopo =
                PartitionInfoUtil.buildTargetTablesFromPartitionInfo(refPartInfo);

            // push down must be single or broadcast, so only one db
            Map.Entry<String, List<List<String>>> refTable = new ArrayList<>(refTopo.entrySet()).get(0);
            Map.Entry<String, List<List<String>>> table = new ArrayList<>(tableTopology.entrySet()).get(0);
            final List<List<String>> part = refTopo.remove(refTable.getKey());
            refTopo.put(table.getKey(), part);

            if (refPartInfo.isBroadcastTable()) {
                final String phyTable =
                    refTopo.values().stream().map(l -> l.get(0).get(0)).findFirst().orElse(null);
                assert phyTable != null;
                for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
                    for (List<String> l : entry.getValue()) {
                        l.add(phyTable);
                    }
                }
            } else {
                for (Map.Entry<String, List<List<String>>> entry : refTopo.entrySet()) {
                    final List<List<String>> partList = tableTopology.get(entry.getKey());
                    assert partList != null;
                    assert partList.size() == entry.getValue().size();
                    for (int i = 0; i < entry.getValue().size(); ++i) {
                        partList.get(i).addAll(entry.getValue().get(i));
                    }
                }
            }
        }
    }
}
