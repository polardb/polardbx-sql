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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LogicalAlterTableDropPartition extends BaseDdlOperation {

    protected AlterTableGroupDropPartitionPreparedData preparedData;

    public LogicalAlterTableDropPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableDropPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByCci(ExecutionContext ec) {
        String schemaName = this.schemaName;
        String tblName = Util.last(((SqlIdentifier) relDdl.getTableName()).names);

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            TableMeta tblMeta = ec.getSchemaManager(schemaName).getTable(tblName);
            if (tblMeta.isColumnar()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        String logicalTableName = Util.last(((SqlIdentifier) relDdl.getTableName()).names);
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + logicalTableName);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        SqlAlterTableDropPartition sqlAlterTableDropPartition =
            (SqlAlterTableDropPartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        preparedData = new AlterTableDropPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);

        List<String> oldPartitionNames = new ArrayList<>();
        for (SqlNode sqlNode : sqlAlterTableDropPartition.getPartitionNames()) {
            final String origName = ((SqlIdentifier) sqlNode).getLastName().toLowerCase();
            oldPartitionNames.add(origName);
        }

        preparedData.setOldPartitionNames(oldPartitionNames);

        preparedData.setOperateOnSubPartition(sqlAlterTableDropPartition.isSubPartition());

        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTableName(logicalTableName);
        preparedData.setTargetImplicitTableGroupName(sqlAlterTable.getTargetImplicitTableGroupName());
        if (preparedData.needFindCandidateTableGroup()) {
            PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    preparedData,
                    partitionInfo,
                    false,
                    sqlAlterTableDropPartition,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    preparedData.getTableGroupName(),
                    null,
                    preparedData.getInvisiblePartitionGroups(),
                    null,
                    ec);

            int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;

            preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null, null, flag,
                ec);
        }
    }

    public static List<String> getDroppingPartitionNames(SqlAlterTableDropPartition sqlAlterTableDropPartition,
                                                         PartitionInfo partitionInfo,
                                                         AlterTableGroupDropPartitionPreparedData preparedData) {
        List<String> actualPartitionNames = new ArrayList<>();
        Map<String, List<String>> partitionNamesByLevel = new TreeMap<>(String::compareToIgnoreCase);

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        for (SqlNode sqlNode : sqlAlterTableDropPartition.getPartitionNames()) {
            final String origName = ((SqlIdentifier) sqlNode).getLastName().toLowerCase();
            if (subPartByDef != null) {
                if (subPartByDef.isUseSubPartTemplate()) {
                    if (sqlAlterTableDropPartition.isSubPartition()) {
                        // DROP SUBPARTITION
                        // The origName is a subpartition name for templated subpartition.
                        for (PartitionSpec partSpec : partByDef.getPartitions()) {
                            String partName = partSpec.getName().toLowerCase();

                            String actualSubPartName = PartitionNameUtil.autoBuildSubPartitionName(partName, origName);
                            actualPartitionNames.add(actualSubPartName);

                            if (!partitionNamesByLevel.containsKey(partName)) {
                                partitionNamesByLevel.put(partName, new ArrayList<>());
                            }

                            partitionNamesByLevel.get(partName).add(actualSubPartName);
                        }
                    } else {
                        // DROP PARTITION
                        // The origName is a partition name for templated subpartition.
                        validatePartition(partByDef, origName);

                        if (!partitionNamesByLevel.containsKey(origName)) {
                            partitionNamesByLevel.put(origName, new ArrayList<>());
                        }

                        for (PartitionSpec subPartSpec : subPartByDef.getPartitions()) {
                            String subPartName = subPartSpec.getName().toLowerCase();

                            String actualSubPartName =
                                PartitionNameUtil.autoBuildSubPartitionName(origName, subPartName);
                            actualPartitionNames.add(actualSubPartName);

                            partitionNamesByLevel.get(origName).add(actualSubPartName);
                        }
                    }
                } else {
                    if (sqlAlterTableDropPartition.getPartitionName() != null) {
                        // MODIFY PARTITION DROP SUBPARTITION
                        // The origName is an actual subpartition name for non-templated subpartition.
                        actualPartitionNames.add(origName);

                        String partName = sqlAlterTableDropPartition.getPartitionName().toString().toLowerCase();

                        validatePartition(partByDef, partName);

                        if (!partitionNamesByLevel.containsKey(partName)) {
                            partitionNamesByLevel.put(partName, new ArrayList<>());
                        }

                        partitionNamesByLevel.get(partName).add(origName);
                    } else if (sqlAlterTableDropPartition.isSubPartition()) {
                        boolean found = false;

                        for (PartitionSpec partSpec : partByDef.getPartitions()) {
                            for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                                if (subPartSpec.getName().equalsIgnoreCase(origName)) {
                                    actualPartitionNames.add(origName);

                                    String partName = partSpec.getName();

                                    if (!partitionNamesByLevel.containsKey(partName)) {
                                        partitionNamesByLevel.put(partName, new ArrayList<>());
                                    }

                                    partitionNamesByLevel.get(partName).add(origName);

                                    found = true;

                                    break;
                                }
                            }
                            if (found) {
                                break;
                            }
                        }

                        if (!found) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DROP_SUBPARTITION,
                                String.format("Subpartition '%s' doesn't exist", origName));
                        }
                    } else {
                        // DROP PARTITION
                        // The origName is a partition name for non-templated subpartition.
                        PartitionSpec partSpec = validatePartition(partByDef, origName);

                        if (!partitionNamesByLevel.containsKey(origName)) {
                            partitionNamesByLevel.put(origName, new ArrayList<>());
                        }

                        for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                            String subPartName = subPartSpec.getName().toLowerCase();
                            actualPartitionNames.add(subPartName);
                            partitionNamesByLevel.get(origName).add(subPartName);
                        }
                    }
                }
            } else {
                // DROP PARTITION
                // The origName is a partition name.
                actualPartitionNames.add(origName);
                partitionNamesByLevel.put(origName, Collections.EMPTY_LIST);
            }
        }

        if (preparedData != null) {
            preparedData.setOldPartitionNamesByLevel(partitionNamesByLevel);
        }

        return actualPartitionNames;
    }

    protected static PartitionSpec validatePartition(PartitionByDefinition partByDef, String partitionName) {
        PartitionSpec partitionSpec = partByDef.getPartitionByPartName(partitionName);
        if (partitionSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "partition '" + partitionName + "' doesn't exist");
        }
        return partitionSpec;
    }

    public AlterTableGroupDropPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableDropPartition create(DDL ddl) {
        return new LogicalAlterTableDropPartition(ddl);
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getPreparedData()) {
            getPreparedData().setDdlVersionId(ddlVersionId);
        }
    }

}
