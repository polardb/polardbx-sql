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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupModifyPartition;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;

public class LogicalAlterTableGroupModifyPartition extends LogicalAlterTableModifyPartition {

    public LogicalAlterTableGroupModifyPartition(DDL ddl) {
        super(ddl, true);
    }

    /**
     * Unused
     */
//    @Override
//    public void preparedData2(ExecutionContext ec) {
//        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
//        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
//        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupModifyPartition.getAst();
//        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
//            (SqlAlterTableModifyPartitionValues) sqlAlterTableGroup.getAlters().get(0);
//        boolean isSubPart = sqlAlterTableModifyPartitionValues.isSubPartition();
//        boolean isDropVal = sqlAlterTableModifyPartitionValues.isDrop();
//
//        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
//            .getTableGroupConfigByName(tableGroupName);
//        String tableName = tableGroupConfig.getTables().get(0).getLogTbRec().getTableName();
//
//        PartitionInfo partInfo =
//            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).getPartitionInfo();
//        PartitionByDefinition subPartBy = partInfo.getPartitionBy().getSubPartitionBy();
//        boolean useSubPartTemp = false;
//
//        preparedData = new AlterTableGroupModifyPartitionPreparedData();
//        preparedData.setTableGroupName(tableGroupName);
//        preparedData.setSchemaName(schemaName);
//        preparedData.setTableName(tableName);
//        preparedData.setWithHint(targetTablesHintCache != null);
//        preparedData.setModifySubPart(isSubPart);
//        List<String> oldPartition = new ArrayList<>();
//
//        String targetPartName = null;
//        SqlPartition targetPartToBeModified = sqlAlterTableModifyPartitionValues.getPartition();
//        if (!isSubPart) {
//            targetPartName = ((SqlIdentifier) targetPartToBeModified.getName()).getLastName();
//            targetPartName = SQLUtils.normalizeNoTrim(targetPartName);
//            oldPartition.add(targetPartName);
//        } else {
//            SqlSubPartition targetSubPartToBeModified =
//                (SqlSubPartition) targetPartToBeModified.getSubPartitions().get(0);
//            targetPartName = ((SqlIdentifier) targetSubPartToBeModified.getName()).getLastName();
//            if (subPartBy != null) {
//                useSubPartTemp = subPartBy.isUseSubPartTemplate();
//            }
//
//            if (useSubPartTemp) {
//                targetPartName = SQLUtils.normalizeNoTrim(targetPartName);
//                //oldPartition.add(targetPartName);
//
//                String subPartTempName = targetPartName;
//                List<PartitionSpec> phySpecListOfSameSubPartTemp =
//                    partInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(subPartTempName);
//                for (int i = 0; i < phySpecListOfSameSubPartTemp.size(); i++) {
//                    oldPartition.add(phySpecListOfSameSubPartTemp.get(i).getName());
//                }
//            } else {
//                targetPartName = SQLUtils.normalizeNoTrim(targetPartName);
//                oldPartition.add(targetPartName);
//            }
//        }
//
//        preparedData.setOldPartitionNames(oldPartition);
//        preparedData.setDropVal(isDropVal);
//
//        Set<String> phyPartitionNames = new TreeSet<>(String::compareToIgnoreCase);
//        phyPartitionNames.addAll(oldPartition);
//
//        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
//            LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName, oldPartition.get(0),
//                phyPartitionNames, false);
//        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
//        preparedData.prepareInvisiblePartitionGroup();
//        List<String> newPartitionNames =
//            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
//                .collect(Collectors.toList());
//        preparedData.setNewPartitionNames(newPartitionNames);
//        preparedData.setPartBoundExprInfo(sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION));
//        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION);
//
//    }
    @Override
    public void preparedData(ExecutionContext ec) {
        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupModifyPartition.getAst();
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTableGroup.getAlters().get(0);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        String tableName = tableGroupConfig.getTables().get(0).getLogTbRec().getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).getPartitionInfo();

        boolean isDropVal = sqlAlterTableModifyPartitionValues.isDrop();
        boolean isModifySubPart = sqlAlterTableModifyPartitionValues.isSubPartition();
        boolean useSubPartTemp = false;
        boolean useSubPart = false;
        if (curPartitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            useSubPart = true;
            useSubPartTemp = curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        preparedData = new AlterTableGroupModifyPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setModifySubPart(isModifySubPart);
        List<String> oldPartitions = new ArrayList<>();

        SqlPartition targetPartition = null;
        SqlSubPartition targetSubPartition = null;
        SqlIdentifier targetPartitionNameAst = null;
        targetPartition = sqlAlterTableModifyPartitionValues.getPartition();

        /**
         * Find the target (sub)part name from ast(sqlAlterTableModifyPartitionValues)
         */
        String targetPartNameStr = null;
        if (!isModifySubPart) {
            targetPartitionNameAst = (SqlIdentifier) targetPartition.getName();
        } else {
            targetSubPartition = (SqlSubPartition) targetPartition.getSubPartitions().get(0);
            targetPartitionNameAst = (SqlIdentifier) targetSubPartition.getName();

        }
        targetPartNameStr = SQLUtils.normalizeNoTrim(targetPartitionNameAst.getLastName());

        /**
         * Find the target physical part names for templated subpart if need
         */
        List<String> allTargetPhySpecNames = new ArrayList<>();
        List<String> allTargetSpecNames = new ArrayList<>();
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords = new ArrayList<>();
        if (isModifySubPart) {

            if (useSubPartTemp) {
                List<PartitionSpec> phySpecsOfSameSubPartTemp =
                    curPartitionInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(targetPartNameStr);
                for (int i = 0; i < phySpecsOfSameSubPartTemp.size(); i++) {
                    PartitionSpec phySpec = phySpecsOfSameSubPartTemp.get(i);
                    allTargetPhySpecNames.add(phySpec.getName());
                }
            } else {
                allTargetPhySpecNames.add(targetPartNameStr);
            }
            allTargetSpecNames.addAll(allTargetPhySpecNames);

        } else {
            if (useSubPart) {
                PartitionSpec targetPartToBeModified =
                    curPartitionInfo.getPartSpecSearcher().getPartSpecByPartName(targetPartNameStr);
                List<PartitionSpec> phySpecsOfTargetPart = targetPartToBeModified.getSubPartitions();
                for (int i = 0; i < phySpecsOfTargetPart.size(); i++) {
                    allTargetPhySpecNames.add(phySpecsOfTargetPart.get(i).getName());
                }
                allTargetSpecNames.add(targetPartNameStr);
            } else {
                allTargetPhySpecNames.add(targetPartNameStr);
                allTargetSpecNames.add(targetPartNameStr);
            }
        }

        oldPartitions = allTargetPhySpecNames;
        Set<String> phyPartitionNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        phyPartitionNames.addAll(allTargetPhySpecNames);
        targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName,
                isModifySubPart ? "" : oldPartitions.get(0),
                phyPartitionNames, false);

        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setOldPartitionNames(oldPartitions);
        /**
         * Because all the OldPartitionNames have been converted to phy partition name
         */
        preparedData.setOperateOnSubPartition(useSubPart ? true : false);

        /**
         * Prepare new partition group for the partition spec to be modified
         * (including allocate the phy_db value for the new partition group )
         */
        preparedData.setDropVal(isDropVal);
        preparedData.prepareInvisiblePartitionGroup();

        /**
         * Collect all the partitionNames from the new partition groups
         */
        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());

        preparedData.setNewPartitionNames(newPartitionNames);
        preparedData.setSourceSql(sqlAlterTableGroup.getSourceSql());
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION);
        preparedData.setPartBoundExprInfo(sqlAlterTableGroup.getPartRexInfoCtxByLevel().get(PARTITION_LEVEL_PARTITION));
    }

    public static LogicalAlterTableGroupModifyPartition create(DDL ddl) {
        return new LogicalAlterTableGroupModifyPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroupName);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
        return TableGroupNameUtil.isOssTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupModifyPartition alterTableGroupModifyPartition = (AlterTableGroupModifyPartition) relDdl;
        String tableGroupName = alterTableGroupModifyPartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
