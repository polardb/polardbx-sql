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
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableModifyPartition extends BaseDdlOperation {

    protected AlterTableGroupModifyPartitionPreparedData preparedData;

    public LogicalAlterTableModifyPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableModifyPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableModifyPartitionValues;
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
        boolean isDropVal = sqlAlterTableModifyPartitionValues.isDrop();
        boolean isModifySubPart = sqlAlterTableModifyPartitionValues.isSubPartition();
        boolean useSubPartTemp = false;
        boolean useSubPart = false;
        if (curPartitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            useSubPart = true;
            useSubPartTemp = curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        preparedData = new AlterTableModifyPartitionPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setModifySubPart(isModifySubPart);
//        preparedData.setOperateOnSubPartition(isModifySubPart);
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
        preparedData.prepareInvisiblePartitionGroup(preparedData.isUseSubPart());

        /**
         * Collect all the partitionNames from the new partition groups
         */
        List<String> newPartitionNames =
            preparedData.getInvisiblePartitionGroups().stream().map(o -> o.getPartition_name())
                .collect(Collectors.toList());

//        List<String> allOldDatedPartNames = new ArrayList<>();
//        allOldDatedPartNames.addAll(oldPartition);
//        if (isDropVal && preparedData.getTempPartitionNames() != null) {
//            allOldDatedPartNames.addAll(preparedData.getTempPartitionNames());
//        }
//        preparedData.setOldPartitionNames(allOldDatedPartNames);

        preparedData.setNewPartitionNames(newPartitionNames);
        //preparedData.getOldPartitionNames().addAll(preparedData.getTempPartitionNames());
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTableName(logicalTableName);
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setTargetImplicitTableGroupName(sqlAlterTable.getTargetImplicitTableGroupName());

        if (preparedData.needFindCandidateTableGroup()) {
            List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
            /**
             * Allocate a mock location with empty phyTbl name and a real phy_db for each new partition group
             *
             * key: part_grp_name( also as part_name)
             * val: a pair of location val
             *      key: phy_tbl_name
             *      val: phy_db_name
             */
            Map<String, Pair<String, String>> mockOrderedTargetTableLocations =
                new TreeMap<>(String::compareToIgnoreCase);
            for (int j = 0; j < newPartitionGroups.size(); j++) {
                String mockTableName = "";
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, new Pair<>(mockTableName,
                    GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(j).phy_db)));
            }

            /**
             *  Assign the physical location for new partition partSpec by using new partiiton groups
             */
            PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    preparedData,
                    curPartitionInfo,
                    false,
                    sqlAlterTableModifyPartitionValues,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    preparedData.getTableGroupName(),
                    null,
                    preparedData.getInvisiblePartitionGroups(),
                    mockOrderedTargetTableLocations,
                    ec);

            int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
            preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
                null, flag, ec);
        }
    }

    public AlterTableGroupModifyPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableModifyPartition create(DDL ddl) {
        return new LogicalAlterTableModifyPartition(ddl);
    }

}
