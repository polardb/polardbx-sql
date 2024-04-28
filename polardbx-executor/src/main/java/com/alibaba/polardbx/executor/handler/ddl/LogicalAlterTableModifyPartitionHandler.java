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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableModifyPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableModifyPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class LogicalAlterTableModifyPartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableModifyPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableModifyPartition logicalAlterTableModifyPartition =
            (LogicalAlterTableModifyPartition) logicalDdlPlan;
        logicalAlterTableModifyPartition.preparedData(executionContext);
        return AlterTableModifyPartitionJobFactory
            .create(logicalAlterTableModifyPartition.relDdl,
                (AlterTableModifyPartitionPreparedData) logicalAlterTableModifyPartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableModifyPartition logicalAlterTableModifyPartition =
            (LogicalAlterTableModifyPartition) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableModifyPartition.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);

        boolean isModifySubpartitions = sqlAlterTableModifyPartitionValues.isSubPartition();

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        String schemaName = logicalAlterTableModifyPartition.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the modify partition command in drds mode");
        }
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        PartitionByDefinition partBy = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartBy = partBy.getSubPartitionBy();
        PartitionByDefinition targetPartBy = isModifySubpartitions ? subPartBy : partBy;
        boolean useSubPartTemp = false;
        if (targetPartBy != null) {
            useSubPartTemp = targetPartBy.isUseSubPartTemplate();
        }

        if (targetPartBy.getStrategy() != PartitionStrategy.LIST
            && targetPartBy.getStrategy() != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("it's not allow to add/drop values for table[%s], which partition strategy is [%s]",
                    logicalTableName,
                    targetPartBy.getStrategy().toString()));
        }

        /**
         * Find the target partSpec to be modified
         * from ast of sqlAlterTableModifyPartitionValues
         */
        SqlPartition partSpec = sqlAlterTableModifyPartitionValues.getPartition();
        SqlIdentifier partAst = null;
        partAst = (SqlIdentifier) partSpec.getName();
        final String partName = partAst == null ? null : SQLUtils.normalizeNoTrim(partAst.getLastName());

        PartitionSpec partitionSpec =
            partitionInfo.getPartSpecSearcher().getPartSpecByPartName(partName == null ? "" : partName);
        PartitionSpec targetSpecToBeModified = null;
        if (!isModifySubpartitions) {
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition[%s] is not exists", partName));
            }
            targetSpecToBeModified = partitionSpec;
        } else {
            boolean needCheckPartName = false;
            if (partName != null && !partName.isEmpty()) {
                if (partitionSpec == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition[%s] is not exists", partName));
                }
                needCheckPartName = true;
            }

            List<SqlNode> subPartSpecs = partSpec.getSubPartitions();
            SqlSubPartition subPartSpec = (SqlSubPartition) subPartSpecs.get(0);
            String subPartName = ((SqlIdentifier) (subPartSpec.getName())).getLastName();
            if (useSubPartTemp) {
                List<PartitionSpec> tarPhySpecs =
                    partitionInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(subPartName);

                /**
                 * the count of the target physical subparts should be the same as the count of parts
                 */
                if (tarPhySpecs.size() != partitionInfo.getPartitionBy().getPartitions().size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("some subpartitions of the subpartition template[%s] are missing", subPartName));
                }

                if (needCheckPartName) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("not allow to specify the partition name[%s] to modify templated subpartition",
                            partName));
                }

                List<PartitionSpec> subPartTemps = subPartBy.getPartitions();
                PartitionSpec targetSubPartTemp = null;
                for (int i = 0; i < subPartTemps.size(); i++) {
                    if (subPartTemps.get(i).getName().equalsIgnoreCase(subPartName)) {
                        targetSubPartTemp = subPartTemps.get(i);
                        break;
                    }
                }
                targetSpecToBeModified = targetSubPartTemp;

            } else {
                PartitionSpec subPartitionSpec = partitionInfo.getPartSpecSearcher().getPartSpecByPartName(subPartName);
                if (subPartitionSpec == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("subpartition[%s] is not exists", subPartName));

                }
                if (needCheckPartName) {
                    PartitionSpec parentPartSpec = partitionInfo.getPartitionBy().getPartitions()
                        .get(subPartitionSpec.getParentPartPosi().intValue() - 1);
                    String targetPartName = parentPartSpec.getName();
                    if (!targetPartName.equalsIgnoreCase(partName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("only allow to modify the subpartitions of the partition[%s] ", partName));
                    }
                }
                targetSpecToBeModified = subPartitionSpec;
            }

        }

        if (sqlAlterTableModifyPartitionValues.isDrop()) {
            /**
             * Validate if the partition values exists
             */
            List<SearchDatumInfo> originalDatums = targetSpecToBeModified.getBoundSpec().getMultiDatums();
            SearchDatumComparator bndValCmp = targetPartBy.getBoundSpaceComparator();
            Set<SearchDatumInfo> bndValSetOfTargetSpec = new TreeSet<>(bndValCmp);
            for (int i = 0; i < originalDatums.size(); i++) {
                bndValSetOfTargetSpec.add(originalDatums.get(i));
            }

//            SqlPartition targetPartAst = sqlAlterTableModifyPartitionValues.getPartition();
//            SqlSubPartition targetSubPartAst = (SqlSubPartition) targetPartAst.getSubPartitions().get(0);
//            SqlPartitionValue toBeDroppedValues = null;
//            if (isModifySubpartitions) {
//                toBeDroppedValues = targetSubPartAst.getValues();
//            } else {
//                toBeDroppedValues = targetPartAst.getValues();
//            }

//            if (originalDatums.size() == 1) {
//                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                    String.format("partition[%s] has only one value now, can't drop value any more", partName));
//            }
//
//            if (originalDatums.size() <= sqlAlterTableModifyPartitionValues.getPartition().getValues().getItems()
//                .size()) {
//                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                    String.format(
//                        "the number of drop values should less than the number of values contain by partition[%s]",
//                        partName));
//            }

            if (tableMeta.withGsi() && !tableMeta.withCci()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("it's not support to drop value when table[%s] with GSI", logicalTableName));
            }
            if (tableMeta.isGsi() && !tableMeta.isColumnar()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_MODIFY_PARTITION_DROP_VALUE,
                    String.format("it's not support to drop value for global index[%s]", logicalTableName));
            }
        }
        return false;
    }

}
