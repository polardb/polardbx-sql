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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;

public class AlterTableGroupAddPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupAddPartitionPreparedData() {
    }

    private List<SqlPartition> newPartitions;
    private List<String> targetStorageInstIds;
    private Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel;

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions,
                                 PartitionByDefinition partByDef,
                                 TableGroupConfig tableGroupConfig,
                                 boolean isSubPartition) {
        this.newPartitions = newPartitions;

        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        List<String> newPartitionNames = new ArrayList<>();
        Map<String, String> newPartitionLocalities = new HashMap<>();
        if (subPartByDef != null) {
            // Two-level Partitioned Table
            PartitionStrategy subPartStrategy = subPartByDef.getStrategy();

            if (subPartByDef.isUseSubPartTemplate()) {
                // Templated Subpartition
                if (isSubPartition) {
                    assert newPartitions.size() == 1;
                    SqlPartition sqlPartition = newPartitions.get(0);

                    if (sqlPartition.getName() != null) {
                        // MODIFY PARTITION ADD SUBPARTITION
                        throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_PARTITION,
                            "Don't allow to modify a partition to add new subpartitions for templated subpartition");
                    }

                    // ADD SUBPARTITION
                    for (PartitionSpec partitionSpec : partByDef.getPartitions()) {
                        for (SqlNode subPartition : sqlPartition.getSubPartitions()) {
                            newPartitionNames.add(PartitionNameUtil.autoBuildSubPartitionName(partitionSpec.getName(),
                                ((SqlSubPartition) subPartition).getName().toString()));
                        }
                    }
                } else {
                    // ADD PARTITION
                    List<PartitionSpec> subPartitionSpecs = subPartByDef.getPartitions();
                    for (SqlPartition newPartition : newPartitions) {
                        if (GeneralUtil.isNotEmpty(newPartition.getSubPartitions())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                                "Don't allow to add new subpartitions with newly added partitions for templated subpartition");
                        }

                        SqlNode sqlSubPartCount = newPartition.getSubPartitionCount();
                        if (sqlSubPartCount != null && sqlSubPartCount instanceof SqlNumericLiteral) {
                            int subPartCount = Integer.valueOf(sqlSubPartCount.toString());
                            if (subPartCount != subPartByDef.getPartitions().size()) {
                                throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                                    "Don't allow to specify new number of subpartitions for newly added partitions for templated subpartition");
                            }
                        }

                        for (PartitionSpec subPartitionSpec : subPartitionSpecs) {
                            newPartitionNames.add(
                                PartitionNameUtil.autoBuildSubPartitionName(newPartition.getName().toString(),
                                    subPartitionSpec.getName()));
                        }
                    }
                }
            } else {
                // Non-Templated Subpartition
                if (isSubPartition) {
                    assert newPartitions.size() == 1;
                    SqlPartition sqlPartition = newPartitions.get(0);

                    if (sqlPartition.getName() == null) {
                        // ADD SUBPARTITION
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_SUBPARTITION,
                            "Don't allow to add new subpartitions without specifying any existing partition for non-templated subpartition");
                    }

                    if (subPartStrategy == PartitionStrategy.KEY || subPartStrategy == PartitionStrategy.HASH) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_SUBPARTITION,
                            "Don't allow to modify an existing partition to add new subpartitions for the KEY/HASH strategy");
                    }

                    String partNameModified = sqlPartition.getName().toString();

                    PartitionSpec partSpec = partByDef.getPartitionByPartName(partNameModified);
                    if (partSpec == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition '%s' doesn't exist", partNameModified));
                    }

                    // MODIFY PARTITION ADD SUBPARTITION
                    for (SqlNode subPartition : sqlPartition.getSubPartitions()) {
                        newPartitionNames.add(((SqlSubPartition) subPartition).getName().toString().toLowerCase());
                    }
                } else {
                    // ADD PARTITION
                    for (SqlPartition newPartition : newPartitions) {

                        String partName = newPartition.getName().toString();

                        if (GeneralUtil.isEmpty(newPartition.getSubPartitions())) {
                            if (subPartStrategy == PartitionStrategy.KEY || subPartStrategy == PartitionStrategy.HASH) {

                                SqlNode sqlSubPartCount = newPartition.getSubPartitionCount();

                                if (sqlSubPartCount != null && !(sqlSubPartCount instanceof SqlNumericLiteral)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                                        "Please specify a number of subpartitions for the newly added partition for non-templated subpartition");
                                }

                                int subPartCount = sqlSubPartCount == null ? 1 :
                                    Integer.valueOf(((SqlNumericLiteral) sqlSubPartCount).toValue());

                                for (int i = 1; i <= subPartCount; i++) {
                                    String defaultSubPartName =
                                        PartitionNameUtil.autoBuildSubPartitionTemplateName(Long.valueOf(i));

                                    String actualSubPartName =
                                        PartitionNameUtil.autoBuildSubPartitionName(partName, defaultSubPartName);

                                    SqlSubPartition subPartition =
                                        PartitionInfoUtil.buildDefaultSubPartitionForKey(actualSubPartName);

                                    newPartition.getSubPartitions().add(subPartition);

                                    newPartitionNames.add(actualSubPartName);
                                }
                            } else if (subPartStrategy == PartitionStrategy.RANGE
                                || subPartStrategy == PartitionStrategy.RANGE_COLUMNS) {

                                // Add one default subpartition to new partitions
                                final String defaultSubPartName =
                                    PartitionNameUtil.autoBuildSubPartitionTemplateName(1L);

                                String actualSubPartName =
                                    PartitionNameUtil.autoBuildSubPartitionName(partName, defaultSubPartName);

                                int subPartColCount = subPartByDef.getPartitionFieldList().size();

                                SqlSubPartition subPartition =
                                    PartitionInfoUtil.buildDefaultSubPartitionForRange(actualSubPartName,
                                        subPartColCount);

                                newPartition.getSubPartitions().add(subPartition);

                                newPartitionNames.add(actualSubPartName);
                            } else if (subPartStrategy == PartitionStrategy.LIST
                                || subPartStrategy == PartitionStrategy.LIST_COLUMNS) {

                                // Add one default subpartition to new partitions
                                final String defaultSubPartName =
                                    PartitionNameUtil.autoBuildSubPartitionTemplateName(1L);

                                String actualSubPartName =
                                    PartitionNameUtil.autoBuildSubPartitionName(partName, defaultSubPartName);

                                SqlSubPartition subPartition =
                                    PartitionInfoUtil.buildDefaultSubPartitionForList(actualSubPartName);

                                newPartition.getSubPartitions().add(subPartition);

                                newPartitionNames.add(actualSubPartName);
                            } else {
                                throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                                    "Unsupported subpartition strategy: " + subPartStrategy);
                            }
                            partBoundExprInfoByLevel.put(PARTITION_LEVEL_SUBPARTITION, new HashMap<>());
                        } else {
                            for (SqlNode subPartition : newPartition.getSubPartitions()) {
                                newPartitionNames.add(
                                    ((SqlSubPartition) subPartition).getName().toString().toLowerCase());
                            }
                        }
                    }
                }
            }
        } else {
            // One-level Partitioned Table
            if (isSubPartition) {
                // ADD SUBPARTITION
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_SUBPARTITION,
                    "Don't support ADD SUBPARTITION for one-level partitioned table");
            }
            for (SqlPartition sqlPartition : newPartitions) {
                newPartitionNames.add(sqlPartition.getName().toString().toLowerCase());
                newPartitionLocalities.put(sqlPartition.getName().toString(), sqlPartition.getLocality());
            }
        }

        Set<String> allPartitionNames = new TreeSet<>(String::compareToIgnoreCase);

        Set<String> allPartitionGroupNames =
            tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
                .collect(Collectors.toSet());

        allPartitionNames.addAll(allPartitionGroupNames);

        if (subPartByDef != null) {
            Set<String> partNames = partByDef.getPartitions().stream().map(p -> p.getName())
                .collect(Collectors.toSet());
            allPartitionNames.addAll(partNames);
        }

        for (String partName : newPartitionNames) {
            if (allPartitionNames.contains(partName.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Partition name: " + partName + " already exists. Please use another name.");
            }
        }

        setNewPartitionNames(newPartitionNames);
        setNewPartitionLocalities(newPartitionLocalities);
    }

    public List<String> getTargetStorageInstIds() {
        return targetStorageInstIds;
    }

    public void setTargetStorageInstIds(List<String> targetStorageInstIds) {
        this.targetStorageInstIds = targetStorageInstIds;
    }

    public Map<Integer, Map<SqlNode, RexNode>> getPartBoundExprInfoByLevel() {
        return partBoundExprInfoByLevel;
    }

    public void setPartBoundExprInfoByLevel(Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel) {
        this.partBoundExprInfoByLevel = partBoundExprInfoByLevel;
    }
}
