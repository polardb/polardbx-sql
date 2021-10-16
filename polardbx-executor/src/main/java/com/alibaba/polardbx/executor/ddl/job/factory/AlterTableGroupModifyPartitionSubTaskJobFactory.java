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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValueItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupModifyPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {
    private final AlterTableGroupModifyPartitionPreparedData parentPreparedData;
    private PartitionSpec tempPartitionInfo;

    public AlterTableGroupModifyPartitionSubTaskJobFactory(DDL ddl,
                                                           AlterTableGroupModifyPartitionPreparedData parentPreparedData,
                                                           AlterTableGroupItemPreparedData preparedData,
                                                           List<PhyDdlTableOperation> phyDdlTableOperations,
                                                           Map<String, List<List<String>>> tableTopology,
                                                           Map<String, Set<String>> targetTableTopology,
                                                           Map<String, Set<String>> sourceTableTopology,
                                                           List<Pair<String, String>> orderedTargetTableLocations,
                                                           String targetPartition,
                                                           boolean skipBackfill,
                                                           ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, executionContext);
        this.parentPreparedData = parentPreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        PartitionInfo partitionInfo = super.generateNewPartitionInfo();
        if (parentPreparedData.isDropVal()) {
            tempPartitionInfo = buildNewTempPartitionSpec(partitionInfo);
            partitionInfo.getPartitionBy().getPartitions().add(tempPartitionInfo);
        }
        return partitionInfo;
    }

    private PartitionSpec buildNewTempPartitionSpec(PartitionInfo partitionInfo) {
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues =
            (SqlAlterTableModifyPartitionValues) ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().get(0).copy();
        partitionSpec.setName(parentPreparedData.getTempPartition());
        PartitionGroupRecord partitionGroupRecord = parentPreparedData.getInvisiblePartitionGroups().stream()
            .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionSpec.getName())).findFirst().orElse(null);

        Pair<String, String> tempPartLocation =
            orderedTargetTableLocations.get(orderedTargetTableLocations.size() - 1);
        partitionSpec.getLocation()
            .setGroupKey(tempPartLocation.getValue());
        partitionSpec.getLocation()
            .setPhyTableName(tempPartLocation.getKey());
        partitionSpec.getLocation().setVisiable(false);
        partitionSpec.setPosition(Long.valueOf(partitionInfo.getPartitionBy().getPartitions().size() + 1));

        assert partitionGroupRecord != null;
        SearchDatumComparator cmp = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        List<SearchDatumInfo> partBoundValues = new ArrayList<>();
        SqlPartition targetModifyPart = sqlAlterTableModifyPartitionValues.getPartition();
        List<SqlPartitionValueItem> itemsOfVals = targetModifyPart.getValues().getItems();
        Map<SqlNode, RexNode> allRexExprInfo =
            ((SqlAlterTableGroup) sqlAlterTableModifyPartitionValues.getParent()).getPartRexInfoCtx();
        boolean isMultiCols = partitionInfo.getPartitionBy().getPartitionColumnNameList().size() > 1;
        for (int i = 0; i < itemsOfVals.size(); i++) {
            SqlNode oneItem = itemsOfVals.get(i).getValue();
            RexNode bndRexValsOfOneItem = allRexExprInfo.get(oneItem);

            List<PartitionBoundVal> oneColsVal = new ArrayList<>();
            if (isMultiCols) {

                List<RexNode> opList = ((RexCall) bndRexValsOfOneItem).getOperands();
                for (int j = 0; j < opList.size(); j++) {
                    RexNode oneBndExpr = opList.get(j);

                    RelDataType bndValDt = cmp.getDatumRelDataTypes()[j];
                    PartitionBoundVal bndVal =
                        PartitionPrunerUtils.getBoundValByRexExpr(oneBndExpr, bndValDt, PartFieldAccessType.DDL_EXECUTION, executionContext);
                    oneColsVal.add(bndVal);
                }
            } else {

                // oneItem is a SqlLiteral or a SqlCall With func,
                // So bndRexValsOfOneItem is a RexCall or RexLiteral
                RelDataType bndValDt = cmp.getDatumRelDataTypes()[0];
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(bndRexValsOfOneItem, bndValDt, PartFieldAccessType.DDL_EXECUTION, executionContext);
                oneColsVal.add(bndVal);
            }
            SearchDatumInfo searchDatumInfo = new SearchDatumInfo(oneColsVal);
            partBoundValues.add(searchDatumInfo);
        }
        PartitionBoundSpec boundSpec = PartitionInfoBuilder
            .createPartitionBoundSpec(partitionInfo.getPartitionBy().getStrategy(), targetModifyPart.getValues(),
                partBoundValues);
        partitionSpec.setBoundSpec(boundSpec);
        return partitionSpec;
    }

    public PartitionSpec getTempPartitionInfo() {
        return tempPartitionInfo;
    }
}
