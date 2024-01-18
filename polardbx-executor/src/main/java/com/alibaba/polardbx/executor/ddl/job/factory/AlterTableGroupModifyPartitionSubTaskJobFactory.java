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

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class AlterTableGroupModifyPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {
    protected final AlterTableGroupModifyPartitionPreparedData parentPreparedData;
    protected List<PartitionSpec> tempPartitionSpecs;

    public AlterTableGroupModifyPartitionSubTaskJobFactory(DDL ddl,
                                                           AlterTableGroupModifyPartitionPreparedData parentPreparedData,
                                                           AlterTableGroupItemPreparedData preparedData,
                                                           List<PhyDdlTableOperation> phyDdlTableOperations,
                                                           Map<String, List<List<String>>> tableTopology,
                                                           Map<String, Set<String>> targetTableTopology,
                                                           Map<String, Set<String>> sourceTableTopology,
                                                           Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                           String targetPartition,
                                                           boolean skipBackfill,
                                                           ExecutionContext executionContext) {
        super(ddl, parentPreparedData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology, orderedTargetTableLocations, targetPartition, skipBackfill,
            ComplexTaskMetaManager.ComplexTaskType.MODIFY_PARTITION, executionContext);
        this.parentPreparedData = parentPreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        PartitionInfo newPartInfo = super.generateNewPartitionInfo();
        if (parentPreparedData.isDropVal()) {
            tempPartitionSpecs = buildAndAppendNewTempPartitionSpec(newPartInfo);
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;
    }

    protected SqlAlterTableModifyPartitionValues getSqlNode() {
        return (SqlAlterTableModifyPartitionValues) ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
    }

    /**
     * Build the tmp partition spec which contain all values to be dropped
     *
     * <pre>
     * For list partition drop values , eg.
     *      list p1: (v1,v2,v3)
     *      modify partition drop values (v2)
     * its drop values process is combined as two steps::
     *  ==>
     *    1. split p1 to  p1(v1,v3) and p1'(v2)
     *    2. drop partition p1'
     * , so the p1' is the newTempPartitionSpec that contain all the values to be dropped
     * </pre>
     */
    protected List<PartitionSpec> buildAndAppendNewTempPartitionSpec(PartitionInfo newPartInfo) {
        SqlAlterTableModifyPartitionValues sqlAlterTableModifyPartitionValues = getSqlNode();
        boolean isModifySubPart = sqlAlterTableModifyPartitionValues.isSubPartition();
        boolean useSubPartTemp = false;
        boolean useSubPart = false;

        if (newPartInfo.getPartitionBy().getSubPartitionBy() != null) {
            useSubPart = true;
            useSubPartTemp = newPartInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        /**
         * Find the list values ast from sqlAlterTableModifyPartitionValues
         */
        SqlIdentifier targetMoidifyNameAst = null;
        SqlPartitionValue targetModifyPartValsAst = null;
        SqlPartition targetModifyPart = sqlAlterTableModifyPartitionValues.getPartition();
        SqlSubPartition targetModifySubPart = null;
        if (!isModifySubPart) {
            targetMoidifyNameAst = (SqlIdentifier) targetModifyPart.getName();
            targetModifyPartValsAst = targetModifyPart.getValues();
        } else {
            targetModifySubPart = (SqlSubPartition) targetModifyPart.getSubPartitions().get(0);
            targetMoidifyNameAst = (SqlIdentifier) targetModifySubPart.getName();
            targetModifyPartValsAst = targetModifySubPart.getValues();
        }
        String targetPartNameStr = SQLUtils.normalizeNoTrim(targetMoidifyNameAst.getLastName());

        /**
         * Copy a new spec from already exist part spec
         */
        PartitionByDefinition targetPartByDef = newPartInfo.getPartitionBy();
        PartitionSpec parentPartSpec = null;
        PartitionSpec newTmpPartitionSpec = null;
        List<PartitionSpec> allPartSpecsWithSameParent = new ArrayList<>();
        List<PartitionSpec> allPhySpecs = newPartInfo.getPartitionBy().getPhysicalPartitions();
        if (isModifySubPart) {
            targetPartByDef = newPartInfo.getPartitionBy().getSubPartitionBy();
            if (useSubPartTemp) {

                /**
                 * For modification of templated subpart , use the subpart templates
                 * to build the temp part that contain all the dropping values
                 */
                List<PartitionSpec> allPartSpecTemps = newPartInfo.getPartitionBy().getSubPartitionBy().getPartitions();
                PartitionSpec targetPartSubSpecTempToBeDropValues = null;
                for (int i = 0; i < allPartSpecTemps.size(); i++) {
                    PartitionSpec partSpecTemp = allPartSpecTemps.get(i);
                    if (partSpecTemp.getName().equalsIgnoreCase(targetPartNameStr)) {
                        targetPartSubSpecTempToBeDropValues = partSpecTemp;
                        break;
                    }
                }

                allPartSpecsWithSameParent = allPartSpecTemps;
                newTmpPartitionSpec = targetPartSubSpecTempToBeDropValues.copy();

            } else {
                PartitionSpec targetPartSubSpecDroppedValues =
                    newPartInfo.getPartSpecSearcher().getPartSpecByPartName(targetPartNameStr);
                int parentPosi = targetPartSubSpecDroppedValues.getParentPartPosi().intValue();
                parentPartSpec = newPartInfo.getPartitionBy().getPartitions().get(parentPosi - 1);
                allPartSpecsWithSameParent = parentPartSpec.getSubPartitions();
                newTmpPartitionSpec = targetPartSubSpecDroppedValues.copy();
            }
        } else {

            PartitionSpec targetPartSpecToBeDropVals =
                newPartInfo.getPartSpecSearcher().getPartSpecByPartName(targetPartNameStr);
            newTmpPartitionSpec = targetPartSpecToBeDropVals.copy();
            allPartSpecsWithSameParent = newPartInfo.getPartitionBy().getPartitions();
            parentPartSpec = null;

        }

        /**
         * Init the bound spec of those values to be dropped
         */
        SearchDatumComparator cmp = targetPartByDef.getBoundSpaceComparator();
        List<SearchDatumInfo> partBoundValues = new ArrayList<>();
        List<SqlPartitionValueItem> itemsOfVals = targetModifyPartValsAst.getItems();
        Map<SqlNode, RexNode> allRexExprInfo = parentPreparedData.getPartBoundExprInfo();
        boolean isMultiCols = targetPartByDef.getPartitionColumnNameList().size() > 1;
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
                        PartitionPrunerUtils.getBoundValByRexExpr(oneBndExpr, bndValDt,
                            PartFieldAccessType.DDL_EXECUTION, executionContext);
                    oneColsVal.add(bndVal);
                }
            } else {

                // oneItem is a SqlLiteral or a SqlCall With func,
                // So bndRexValsOfOneItem is a RexCall or RexLiteral
                RelDataType bndValDt = cmp.getDatumRelDataTypes()[0];
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(bndRexValsOfOneItem, bndValDt,
                        PartFieldAccessType.DDL_EXECUTION, executionContext);
                oneColsVal.add(bndVal);
            }
            SearchDatumInfo searchDatumInfo = new SearchDatumInfo(oneColsVal);
            partBoundValues.add(searchDatumInfo);
        }
        PartitionBoundSpec boundSpec = PartitionInfoBuilder
            .createPartitionBoundSpec(targetPartByDef.getStrategy(), targetModifyPart.getValues(),
                partBoundValues);

        List<PartitionSpec> targetNewTmpPartSpecThatContainAllDropVals = new ArrayList<>();
        if (isModifySubPart) {
            if (useSubPartTemp) {

                /**
                 * Build new temp subpartSpec for each first-level partition
                 */
                Set<String> newTmpSubpartSpecTempNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                int newTmpSpecCount = parentPreparedData.getTempPartitionNames().size();
                for (int i = 0; i < newTmpSpecCount; i++) {
                    final String newSpecName = parentPreparedData.getTempPartitionNames().get(i);
                    final String newSpecParentName = parentPreparedData.getParentPartitionNames().get(i);
                    final String newSpecSubPartTempName = parentPreparedData.getSubPartTempNames().get(i);
                    PartitionSpec parentSpec =
                        newPartInfo.getPartSpecSearcher().getPartSpecByPartName(newSpecParentName);
                    List<PartitionSpec> subPartSpecs = parentSpec.getSubPartitions();

                    PartitionSpec newCopySpec = subPartSpecs.get(0).copy();
                    PartitionGroupRecord newSpecPgRec = parentPreparedData.getInvisiblePartitionGroups().stream()
                        .filter(o -> o.getPartition_name().equalsIgnoreCase(newSpecName)).findFirst().orElse(null);
                    Pair<String, String> newSpecLocation = orderedTargetTableLocations.get(newSpecName);
                    Long newSpecPosi = Long.valueOf(subPartSpecs.size() + 1);
                    Long newSpecParentPosi = parentSpec.getPosition();
                    Long newSpecPhyPosi = Long.valueOf(allPhySpecs.size() + i + 1);
                    PartitionBoundSpec newSpecBoundVal = boundSpec;
                    newTmpPartitionSpec = initTempPartSpec(newCopySpec,
                        newSpecName, newSpecSubPartTempName, newSpecPgRec, newSpecLocation, newSpecPosi,
                        newSpecParentPosi, newSpecPhyPosi,
                        newSpecBoundVal);
                    subPartSpecs.add(newTmpPartitionSpec);
                    targetNewTmpPartSpecThatContainAllDropVals.add(newTmpPartitionSpec);

                    /**
                     * Build new tmp subpartSpec for subpartition template
                     */
                    if (!newTmpSubpartSpecTempNameSet.contains(newSpecSubPartTempName)) {
                        PartitionSpec newCopySpecTemp = newTmpPartitionSpec.copy();
                        newCopySpecTemp.setName(newSpecSubPartTempName);
                        newCopySpecTemp.setTemplateName(newSpecSubPartTempName);
                        newCopySpecTemp.setParentPartPosi(0L);
                        newCopySpecTemp.setPhyPartPosition(0L);
                        newCopySpecTemp.setSpecTemplate(true);
                        newCopySpecTemp.setLogical(true);
                        newCopySpecTemp.setLocation(null);
                        newPartInfo.getPartitionBy().getSubPartitionBy().getPartitions().add(newCopySpecTemp);

                        newTmpSubpartSpecTempNameSet.add(newSpecSubPartTempName);
                    }
                }

            } else {

                PartitionSpec newCopySpec = newTmpPartitionSpec;
                final String newSpecName = parentPreparedData.getTempPartitionNames().get(0);
                PartitionGroupRecord newSpecPgRec = parentPreparedData.getInvisiblePartitionGroups().stream()
                    .filter(o -> o.getPartition_name().equalsIgnoreCase(newSpecName)).findFirst().orElse(null);
                Pair<String, String> newSpecLocation = orderedTargetTableLocations.get(newSpecName);
                Long newSpecPosi = Long.valueOf(allPartSpecsWithSameParent.size() + 1);
                Long newSpecParentPosi = isModifySubPart ? parentPartSpec.getPosition() : 0;
                Long newSpecPhyPosi = Long.valueOf(allPhySpecs.size() + 1);
                PartitionBoundSpec newSpecBoundVal = boundSpec;
                newTmpPartitionSpec = initTempPartSpec(newCopySpec,
                    newSpecName, null, newSpecPgRec, newSpecLocation, newSpecPosi, newSpecParentPosi, newSpecPhyPosi,
                    newSpecBoundVal);
                allPartSpecsWithSameParent.add(newTmpPartitionSpec);

                targetNewTmpPartSpecThatContainAllDropVals.add(newTmpPartitionSpec);
            }
        } else {

            List<String> tempPartitionNames = parentPreparedData.getTempPartitionNames();
            List<String> parentPartitionNames = parentPreparedData.getParentPartitionNames();
            List<String> subPartTempNames = parentPreparedData.getSubPartTempNames();

            if (!tempPartitionNames.isEmpty()) {
                if (useSubPart) {

                    /**
                     * now modify values only support modify one partition,
                     * so all parentPartitionNames should be the same,
                     */
                    PartitionSpec newParentSpec = newTmpPartitionSpec.copy();
                    List<PartitionSpec> newSubPartSpecs = newParentSpec.getSubPartitions();

                    newParentSpec.setBoundSpec(boundSpec);
                    newParentSpec.setName(parentPartitionNames.get(0));
                    long newParentSpecPosi = allPartSpecsWithSameParent.size() + 1L;
                    newParentSpec.setPosition(newParentSpecPosi);

                    for (int i = 0; i < tempPartitionNames.size(); i++) {
                        final String newTmpSpecName = tempPartitionNames.get(i);
                        PartitionSpec newSubPartSpec = newSubPartSpecs.get(i);

                        String newTmpSubSpecTempName = useSubPartTemp ? subPartTempNames.get(i) : null;
                        PartitionGroupRecord newSpecPgRec = parentPreparedData.getInvisiblePartitionGroups().stream()
                            .filter(o -> o.getPartition_name().equalsIgnoreCase(newTmpSpecName)).findFirst()
                            .orElse(null);
                        Pair<String, String> newSpecLocation = orderedTargetTableLocations.get(newTmpSpecName);
                        Long newSpecPosi = Long.valueOf(i + 1);
                        Long newSpecParentPosi = newParentSpecPosi;
                        Long newSpecPhyPosi = Long.valueOf(allPhySpecs.size() + 1);
                        PartitionBoundSpec sbBndSpec = newSubPartSpec.getBoundSpec();
                        PartitionSpec newTmpSubPartSpec = initTempPartSpec(newSubPartSpec,
                            newTmpSpecName, newTmpSubSpecTempName, newSpecPgRec, newSpecLocation, newSpecPosi,
                            newSpecParentPosi,
                            newSpecPhyPosi, sbBndSpec);
                        newTmpSubPartSpec.setTemplateName(newTmpSubSpecTempName);
                        newSubPartSpecs.set(i, newTmpSubPartSpec);
                        targetNewTmpPartSpecThatContainAllDropVals.add(newTmpSubPartSpec);
                    }
                    allPartSpecsWithSameParent.add(newParentSpec);
                    //targetNewTmpPartSpecThatContainAllDropVals.add(newParentSpec);

                } else {

                    for (int i = 0; i < tempPartitionNames.size(); i++) {
                        PartitionSpec newCopySpec = newTmpPartitionSpec;
                        final String newTmpSpecName = tempPartitionNames.get(i);
                        PartitionGroupRecord newSpecPgRec = parentPreparedData.getInvisiblePartitionGroups().stream()
                            .filter(o -> o.getPartition_name().equalsIgnoreCase(newTmpSpecName)).findFirst()
                            .orElse(null);
                        Pair<String, String> newSpecLocation = orderedTargetTableLocations.get(newTmpSpecName);
                        Long newSpecPosi = Long.valueOf(allPartSpecsWithSameParent.size() + 1);
                        Long newSpecParentPosi = 0L;
                        Long newSpecPhyPosi = Long.valueOf(allPhySpecs.size() + 1);
                        PartitionBoundSpec newSpecBoundVal = boundSpec;
                        newTmpPartitionSpec = initTempPartSpec(newCopySpec,
                            newTmpSpecName, null, newSpecPgRec, newSpecLocation, newSpecPosi, newSpecParentPosi,
                            newSpecPhyPosi, newSpecBoundVal);
                        allPartSpecsWithSameParent.add(newTmpPartitionSpec);
                        targetNewTmpPartSpecThatContainAllDropVals.add(newTmpPartitionSpec);
                    }
                }
            }
        }

        AlterTableGroupSnapShotUtils.updatePartitionSpecRelationship(newPartInfo);
        return targetNewTmpPartSpecThatContainAllDropVals;
    }

    private PartitionSpec initTempPartSpec(
        PartitionSpec newCopySpec,
        String newSpecName,
        String newSpecSubPartTempName,
        PartitionGroupRecord newSpecPgRec,
        Pair<String, String> newSpecLocation,
        Long newSpecPosi,
        Long newSpecParentPosi,
        Long newSpecPhyPosi,
        PartitionBoundSpec newSpecBoundVal
    ) {

        PartitionSpec newTmpPartitionSpec = newCopySpec;

        /**
         * Init the name for new tmp part spec
         */
        final String tmpPartName = newSpecName;
        newTmpPartitionSpec.setName(tmpPartName);
        newTmpPartitionSpec.setTemplateName(newSpecSubPartTempName);

        /**
         * Init the physical location for the tmp part spec
         */
        Pair<String, String> tempPartLocation = newSpecLocation;
        newTmpPartitionSpec.getLocation().setGroupKey(tempPartLocation.getValue());
        newTmpPartitionSpec.getLocation().setPhyTableName(tempPartLocation.getKey());
        newTmpPartitionSpec.getLocation().setVisiable(false);

        /**
         * Init the pg_id for new tmp part spec
         */
        PartitionGroupRecord partitionGroupRecord = newSpecPgRec;
        assert partitionGroupRecord != null;
        newTmpPartitionSpec.getLocation().setPartitionGroupId(partitionGroupRecord.getId());

        /**
         * Init the part position & phyPart position for the tmp part spec
         */
        newTmpPartitionSpec.setPosition(newSpecPosi);
        newTmpPartitionSpec.setParentPartPosi(newSpecParentPosi);
        newTmpPartitionSpec.setPhyPartPosition(newSpecPhyPosi);

        /**
         * Init the bound spec of those values to be dropped
         */
        newTmpPartitionSpec.setBoundSpec(newSpecBoundVal.copy());

        return newTmpPartitionSpec;
    }

    public List<PartitionSpec> getTempPartitionSpecs() {
        return tempPartitionSpecs;
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPreparedData;
    }
}
