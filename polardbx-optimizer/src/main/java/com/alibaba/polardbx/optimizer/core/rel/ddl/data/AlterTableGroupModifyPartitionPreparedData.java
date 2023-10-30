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

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AlterTableGroupModifyPartitionPreparedData extends AlterTableGroupBasePreparedData {
    // the temp partition for the values to be drop
    private String tempPartition;

    /**
     * <pre>
     *    the element i of tempPartitionNames[i] means:
     *      the new generated tmp physical part name
     * </pre>
     */
    protected List<String> tempPartitionNames;
    /**
     * <pre>
     *    the element i of parentPartitionNames[i] means:
     *      the parent part spec name of the tmp part of tempPartitionNames[i]
     * </pre>
     */
    protected List<String> parentPartitionNames;
    /**
     * <pre>
     *    the element i of subPartTempNames[i] means:
     *      the new template subpart name of the tmp part of tempPartitionNames[i]
     * </pre>
     */
    protected List<String> subPartTempNames;

    private Map<SqlNode, RexNode> partBoundExprInfo;
    protected boolean isModifySubPart = false;
    protected boolean useSubPartTemp = false;
    protected boolean useSubPart = false;

    public AlterTableGroupModifyPartitionPreparedData() {
    }

    @Override
    public void prepareInvisiblePartitionGroup() {
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(getTableGroupName());
        assert tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables());
        String firstTbName = tableGroupConfig.getAllTables().get(0).getLogTbRec().getTableName();
        PartitionInfo firstTblPartInfo =
            OptimizerContext.getContext(getSchemaName()).getPartitionInfoManager().getPartitionInfo(firstTbName);
        Long tableGroupId = firstTblPartInfo.getTableGroupId();

        int oldPartNameCountToBeModified = getOldPartitionNames().size();
        assert oldPartNameCountToBeModified > 0;
        //assert getOldPartitionNames().size() == 1;

        int targetDbCount = getTargetGroupDetailInfoExRecords().size();
        int i = 0;

        List<String> allPhyPartsToBeMoidfied = new ArrayList<>();

        useSubPart = false;
        useSubPartTemp = false;
        if (firstTblPartInfo.getPartitionBy().getSubPartitionBy() != null) {
            useSubPart = true;
            useSubPartTemp = firstTblPartInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        if (isDropVal()) {

            /**
             * Generate the temporaried new physical partition (group) names for
             * catching all values to be dropped of first-part-level or second-part-level.
             */
            List<String> tempPartitionNames = new ArrayList<>();
            List<String> parentPartNames = new ArrayList<>();
            List<String> subpartTempNames = new ArrayList<>();

            TreeSet<String> alreadyExistsSubPartNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            alreadyExistsSubPartNames.addAll(
                firstTblPartInfo.getPartSpecSearcher().fetchBothPartNameAndSubPartNameSet());

            if (isModifySubPart) {
                if (useSubPartTemp) {

                    /**
                     * For generation the names of phy subpart specs  for templated subpart,
                     * should use the format: partName + newSubPartTempName
                     */
                    List<String> tmpSubPartTempNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                        1, alreadyExistsSubPartNames, isModifySubPart);

                    String tmpSubPartTemp = tmpSubPartTempNames.get(0);
                    List<PartitionSpec> firstPartLevelSpecs = firstTblPartInfo.getPartitionBy().getPartitions();
                    for (int j = 0; j < firstPartLevelSpecs.size(); j++) {
                        PartitionSpec part = firstPartLevelSpecs.get(j);
                        String partName = part.getName();
                        String newTmpSubPartName =
                            PartitionNameUtil.autoBuildSubPartitionName(partName, tmpSubPartTemp);
                        parentPartNames.add(partName);
                        subpartTempNames.add(tmpSubPartTemp);
                        tempPartitionNames.add(newTmpSubPartName);
                    }

                } else {
                    /**
                     * For generation the names of phy subpart specs,
                     * should use the format: spxxx
                     */
                    tempPartitionNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                        oldPartNameCountToBeModified, alreadyExistsSubPartNames, isModifySubPart);
                }
            } else {
                if (useSubPart) {
                    List<String> oldPhyPartNames = getOldPartitionNames();
                    Map<String, PartitionSpec> oldParentPartInfoMap =
                        new TreeMap<String, PartitionSpec>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    for (int j = 0; j < oldPhyPartNames.size(); j++) {
                        String phyPartName = oldPhyPartNames.get(i);
                        PartitionSpec spSpec =
                            firstTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(phyPartName);
                        Long parentPosi = spSpec.getParentPartPosi();
                        PartitionSpec pSpec =
                            firstTblPartInfo.getPartitionBy().getPartitions().get(parentPosi.intValue() - 1);
                        if (!oldParentPartInfoMap.containsKey(pSpec.getName())) {
                            oldParentPartInfoMap.put(pSpec.getName(), pSpec);
                        }
                    }

                    /**
                     * Generate one new first-level-part name which will be used for the temporary first-level-part
                     * which contains all the values to be dropped
                     */
                    int parentPartCnt = oldParentPartInfoMap.keySet().size();
                    List<String> newTmpParentPartNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                        parentPartCnt, alreadyExistsSubPartNames, false);

                    List<String> parentParts = oldParentPartInfoMap.keySet().stream().collect(Collectors.toList());
                    for (int j = 0; j < parentPartCnt; j++) {
                        String partName = parentParts.get(j);
                        PartitionSpec pSpec = oldParentPartInfoMap.get(partName);
                        String newTempPartName = newTmpParentPartNames.get(j);
                        List<PartitionSpec> subPartSpecs = pSpec.getSubPartitions();

                        if (useSubPartTemp) {
                            /**
                             * Generate the new second-level-subpart names by partName + subPartTempName
                             */
                            for (int k = 0; k < subPartSpecs.size(); k++) {
                                PartitionSpec sp = subPartSpecs.get(k);
                                String spName = sp.getName();
                                String spTempName = sp.getTemplateName();
                                String newTmpSubPartName =
                                    PartitionNameUtil.autoBuildSubPartitionName(newTempPartName, spTempName);
                                parentPartNames.add(newTempPartName);
                                subpartTempNames.add(spTempName);
                                tempPartitionNames.add(newTmpSubPartName);

                            }
                        } else {
                            /**
                             * Generate the new second-level-subpart names directly
                             */
                            int subPartCnt = subPartSpecs.size();
                            List<String> newTmpSubPartNames =
                                PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                                    subPartCnt, alreadyExistsSubPartNames, true);
                            for (int k = 0; k < newTmpSubPartNames.size(); k++) {
                                parentPartNames.add(newTempPartName);
                                tempPartitionNames.add(newTmpSubPartNames.get(k));
                            }
                        }
                    }
                } else {
                    /**
                     * For generation the names of part specs,
                     * should use the format: pxxx
                     */
                    tempPartitionNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig,
                        oldPartNameCountToBeModified, alreadyExistsSubPartNames,
                        isModifySubPart);

                }
            }

            this.tempPartitionNames = tempPartitionNames;
            this.parentPartitionNames = parentPartNames;
            this.subPartTempNames = subpartTempNames;

            allPhyPartsToBeMoidfied.addAll(getOldPartitionNames());
            allPhyPartsToBeMoidfied.addAll(tempPartitionNames);
        } else {
            allPhyPartsToBeMoidfied.addAll(getOldPartitionNames());
        }

        /**
         * Allocate the physical locations for each "to be modified" phy parts
         */
        for (String oldPartitionName : allPhyPartsToBeMoidfied) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 0;
            partitionGroupRecord.partition_name = oldPartitionName;
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = getTargetGroupDetailInfoExRecords().get(i % targetDbCount).phyDbName;

            partitionGroupRecord.locality = "";
            partitionGroupRecord.pax_group_id = 0L;
            inVisiblePartitionGroups.add(partitionGroupRecord);
            i++;
        }

        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    public String getTempPartition() {
        return tempPartition;
    }

    public void setTempPartition(String tempPartition) {
        this.tempPartition = tempPartition;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames;
    }

    public void setTempPartitionNames(List<String> tempPartitionNames) {
        this.tempPartitionNames = tempPartitionNames;
    }

    public boolean isModifySubPart() {
        return isModifySubPart;
    }

    public boolean needRemoveTempParentPart() {
        if (isModifySubPart) {
            return false;
        } else {
            if (!useSubPart) {
                return false;
            }
            return true;
        }

    }

    public void setModifySubPart(boolean modifySubPart) {
        isModifySubPart = modifySubPart;
    }

    public List<String> getParentPartitionNames() {
        return parentPartitionNames;
    }

    public void setParentPartitionNames(List<String> parentPartitionNames) {
        this.parentPartitionNames = parentPartitionNames;
    }

    public List<String> getSubPartTempNames() {
        return subPartTempNames;
    }

    public void setSubPartTempNames(List<String> subPartTempNames) {
        this.subPartTempNames = subPartTempNames;
    }

    public boolean isUseSubPartTemp() {
        return useSubPartTemp;
    }

    public void setUseSubPartTemp(boolean useSubPartTemp) {
        this.useSubPartTemp = useSubPartTemp;
    }

    public boolean isUseSubPart() {
        return useSubPart;
    }

    public void setUseSubPart(boolean useSubPart) {
        this.useSubPart = useSubPart;
    }
}
