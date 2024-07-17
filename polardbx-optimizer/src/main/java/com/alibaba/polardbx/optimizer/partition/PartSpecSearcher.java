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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class PartSpecSearcher {

    public static final long NO_FOUND_PART_SPEC = -1L;
    public static final long FOUND_NON_PARTITIONED_TBL = -2L;

    /**
     * key: grp
     * val: {
     * key: tbl
     * val: PartitionSpec
     * }
     */
    protected Map<String, Map<String, PartitionSpec>> phyInfoSpecMap;
    /**
     * key: partName or subPartName
     * val:
     * partSpec of (sub)partName
     */
    protected Map<String, PartitionSpec> partNameToSpecMap;
    /**
     * key: partName of subPartTempName
     * val:
     * subpartSpec Temp
     */
    protected Map<String, PartitionSpec> subPartTempNameToSpecMap;
    protected PartitionTableType tableType;
    protected int phyPartCount;
    /**
     * Use to collate different subpartition spec definitions of different partition
     * (only used for non-subpartition-template definitions)
     * <p>
     * key: the digest of subpartition spec definitions of one partition
     * val: the partition spec that the subpartitions belong to
     */
    protected Map<String, List<PartitionSpec>> subPartSpecDefDigestMap;

    public static PartSpecSearcher buildPartSpecSearcher(PartitionTableType tblType, PartitionByDefinition partByDef) {
        return new PartSpecSearcher(tblType, partByDef);
    }

    private PartSpecSearcher(PartitionTableType tableType, PartitionByDefinition partByDef) {
        this.tableType = tableType;
        this.phyInfoSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.partNameToSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.subPartTempNameToSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.subPartSpecDefDigestMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        partByDef.refreshPhysicalPartitionsCache();
        List<PartitionSpec> phySpecList = partByDef.getPhysicalPartitions();
        this.phyPartCount = phySpecList.size();
        for (int i = 0; i < phySpecList.size(); i++) {
            PartitionSpec p = phySpecList.get(i);
            String phyPartName = p.getName();
            PartitionLocation location = p.getLocation();
            String grpName = location.getGroupKey();
            String phyTbl = location.getPhyTableName();
            Map<String, PartitionSpec> phyTbToSpecMap = phyInfoSpecMap.get(grpName);
            if (phyTbToSpecMap == null) {
                phyTbToSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                phyInfoSpecMap.put(grpName, phyTbToSpecMap);
            }
            if (!phyTbToSpecMap.containsKey(phyTbl)) {
                phyTbToSpecMap.put(phyTbl, p);
            }
            this.partNameToSpecMap.put(phyPartName, p);
        }
        if (partByDef.getSubPartitionBy() != null) {
            /**
             * If use subPart, also put top-level partition into partNameToSpecMap
             */
            List<PartitionSpec> pSpecList = partByDef.getPartitions();
            for (int i = 0; i < pSpecList.size(); i++) {
                PartitionSpec p = pSpecList.get(i);
                String partName = p.getName();
                this.partNameToSpecMap.put(partName, p);
            }
            boolean useSubPartTemp = partByDef.getSubPartitionBy().isUseSubPartTemplate();
            if (useSubPartTemp) {
                List<PartitionSpec> subPartSpecTemps = partByDef.getSubPartitionBy().getPartitions();
                for (int i = 0; i < subPartSpecTemps.size(); i++) {
                    this.subPartTempNameToSpecMap.put(subPartSpecTemps.get(i).getName(), subPartSpecTemps.get(i));
                }
            }
        }
    }

    public PartitionSpec getPartSpec(String grpIndex, String phyTb) {
        Map<String, PartitionSpec> phyTbToSpecMap = phyInfoSpecMap.get(grpIndex);
        if (phyTbToSpecMap == null) {
            return null;
        }
        PartitionSpec pSpec = phyTbToSpecMap.get(phyTb);
        if (pSpec == null) {
            return null;
        }
        return pSpec;
    }

    public Long getPartIntraGroupConnKey(String grpIndex, String phyTb) {
        if (!this.tableType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            return FOUND_NON_PARTITIONED_TBL;
        }
        PartitionSpec pSpec = getPartSpec(grpIndex, phyTb);
        if (pSpec == null) {
            return NO_FOUND_PART_SPEC;
        }
        return pSpec.getIntraGroupConnKey();
    }

    public PartitionSpec getPartSpecByPartName(String partNameOrSubPartName) {
        return this.partNameToSpecMap.get(partNameOrSubPartName);
    }

    public int getPhyPartCount() {
        return phyPartCount;
    }

    public Set<String> fetchBothPartNameAndSubPartNameSet() {
        Set<String> alreadyExistsPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        alreadyExistsPartNameSet.addAll(partNameToSpecMap.keySet());
        alreadyExistsPartNameSet.addAll(subPartTempNameToSpecMap.keySet());
        return alreadyExistsPartNameSet;
    }

    public boolean checkIfDuplicated(String newPartName) {
        Set<String> alreadyExistsPartNameSet = partNameToSpecMap.keySet();
        Set<String> alreadyExistsSubPartTempNameSet = partNameToSpecMap.keySet();

        if (alreadyExistsPartNameSet.contains(newPartName)) {
            return true;
        }

        if (alreadyExistsSubPartTempNameSet.contains(newPartName)) {
            return true;
        }

        return false;
    }

}
