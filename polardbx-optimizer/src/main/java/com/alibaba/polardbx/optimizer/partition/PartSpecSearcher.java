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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PartSpecSearcher {

    public static final long NO_FOUND_PART_SPEC = -1L;
    public static final long FOUND_NON_PARTITIONED_TBL = -2L;

    /**
     * key: grp
     * val: {
     *     key: tbl
     *     val: PartitionSpec
     * }
     */
    protected Map<String, Map<String, PartitionSpec>> phyInfoSpecMap;
    protected PartitionTableType tableType;

    public static PartSpecSearcher buildPartSpecSearcher(PartitionTableType tblType, PartitionByDefinition partByDef) {
        return new PartSpecSearcher(tblType, partByDef);
    }

    private PartSpecSearcher(PartitionTableType tableType, PartitionByDefinition partByDef) {
        this.tableType = tableType;
        this.phyInfoSpecMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<PartitionSpec> pSpecList = partByDef.getPartitions();
        for (int i = 0; i < pSpecList.size(); i++) {
            PartitionSpec p = pSpecList.get(i);
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
        if (this.tableType != PartitionTableType.PARTITION_TABLE && this.tableType != PartitionTableType.GSI_TABLE ) {
            return FOUND_NON_PARTITIONED_TBL;
        }
        PartitionSpec pSpec =  getPartSpec(grpIndex, phyTb);
        if (pSpec == null) {
            return NO_FOUND_PART_SPEC;
        }
        return pSpec.getIntraGroupConnKey();
    }
}
