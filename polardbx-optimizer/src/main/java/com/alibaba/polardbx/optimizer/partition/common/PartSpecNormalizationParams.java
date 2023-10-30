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

package com.alibaba.polardbx.optimizer.partition.common;

import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartSpecNormalizationParams {

    protected boolean usePartGroupNameAsPartName;
    protected String partGrpName;
    protected boolean needSortBoundValues;
    protected List<Integer> allLevelPrefixPartColCnts;

    protected PartitionSpec partSpec;
    protected Map<Long, String> partGrpNameInfo;
    protected boolean needSortPartitions;
    protected SearchDatumComparator boundSpaceComparator;
    protected boolean showHashByRange;

    protected boolean useSubPartByTemp;
    protected String textIntentBase;

    public PartSpecNormalizationParams() {
    }

    public boolean isUsePartGroupNameAsPartName() {
        return usePartGroupNameAsPartName;
    }

    public void setUsePartGroupNameAsPartName(boolean usePartGroupNameAsPartName) {
        this.usePartGroupNameAsPartName = usePartGroupNameAsPartName;
    }

    public String getPartGrpName() {
        return partGrpName;
    }

    public void setPartGrpName(String partGrpName) {
        this.partGrpName = partGrpName;
    }

    public boolean isNeedSortBoundValues() {
        return needSortBoundValues;
    }

    public void setNeedSortBoundValues(boolean needSortBoundValues) {
        this.needSortBoundValues = needSortBoundValues;
    }

    public PartitionSpec getPartSpec() {
        return partSpec;
    }

    public void setPartSpec(PartitionSpec partSpec) {
        this.partSpec = partSpec;
    }

    public Map<Long, String> getPartGrpNameInfo() {
        return partGrpNameInfo;
    }

    public void setPartGrpNameInfo(Map<Long, String> partGrpNameInfo) {
        this.partGrpNameInfo = partGrpNameInfo;
    }

    public boolean isNeedSortPartitions() {
        return needSortPartitions;
    }

    public void setNeedSortPartitions(boolean needSortPartitions) {
        this.needSortPartitions = needSortPartitions;
    }

    public boolean isShowHashByRange() {
        return showHashByRange;
    }

    public void setShowHashByRange(boolean showHashByRange) {
        this.showHashByRange = showHashByRange;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(
        SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public boolean isUseSubPartByTemp() {
        return useSubPartByTemp;
    }

    public void setUseSubPartByTemp(boolean useSubPartByTemp) {
        this.useSubPartByTemp = useSubPartByTemp;
    }

    public List<Integer> getAllLevelPrefixPartColCnts() {
        return allLevelPrefixPartColCnts;
    }

    public void setAllLevelPrefixPartColCnts(List<Integer> allLevelPrefixPartColCnts) {
        this.allLevelPrefixPartColCnts = allLevelPrefixPartColCnts;
    }

    public String getTextIntentBase() {
        return textIntentBase;
    }

    public void setTextIntentBase(String textIntentBase) {
        this.textIntentBase = textIntentBase;
    }
}
