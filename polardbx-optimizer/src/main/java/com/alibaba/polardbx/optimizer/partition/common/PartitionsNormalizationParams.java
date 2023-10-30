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
public class PartitionsNormalizationParams {
    protected StringBuilder builder;

    List<PartitionSpec> partSpecList;
    List<PartitionSpec> orderedPartSpecList;

    protected boolean needSortPartitions;
    protected boolean showHashByRange;
    protected List<Integer> allLevelPrefixPartColCnt;
    protected boolean usePartGroupNameAsPartName;
    protected Map<Long, String> partGrpNameInfo;
    protected SearchDatumComparator boundSpaceComparator;
    protected String textIndentBase = "";
    protected String textIndent = "";

    boolean useSubPartBy = false;
    boolean useSubPartByTemp = false;
    boolean buildSubPartByTemp = false;
    boolean nextPartLevelUseTemp = false;

    protected PartitionStrategy parentPartStrategy;

    public PartitionsNormalizationParams() {
    }

    public StringBuilder getBuilder() {
        return builder;
    }

    public void setBuilder(StringBuilder builder) {
        this.builder = builder;
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

    public boolean isUsePartGroupNameAsPartName() {
        return usePartGroupNameAsPartName;
    }

    public void setUsePartGroupNameAsPartName(boolean usePartGroupNameAsPartName) {
        this.usePartGroupNameAsPartName = usePartGroupNameAsPartName;
    }

    public Map<Long, String> getPartGrpNameInfo() {
        return partGrpNameInfo;
    }

    public void setPartGrpNameInfo(Map<Long, String> partGrpNameInfo) {
        this.partGrpNameInfo = partGrpNameInfo;
    }

    public List<PartitionSpec> getPartSpecList() {
        return partSpecList;
    }

    public void setPartSpecList(List<PartitionSpec> partSpecList) {
        this.partSpecList = partSpecList;
    }

    public List<PartitionSpec> getOrderedPartSpecList() {
        return orderedPartSpecList;
    }

    public void setOrderedPartSpecList(
        List<PartitionSpec> orderedPartSpecList) {
        this.orderedPartSpecList = orderedPartSpecList;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(
        SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public String getTextIndent() {
        return textIndent;
    }

    public void setTextIndent(String textIndent) {
        this.textIndent = textIndent;
    }

    public boolean isUseSubPartBy() {
        return useSubPartBy;
    }

    public void setUseSubPartBy(boolean useSubPartBy) {
        this.useSubPartBy = useSubPartBy;
    }

    public boolean isBuildSubPartByTemp() {
        return buildSubPartByTemp;
    }

    public void setBuildSubPartByTemp(boolean buildSubPartByTemp) {
        this.buildSubPartByTemp = buildSubPartByTemp;
    }

    public boolean isUseSubPartByTemp() {
        return useSubPartByTemp;
    }

    public void setUseSubPartByTemp(boolean useSubPartByTemp) {
        this.useSubPartByTemp = useSubPartByTemp;
    }

    public List<Integer> getAllLevelPrefixPartColCnt() {
        return allLevelPrefixPartColCnt;
    }

    public void setAllLevelPrefixPartColCnt(List<Integer> allLevelPrefixPartColCnt) {
        this.allLevelPrefixPartColCnt = allLevelPrefixPartColCnt;
    }

    public boolean isNextPartLevelUseTemp() {
        return nextPartLevelUseTemp;
    }

    public void setNextPartLevelUseTemp(boolean nextPartLevelUseTemp) {
        this.nextPartLevelUseTemp = nextPartLevelUseTemp;
    }

    public String getTextIndentBase() {
        return textIndentBase;
    }

    public void setTextIndentBase(String textIndentBase) {
        this.textIndentBase = textIndentBase;
    }

    public PartitionStrategy getParentPartStrategy() {
        return parentPartStrategy;
    }

    public void setParentPartStrategy(PartitionStrategy parentPartStrategy) {
        this.parentPartStrategy = parentPartStrategy;
    }
}
