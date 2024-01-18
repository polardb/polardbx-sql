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

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartitionByNormalizationParams {

    protected PartitionByDefinition parentPartBy;
    protected boolean showHashByRange;
    protected String textIntentBase;

    protected boolean normalizeForTableGroup = false;
    protected TableGroupConfig tableGroupConfig;
    protected boolean usePartGroupNameAsPartName = false;
    protected boolean needSortPartitions = false;
    protected List<Integer> tgAllLevelPrefixPartColCnts;
    /**
     * partGrpId -> phyPartName
     */
    protected Map<Long, String> partGrpNameInfo;

    public PartitionByDefinition getParentPartBy() {
        return parentPartBy;
    }

    public void setParentPartBy(PartitionByDefinition parentPartBy) {
        this.parentPartBy = parentPartBy;
    }

    public boolean isShowHashByRange() {
        return showHashByRange;
    }

    public void setShowHashByRange(boolean showHashByRange) {
        this.showHashByRange = showHashByRange;
    }

    public String getTextIntentBase() {
        return textIntentBase;
    }

    public void setTextIntentBase(String textIntentBase) {
        this.textIntentBase = textIntentBase;
    }

    public boolean isNormalizeForTableGroup() {
        return normalizeForTableGroup;
    }

    public void setNormalizeForTableGroup(boolean normalizeForTableGroup) {
        this.normalizeForTableGroup = normalizeForTableGroup;
    }

    public TableGroupConfig getTableGroupConfig() {
        return tableGroupConfig;
    }

    public void setTableGroupConfig(TableGroupConfig tableGroupConfig) {
        this.tableGroupConfig = tableGroupConfig;
    }

    public boolean isUsePartGroupNameAsPartName() {
        return usePartGroupNameAsPartName;
    }

    public void setUsePartGroupNameAsPartName(boolean usePartGroupNameAsPartName) {
        this.usePartGroupNameAsPartName = usePartGroupNameAsPartName;
    }

    public boolean isNeedSortPartitions() {
        return needSortPartitions;
    }

    public void setNeedSortPartitions(boolean needSortPartitions) {
        this.needSortPartitions = needSortPartitions;
    }

    public List<Integer> getTgAllLevelPrefixPartColCnts() {
        return tgAllLevelPrefixPartColCnts;
    }

    public void setTgAllLevelPrefixPartColCnts(List<Integer> tgAllLevelPrefixPartColCnts) {
        this.tgAllLevelPrefixPartColCnts = tgAllLevelPrefixPartColCnts;
    }

    public Map<Long, String> getPartGrpNameInfo() {
        return partGrpNameInfo;
    }

    public void setPartGrpNameInfo(Map<Long, String> partGrpNameInfo) {
        this.partGrpNameInfo = partGrpNameInfo;
    }
}
