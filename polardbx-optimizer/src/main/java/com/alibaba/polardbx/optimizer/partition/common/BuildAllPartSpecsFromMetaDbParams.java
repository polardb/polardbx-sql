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

import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
public class BuildAllPartSpecsFromMetaDbParams {
    protected List<TablePartitionSpecConfig> partitionSpecConfigs;
    protected Map<Long, PartitionGroupRecord> partitionGroupRecordsMap;
    protected PartitionStrategy partStrategy;
    protected List<RelDataType> partExprTypeList;
    protected SearchDatumComparator boundSpaceComparator;
    protected String defaultDbIndex;
    protected PartitionTableType tblType;
    protected boolean buildSubPartByTemp = false;
    protected boolean buildSubPartBy = false;
    protected boolean useSubPartBy = false;
    protected boolean useSubPartByTemp = false;
    protected AtomicLong phyPartSpecCounter;
    protected Long parentSpecPosition = 0L;

    public BuildAllPartSpecsFromMetaDbParams() {
    }

    public List<TablePartitionSpecConfig> getPartitionSpecConfigs() {
        return partitionSpecConfigs;
    }

    public void setPartitionSpecConfigs(
        List<TablePartitionSpecConfig> partitionSpecConfigs) {
        this.partitionSpecConfigs = partitionSpecConfigs;
    }

    public Map<Long, PartitionGroupRecord> getPartitionGroupRecordsMap() {
        return partitionGroupRecordsMap;
    }

    public void setPartitionGroupRecordsMap(
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {
        this.partitionGroupRecordsMap = partitionGroupRecordsMap;
    }

    public PartitionStrategy getPartStrategy() {
        return partStrategy;
    }

    public void setPartStrategy(PartitionStrategy partStrategy) {
        this.partStrategy = partStrategy;
    }

    public List<RelDataType> getPartExprTypeList() {
        return partExprTypeList;
    }

    public void setPartExprTypeList(List<RelDataType> partExprTypeList) {
        this.partExprTypeList = partExprTypeList;
    }

    public SearchDatumComparator getBoundSpaceComparator() {
        return boundSpaceComparator;
    }

    public void setBoundSpaceComparator(
        SearchDatumComparator boundSpaceComparator) {
        this.boundSpaceComparator = boundSpaceComparator;
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }

    public PartitionTableType getTblType() {
        return tblType;
    }

    public void setTblType(PartitionTableType tblType) {
        this.tblType = tblType;
    }

    public boolean isBuildSubPartByTemp() {
        return buildSubPartByTemp;
    }

    public void setBuildSubPartByTemp(boolean buildSubPartByTemp) {
        this.buildSubPartByTemp = buildSubPartByTemp;
    }

    public boolean isBuildSubPartBy() {
        return buildSubPartBy;
    }

    public void setBuildSubPartBy(boolean buildSubPartBy) {
        this.buildSubPartBy = buildSubPartBy;
    }

    public boolean isUseSubPartBy() {
        return useSubPartBy;
    }

    public void setUseSubPartBy(boolean useSubPartBy) {
        this.useSubPartBy = useSubPartBy;
    }

    public boolean isUseSubPartByTemp() {
        return useSubPartByTemp;
    }

    public void setUseSubPartByTemp(boolean useSubPartByTemp) {
        this.useSubPartByTemp = useSubPartByTemp;
    }

    public AtomicLong getPhyPartSpecCounter() {
        return phyPartSpecCounter;
    }

    public void setPhyPartSpecCounter(AtomicLong phyPartSpecCounter) {
        this.phyPartSpecCounter = phyPartSpecCounter;
    }

    public Long getParentSpecPosition() {
        return parentSpecPosition;
    }

    public void setParentSpecPosition(Long parentSpecPosition) {
        this.parentSpecPosition = parentSpecPosition;
    }
}
