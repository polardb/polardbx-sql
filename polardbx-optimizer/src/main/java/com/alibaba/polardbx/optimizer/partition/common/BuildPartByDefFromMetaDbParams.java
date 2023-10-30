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

import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Params for building a PartitionBy
 *
 * @author chenghui.lch
 */
public class BuildPartByDefFromMetaDbParams {
    protected TablePartitionRecord logTblConfig;
    protected TablePartitionSpecConfig parentSpecConfig;
    protected List<TablePartitionSpecConfig> partitionSpecConfigs;
    protected String defaultDbIndex;

    protected List<ColumnMeta> allColumnMetas;
    protected Map<Long, PartitionGroupRecord> partitionGroupRecordsMap;
    protected Long tableGroupId;
    protected Integer spPartTempFlag;
    protected boolean buildSubPartByTemp = false;
    protected boolean buildSubPartBy = false;
    protected AtomicLong phyPartSpecCounter;

    public BuildPartByDefFromMetaDbParams() {
    }

    public TablePartitionRecord getLogTblConfig() {
        return logTblConfig;
    }

    public void setLogTblConfig(TablePartitionRecord logTblConfig) {
        this.logTblConfig = logTblConfig;
    }

    public List<TablePartitionSpecConfig> getPartitionSpecConfigs() {
        return partitionSpecConfigs;
    }

    public void setPartitionSpecConfigs(
        List<TablePartitionSpecConfig> partitionSpecConfigs) {
        this.partitionSpecConfigs = partitionSpecConfigs;
    }

    public List<ColumnMeta> getAllColumnMetas() {
        return allColumnMetas;
    }

    public void setAllColumnMetas(List<ColumnMeta> allColumnMetas) {
        this.allColumnMetas = allColumnMetas;
    }

    public Map<Long, PartitionGroupRecord> getPartitionGroupRecordsMap() {
        return partitionGroupRecordsMap;
    }

    public void setPartitionGroupRecordsMap(
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {
        this.partitionGroupRecordsMap = partitionGroupRecordsMap;
    }

    public boolean isBuildSubPartBy() {
        return buildSubPartBy;
    }

    public void setBuildSubPartBy(boolean buildSubPartBy) {
        this.buildSubPartBy = buildSubPartBy;
    }

    public Long getTableGroupId() {
        return tableGroupId;
    }

    public void setTableGroupId(Long tableGroupId) {
        this.tableGroupId = tableGroupId;
    }

    public Integer getSpPartTempFlag() {
        return spPartTempFlag;
    }

    public void setSpPartTempFlag(Integer spPartTempFlag) {
        this.spPartTempFlag = spPartTempFlag;
    }

    public TablePartitionSpecConfig getParentSpecConfig() {
        return parentSpecConfig;
    }

    public void setParentSpecConfig(TablePartitionSpecConfig parentSpecConfig) {
        this.parentSpecConfig = parentSpecConfig;
    }

    public boolean isBuildSubPartByTemp() {
        return buildSubPartByTemp;
    }

    public void setBuildSubPartByTemp(boolean buildSubPartByTemp) {
        this.buildSubPartByTemp = buildSubPartByTemp;
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }

    public AtomicLong getPhyPartSpecCounter() {
        return phyPartSpecCounter;
    }

    public void setPhyPartSpecCounter(AtomicLong phyPartSpecCounter) {
        this.phyPartSpecCounter = phyPartSpecCounter;
    }
}
