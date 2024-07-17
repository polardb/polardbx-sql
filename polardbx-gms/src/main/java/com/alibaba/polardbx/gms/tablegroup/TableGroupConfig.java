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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Combine the table group and partition group.
 *
 * @author luoyanxin
 */
public class TableGroupConfig {

    TableGroupRecord tableGroupRecord;
    List<PartitionGroupRecord> partitionGroupRecords;
    List<String> tables;
    LocalityDesc localityDesc;

    // index for speed up partition-group & table lookup
    // TODO(moyi)
    // Map<Long, PartitionGroupRecord> pgIdMap;
    // Map<String, TablePartRecordInfoContext> tableNameMap;

    /*public List<TablePartRecordInfoContext> getAllTables() {
        return this.tables;
    }*/

    //todo luoyanxin
    public List<String> getAllTables() {
        return getTables();
    }

    public List<String> getTables() {
        return tables;
    }

    public int getTableCount() {
        if (GeneralUtil.isEmpty(tables)) {
            return 0;
        }
        return tables.size();
    }

    public TableGroupConfig() {
    }

    public TableGroupConfig(TableGroupRecord tableGroupRecord,
                            List<PartitionGroupRecord> partitionGroupRecords,
                            List<String> tables,
                            String locality) {
        this.tableGroupRecord = tableGroupRecord;
        this.partitionGroupRecords = partitionGroupRecords;
        this.tables = tables;
        this.localityDesc = LocalityDesc.parse(locality);
    }

    /**
     * Get a specified partition-group
     */
    public PartitionGroupRecord getPartitionGroup(long pgId) {
        // TODO(moyi) optimize performance through indexing by pgid
        return this.partitionGroupRecords.stream()
            .filter(x -> x.id == pgId)
            .findFirst()
            .orElseThrow(() ->
                new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("partition-group %d not exists", pgId)));
    }

    public TableGroupRecord getTableGroupRecord() {
        return tableGroupRecord;
    }

    public void setTableGroupRecord(TableGroupRecord tableGroupRecord) {
        this.tableGroupRecord = tableGroupRecord;
    }

    public List<PartitionGroupRecord> getPartitionGroupRecords() {
        return partitionGroupRecords;
    }

    public void setPartitionGroupRecords(
        List<PartitionGroupRecord> partitionGroupRecords) {
        this.partitionGroupRecords = partitionGroupRecords;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public PartitionGroupRecord getPartitionGroupByName(String pgName) {
        return getPartitionGroupRecords().stream()
            .filter(o -> o.partition_name.equalsIgnoreCase(pgName)).findFirst().orElse(null);

    }

    public void setLocality(String locality) {
        localityDesc = LocalityDesc.parse(locality);
    }

    public LocalityDesc getLocalityDesc() {
        return this.localityDesc;
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(this.partitionGroupRecords) &&
            CollectionUtils.isEmpty(this.tables);
    }

    public boolean isManuallyCreated() {
        return tableGroupRecord != null && tableGroupRecord.manual_create == 1;
    }

    public boolean isAutoSplit() {
        return tableGroupRecord != null && tableGroupRecord.auto_split_policy != 0;
    }

    public boolean isPreDefinePartitionInfo() {
        return tableGroupRecord != null && !StringUtils.isEmpty(tableGroupRecord.partition_definition);
    }

    public boolean isColumnarTableGroup() {
        return tableGroupRecord != null && tableGroupRecord.isColumnarTableGroup();
    }

    public String getPreDefinePartitionInfo() {
        if (isPreDefinePartitionInfo()) {
            return tableGroupRecord.getPartition_definition();
        } else {
            return null;
        }
    }

    public boolean containsTable(String tableName) {
        if (tables == null) {
            return false;
        }

        for (String existTable : tables) {
            if (StringUtils.equalsIgnoreCase(existTable, tableName)) {
                return true;
            }
        }
        return false;
    }

    public static TableGroupConfig copyWithoutTables(TableGroupConfig source) {
        if (source == null) {
            return null;
        }
        TableGroupConfig tableGroupConfig = new TableGroupConfig();
        tableGroupConfig.tableGroupRecord = source.tableGroupRecord;
        tableGroupConfig.partitionGroupRecords = source.partitionGroupRecords;
        tableGroupConfig.tables = new ArrayList<>();
        tableGroupConfig.localityDesc = source.localityDesc;
        return tableGroupConfig;
    }
}
