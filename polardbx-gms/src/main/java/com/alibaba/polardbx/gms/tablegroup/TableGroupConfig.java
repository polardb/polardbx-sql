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
    List<TablePartRecordInfoContext> tables;
    LocalityDesc localityDesc;

    // index for speed up partition-group & table lookup
    // TODO(moyi)
    // Map<Long, PartitionGroupRecord> pgIdMap;
    // Map<String, TablePartRecordInfoContext> tableNameMap;

    public List<TablePartRecordInfoContext> getAllTables() {
        return this.tables;
    }

    public TablePartRecordInfoContext getTableById(long tableId) {
        return this.tables.stream()
            .filter(x -> x.getLogTbRec().id == tableId)
            .findAny()
            .orElseThrow(() -> new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST, String.valueOf(tableId)));
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
                            List<TablePartRecordInfoContext> tables) {
        this.tableGroupRecord = tableGroupRecord;
        this.partitionGroupRecords = partitionGroupRecords;
        this.tables = tables;
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

    public List<TablePartRecordInfoContext> getTables() {
        return tables;
    }

    public void setTables(List<TablePartRecordInfoContext> tables) {
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

    public boolean containsTable(String tableName) {
        if (tables == null) {
            return false;
        }

        for (TablePartRecordInfoContext entry : tables) {
            if (StringUtils.equalsIgnoreCase(entry.getTableName(), tableName)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, List<String>> logicalToPhyTables() {
        Map<String, List<String>> res = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (TablePartRecordInfoContext tableInfo : GeneralUtil.emptyIfNull(getTables())) {
            for (TablePartitionRecord tablePart : tableInfo.getPartitionRecList()) {
                res.computeIfAbsent(tablePart.getTableName(), (x) -> new ArrayList<>())
                    .add(tablePart.getPhyTable());
            }
        }
        return res;
    }

    public Map<String, String> phyToLogicalTables() {
        Map<String, String> res = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (TablePartRecordInfoContext tableInfo : GeneralUtil.emptyIfNull(getTables())) {
            for (TablePartitionRecord tablePart : tableInfo.getPartitionRecList()) {
                res.put(tablePart.getPhyTable(), tablePart.getTableName());
            }
        }
        return res;
    }
}
