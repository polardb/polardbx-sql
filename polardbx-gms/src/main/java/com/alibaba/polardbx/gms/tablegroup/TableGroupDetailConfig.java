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
import java.util.stream.Collectors;

/**
 * Combine the table group and partition group.
 *
 * @author luoyanxin
 */
public class TableGroupDetailConfig extends TableGroupConfig {
    List<TablePartRecordInfoContext> tablesPartRecordInfoContext;

    public TableGroupDetailConfig(TableGroupRecord tableGroupRecord,
                                  List<PartitionGroupRecord> partitionGroupRecords,
                                  List<TablePartRecordInfoContext> tablesPartRecordInfoContext,
                                  String locality) {
        super(tableGroupRecord, partitionGroupRecords,
            tablesPartRecordInfoContext.stream().map(o -> o.getTableName()).collect(
                Collectors.toList()), locality);
        this.tablesPartRecordInfoContext = tablesPartRecordInfoContext;
    }

    public List<TablePartRecordInfoContext> getTablesPartRecordInfoContext() {
        return tablesPartRecordInfoContext;
    }

    public void setTablesPartRecordInfoContext(
        List<TablePartRecordInfoContext> tablesPartRecordInfoContext) {
        this.tablesPartRecordInfoContext = tablesPartRecordInfoContext;
    }

    public Map<String, String> phyToLogicalTables() {
        Map<String, String> res = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (TablePartRecordInfoContext tableInfo : GeneralUtil.emptyIfNull(getTablesPartRecordInfoContext())) {
            List<TablePartitionRecord> tableParts = tableInfo.getPartitionRecList();
            if (!tableInfo.getSubPartitionRecList().isEmpty()) {
                tableParts = tableInfo.getSubPartitionRecList();
            }
            for (TablePartitionRecord tablePart : tableParts) {
                res.put(tablePart.getPhyTable(), tablePart.getTableName());
            }
        }
        return res;
    }
}
