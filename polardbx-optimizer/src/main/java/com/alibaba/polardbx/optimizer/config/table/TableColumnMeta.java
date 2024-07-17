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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author qianjing
 */
public class TableColumnMeta extends AbstractLifecycle {

    private final Map<String, String> columnMultiWriteMapping;
    private boolean modifying;

    public TableColumnMeta(TableMeta tableMeta) {
        List<ColumnMeta> columnMetas = tableMeta.getAllColumns();
        modifying = tableMeta.rebuildingTable();
        Map<String, String> columnMapping = new HashMap<>();
        for (ColumnMeta columnMeta : columnMetas) {
            if (columnMeta.getMappingName() != null) {
                // old --> new
                columnMapping.put(columnMeta.getMappingName().toLowerCase(), columnMeta.getName().toLowerCase());
            }
        }
        if (columnMapping.isEmpty()) {
            this.columnMultiWriteMapping = null;
        } else {
            this.columnMultiWriteMapping = columnMapping;
            this.modifying = true;
        }
    }

    public boolean isModifying() {
        return modifying;
    }

    public Map<String, String> getColumnMultiWriteMapping() {
        return columnMultiWriteMapping;
    }

    public boolean isGsiModifying() {
        return MapUtils.isNotEmpty(columnMultiWriteMapping);
    }
}
