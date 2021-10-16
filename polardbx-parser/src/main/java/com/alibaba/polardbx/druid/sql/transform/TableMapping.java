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

package com.alibaba.polardbx.druid.sql.transform;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.HashMap;
import java.util.Map;

public class TableMapping {
    private final String srcTable;
    private final String destTable;
    private final long srcTableHash;

    private final Map<Long, ColumnMapping> columnMappings = new HashMap<Long, ColumnMapping>();

    public TableMapping(String srcTable, String destTable) {
        this.srcTable = SQLUtils.normalize(srcTable);
        this.destTable = SQLUtils.normalize(destTable);
        this.srcTableHash = FnvHash.hashCode64(srcTable);
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getDestTable() {
        return destTable;
    }

    public long getSrcTableHash() {
        return srcTableHash;
    }

    public String getMappingColumn(String srcColumn) {
        long hash = FnvHash.hashCode64(srcColumn);
        ColumnMapping columnMapping = columnMappings.get(hash);
        if (columnMapping == null) {
            return null;
        }
        return columnMapping.destDestColumn;
    }

    public void addColumnMapping(String srcColumn, String destColumn) {
        ColumnMapping columnMapping = new ColumnMapping(srcColumn, destColumn);
        columnMappings.put(columnMapping.srcColumnHash, columnMapping);
    }

    private static class ColumnMapping {
        public final String srcColumn;
        public final String destDestColumn;
        public final long srcColumnHash;

        public ColumnMapping(String srcColumn, String destDestColumn) {
            this.srcColumn = SQLUtils.normalize(srcColumn);
            this.destDestColumn = SQLUtils.normalize(destDestColumn);
            this.srcColumnHash = FnvHash.hashCode64(srcColumn);
        }
    }
}
