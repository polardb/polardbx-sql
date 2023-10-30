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

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table file storage meta info
 */
public class TableFilesMeta {
    /**
     * for old oss file without column mapping, forbid ddl
     */
    private boolean oldFileStorage;

    // for oss engine
    private Map<String, Map<String, List<FileMeta>>> fileMetaSet = null;
    private Map<String, List<FileMeta>> flatFileMetas = null;

    /**
     * map from column name to fieldId, fieldId must end with '__id__'
     */
    Map<String, String> columnMapping = null;

    public TableFilesMeta(Map<String, String> columnMapping, Map<String, ColumnMeta> columnMetaMap) {
        if (columnMapping == null || columnMapping.isEmpty()) {
            this.oldFileStorage = true;
            this.columnMapping = Maps.newTreeMap(String::compareToIgnoreCase);
            columnMetaMap.keySet().forEach(x -> this.columnMapping.put(x, x));
            return;
        }
        this.oldFileStorage = false;
        this.columnMapping = columnMapping;
    }

    public void setFileMetaSet(Map<String, Map<String, List<FileMeta>>> fileMetaSet) {
        this.fileMetaSet = fileMetaSet;
        // build flat map for physical table - file meta
        Map<String, List<FileMeta>> flatFileMetas = new HashMap<>();
        for (Map.Entry<String, Map<String, List<FileMeta>>> phySchemas : this.fileMetaSet.entrySet()) {
            for (Map.Entry<String, List<FileMeta>> phyTables : phySchemas.getValue().entrySet()) {
                flatFileMetas.put(phyTables.getKey(), phyTables.getValue());
            }
        }
        this.flatFileMetas = flatFileMetas;
    }

    public Map<String, List<FileMeta>> getFlatFileMetas() {
        return flatFileMetas;
    }

    public boolean isOldFileStorage() {
        return oldFileStorage;
    }
}
