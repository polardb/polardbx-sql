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
