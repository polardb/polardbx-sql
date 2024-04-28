package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author fangwu
 */
public class ColumnarIndexManager {
    private static final ColumnarIndexManager INSTANCE = new ColumnarIndexManager();

    public static ColumnarIndexManager getInstance() {
        return INSTANCE;
    }

    private ColumnarIndexManager() {
    }

    Map<String, IndexPruner> indexPrunerMap = Maps.newConcurrentMap();

    public IndexPruner loadColumnarIndex(String schema, String table, long fileId) {
        String fileKey = fileKey(schema, table, fileId);
        // TODO load, cache and return IndexPruner
        return indexPrunerMap.getOrDefault(fileKey, null);
    }

    private String fileKey(String schema, String table, long fileId) {
        return schema + "_" + table + "_" + fileId;
    }

}
