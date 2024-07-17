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
