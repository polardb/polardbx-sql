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
import org.apache.calcite.util.Pair;

/**
 * @author qianjing
 */
public class TableColumnMeta extends AbstractLifecycle {
    private final String tableSchema;
    private final String tableName;

    private final Pair<String, String> columnMultiWriteMapping;
    private final boolean modifying;

    public TableColumnMeta(String tableSchema, String tableName, ColumnMeta sourceColumn, ColumnMeta targetColumn) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;

        modifying = sourceColumn != null || targetColumn != null;

        if (sourceColumn != null && targetColumn != null) {
            this.columnMultiWriteMapping = new Pair<>(sourceColumn.getName(), targetColumn.getName());
        } else {
            this.columnMultiWriteMapping = null;
        }
    }

    protected boolean isModifying() {
        return modifying;
    }

    protected Pair<String, String> getColumnMultiWriteMapping() {
        return columnMultiWriteMapping;
    }
}
