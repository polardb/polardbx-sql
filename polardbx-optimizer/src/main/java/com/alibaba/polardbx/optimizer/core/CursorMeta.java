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

package com.alibaba.polardbx.optimizer.core;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;

import java.util.List;

/**
 * Metadata of columns for a Cursor or Exec
 */
public class CursorMeta {

    private final List<ColumnMeta> columns;

    public static CursorMeta build(List<ColumnMeta> columns) {
        return new CursorMeta(columns);
    }

    private CursorMeta(List<ColumnMeta> columns) {
        this.columns = ImmutableList.copyOf(columns);
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public ColumnMeta getColumnMeta(Integer index) {
        return columns.get(index);
    }
}
