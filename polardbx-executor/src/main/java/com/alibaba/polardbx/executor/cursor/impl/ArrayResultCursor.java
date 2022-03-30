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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ArrayResultCursor extends ResultCursor {

    private List<Row> rows = new ArrayList<>();
    private Iterator<Row> iter = null;
    private CursorMeta meta;
    private final String tableName;
    private boolean closed = false;

    public ArrayResultCursor(String tableName) {
        this.tableName = tableName;
        this.returnColumns = new ArrayList<>();
    }

    public void addColumn(ColumnMeta cm) {
        ColumnMeta c = new ColumnMeta(this.tableName, cm.getName(), null, cm.getField());
        returnColumns.add(c);
    }

    public void addColumn(String columnName, DataType type) {
        addColumn(columnName, type, true);
    }

    public void addColumn(String columnName, DataType type, boolean columnHeaderToUpperCase) {
        Field field = new Field(tableName, columnName, type);
        columnName = columnHeaderToUpperCase ? TStringUtil.upperCase(columnName) : columnName;
        ColumnMeta c = new ColumnMeta(this.tableName, columnName, null, field);
        returnColumns.add(c);
    }

    public void addColumn(String columnName, String alias, DataType type) {
        int size = Field.DEFAULT_COLUMN_SIZE;
        if (DataTypeUtil.equalsSemantically(type, DataTypes.BooleanType)) {
            size = 1;
        }
        ColumnMeta c = new ColumnMeta(this.tableName, columnName, alias, new Field(tableName, columnName, type));
        returnColumns.add(c);
    }

    public void addRow(Object[] values) {
        if (this.meta == null) {
            this.meta = CursorMeta.build(returnColumns);
        }
        ArrayRow row = new ArrayRow(this.meta, values);
        rows.add(row);
    }

    @Override
    public Row doNext() {
        if (iter == null) {
            iter = rows.iterator();
        }
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        this.closed = true;
        if (exceptions == null) {
            exceptions = new ArrayList<>();
        }
        return exceptions;
    }

    public void initMeta() {
        this.meta = CursorMeta.build(returnColumns);
    }

    public boolean isClosed() {
        return this.closed;
    }

    public List<Row> getRows() {
        return rows;
    }

    public CursorMeta getMeta() {
        return meta;
    }

    public String getTableName() {
        return tableName;
    }

}
