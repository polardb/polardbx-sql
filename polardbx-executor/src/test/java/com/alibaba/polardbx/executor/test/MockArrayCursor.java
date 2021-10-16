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

package com.alibaba.polardbx.executor.test;

import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MockArrayCursor extends AbstractCursor {

    private List<Row> rows = new ArrayList<>();
    private Iterator<Row> iter = null;
    private CursorMeta meta;
    private final String tableName;
    private List<ColumnMeta> columns = new ArrayList<>();
    private Row current;
    private boolean closed = false;

    public MockArrayCursor(String tableName) {
        super(false);
        this.tableName = tableName;
    }

    public void addColumn(String columnName, DataType type) {
        Field field = new Field(tableName, columnName, type);
        ColumnMeta c = new ColumnMeta(this.tableName, columnName, null, field);
        columns.add(c);

    }

    public void addRow(Object[] values) {
        ArrayRow row = new ArrayRow(this.meta, values);
        rows.add(row);
    }

    @Override
    public void doInit() {

        iter = rows.iterator();
    }

    @Override
    public Row doNext() {
        if (iter.hasNext()) {
            current = iter.next();
            return current;
        }
        current = null;
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

    @Override
    public List<ColumnMeta> getReturnColumns() {
        return columns;
    }

    public void initMeta() {
        this.meta = CursorMeta.build(columns);
    }

    public boolean isClosed() {
        return this.closed;
    }
}
