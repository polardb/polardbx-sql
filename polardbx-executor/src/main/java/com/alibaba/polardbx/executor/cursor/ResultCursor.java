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

package com.alibaba.polardbx.executor.cursor;

import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

public class ResultCursor extends AbstractCursor {

    public static final String AFFECT_ROW = "AFFECT_ROW";

    protected boolean closed = false;

    private Cursor cursor;
    private CursorMeta cursorMeta;

    public ResultCursor() {
        super(false);
        this.cursor = null;
    }

    public ResultCursor(Cursor cursor) {
        super(false);
        this.cursor = cursor;
    }

    @Override
    public Row doNext() {
        if (closed) {
            return null;
        }
        Row row = cursor.next();
        if (row != null && cursorMeta != null) {
            row.setCursorMeta(cursorMeta);
        }
        return row;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (closed) {
            return exceptions;
        }
        closed = true;
        return cursor.close(exceptions);
    }

    public void setCursorMeta(CursorMeta cursorMeta) {
        if (null != cursorMeta) {
            this.returnColumns = cursorMeta.getColumns();
            this.cursorMeta = cursorMeta;
        }
    }

    public Cursor getCursor() {
        return cursor;
    }

}
