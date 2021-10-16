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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

/**
 * Created by chuanqin on 17/8/3.
 */
public class MultiCursorAdapter extends AbstractCursor {

    public List<Cursor> getSubCursors() {
        return subCursors;
    }

    private List<Cursor> subCursors;

    public MultiCursorAdapter(List<Cursor> subCursors) {
        super(false);
        this.subCursors = subCursors;
    }

    public static Cursor wrap(List<Cursor> cursors) {
        return new MultiCursorAdapter(cursors);
    }

    @Override
    public Row doNext() {
        if (subCursors.size() != 1) {
            throw GeneralUtil.nestedException("cannot be invoked directly");
        }
        return subCursors.get(0).next();
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        for (Cursor cursor : subCursors) {
            exs = cursor.close(exs);
        }
        return exs;
    }

    @Override
    public List<ColumnMeta> getReturnColumns() {
        return subCursors.get(0).getReturnColumns();
    }
}
