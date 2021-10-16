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

import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

import static com.alibaba.polardbx.optimizer.utils.CalciteUtils.buildDmlCursorMeta;

/**
 * Created by minggong.zm on 18/1/22.
 */
public class AffectRowSumCursor extends AbstractCursor {

    private boolean schemaInited = false;
    private CursorMeta cursorMeta;
    private List<Cursor> cursors;
    private boolean isFinished = false;
    private boolean isConcurrent = false;
    private long limitCount;

    public AffectRowSumCursor(List<Cursor> cursors, boolean isConcurrent, long limitCount) {
        super(false);
        this.cursors = cursors;
        this.isConcurrent = isConcurrent;
        this.limitCount = limitCount;
        initSchema();
    }

    private CursorMeta initSchema() {
        if (schemaInited) {
            return cursorMeta;
        }
        schemaInited = true;
        this.cursorMeta = buildDmlCursorMeta();
        return cursorMeta;
    }

    @Override
    public Row doNext() {
        if (isFinished || (cursors != null && cursors.size() == 0)) {
            return null;
        }

        long affectRows = 0;
        /**
         * <pre>
         * For table that has extraDb, isConcurrent = false and limitCount > 0,
         * have and only have TWO input cursors.
         *
         * First call the left, then right.
         * </pre>
         */
        if (hasPriority()) {
            /**
             * First left
             */
            Cursor c = cursors.get(0);
            affectRows = getAffectedRows(c.next());
            if (affectRows < limitCount) {
                /**
                 * If left less then limitCount, then right
                 */
                c = cursors.get(1);
                affectRows += getAffectedRows(c.next());
            }
        } else {
            /**
             * All child cursors return the affected rows.
             */
            for (Cursor c : cursors) {
                affectRows += getAffectedRows(c.next());
            }
        }

        ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
        arrayRow.setObject(0, affectRows);
        arrayRow.setCursorMeta(cursorMeta);
        isFinished = true;
        return arrayRow;
    }

    private boolean hasPriority() {
        return isConcurrent == false && limitCount > 0 && cursors.size() == 2;
    }

    private long getAffectedRows(Row row) {
        if (row instanceof ArrayRow) {
            ArrayRow arrayRow = (ArrayRow) row;
            return arrayRow.getLong(0);
        }
        return 0L;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        return exceptions;
    }
}
