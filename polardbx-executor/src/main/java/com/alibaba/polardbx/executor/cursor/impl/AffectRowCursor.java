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
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;

import java.util.List;

/**
 * Created by minggong.zm on 18/1/22.
 */
public class AffectRowCursor extends AbstractCursor {

    private int[] affectRows;
    private int index = -1;
    private boolean schemaInited = false;
    private CursorMeta cursormeta;

    public AffectRowCursor(int... affectRows) {
        super(false);
        if (affectRows == null) {
            this.affectRows = new int[0];
        } else {
            this.affectRows = affectRows;
        }
    }

    private CursorMeta initSchema() {
        if (schemaInited) {
            return cursormeta;
        }
        schemaInited = true;
        CursorMeta cursurMetaImp = CalciteUtils.buildDmlCursorMeta();
        this.cursormeta = cursurMetaImp;
        return cursurMetaImp;
    }

    @Override
    public Row doNext() {
        initSchema();
        if (++index < affectRows.length) {
            ArrayRow arrayRow = new ArrayRow(1, cursormeta);
            arrayRow.setObject(0, affectRows[index]);
            arrayRow.setCursorMeta(cursormeta);
            return arrayRow;
        } else {
            return null;
        }
    }

    public int[] getAffectRows() {
        return affectRows;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        return exceptions;
    }
}
