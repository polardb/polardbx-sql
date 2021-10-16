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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

/**
 * Created by chuanqin on 17/7/13.
 */
public class LogicalValueCursor extends AbstractCursor {
    private final int count;
    private int current;

    public LogicalValueCursor(int count, List<ColumnMeta> columns) {
        super(false);
        this.returnColumns = columns;
        this.count = count;
        current = 0;
    }

    @Override
    public Row doNext() {
        if (current < count) {
            current++;
            return new ArrayRow(1, null);
        } else {
            return null;
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        return exceptions;
    }
}
