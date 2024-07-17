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
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 17/7/24.
 */
public class GatherCursor extends AbstractCursor {

    private final List<Cursor> cursors;
    private final ExecutionContext executionContext;
    private List<Throwable> exceptionsWhenCloseSubCursor = new ArrayList<>();

    private Cursor currentCursor;
    private int currentIndex = 0;

    public GatherCursor(List<Cursor> cursors, ExecutionContext executionContext) {
        super(ExecUtils.isOperatorMetricEnabled(executionContext));
        this.executionContext = executionContext;
        this.cursors = cursors;
        this.returnColumns = cursors.get(0).getReturnColumns();
    }

    @Override
    public Row doNext() { // commented codes are for async scheme
        init();
        Row ret;
        while (true) {
            if (currentIndex >= cursors.size()) { // 取尽所有cursor.
                return null;
            }
            if (currentCursor == null) {
                currentCursor = cursors.get(currentIndex);
            }
            try {
                ret = FunctionUtils.fromIRowSetToArrayRowSet(currentCursor.next());
                if (ret != null) {
                    return ret;
                } else {
                    switchCursor();
                }
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        exceptions.addAll(exceptionsWhenCloseSubCursor);
        for (Cursor cursor : cursors) {
            exceptions = cursor.close(exceptions);
        }
        return exceptions;
    }

    private void switchCursor() {
        currentCursor.close(exceptionsWhenCloseSubCursor);
        currentIndex++;
        currentCursor = null;
    }

    public List<Cursor> getCursors() {
        return cursors;
    }
}
