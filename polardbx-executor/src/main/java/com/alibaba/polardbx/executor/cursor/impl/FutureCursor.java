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
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 异步cursor
 *
 * @author agapple 2014年9月19日 上午11:11:16
 * @since 5.1.13
 */
public class FutureCursor extends AbstractCursor {

    private Future<Cursor> future;
    private Cursor cursor;

    public FutureCursor(Future<Cursor> future) {
        super(false);
        this.future = future;
    }

    public Cursor get() {
        try {
            if (cursor == null) {
                cursor = future.get();
            }
            return cursor;
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (future != null) {
            Throwable ex = null;
            try {
                cursor = future.get();
            } catch (InterruptedException | ExecutionException e) {
                ex = e;
            }
            if (cursor != null) {
                return cursor.close(exceptions);
            } else {
                exceptions.add(ex);
                return exceptions;
            }
        } else {
            return exceptions;
        }
    }

    @Override
    public Row doNext() {
        return get().next();
    }
}
