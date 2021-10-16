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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

/**
 * ExecutorCursor is an adaptor converting Executor to ISchematicCursor
 */
public class ExecutorCursor extends AbstractCursor {

    private final Executor executor;
    private CursorMeta cursorMeta;

    private boolean inited = false;
    private Chunk chunk;
    private int position;

    public ExecutorCursor(Executor executor, CursorMeta cursorMeta) {
        super(false);
        this.executor = executor;
        this.cursorMeta = cursorMeta;
        this.returnColumns = cursorMeta.getColumns();
    }

    @Override
    public Row doNext() {
        if (!inited) {
            executor.open();
            inited = true;
        }

        while (true) {
            if (chunk == null) {
                chunk = executor.nextChunk();
                if (chunk == null) {
                    return null;
                }
            }

            if (position < chunk.getPositionCount()) {
                Row row = chunk.rowAt(position++);
                row.setCursorMeta(cursorMeta);
                return row;
            } else {
                chunk = null;
                position = 0;
            }
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        try {
            executor.close();
        } catch (Throwable e) {
            exceptions.add(e);
        }
        return exceptions;
    }

    public Executor getExecutor() {
        return executor;
    }
}
