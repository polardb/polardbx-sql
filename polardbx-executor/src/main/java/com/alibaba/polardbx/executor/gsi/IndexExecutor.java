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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public abstract class IndexExecutor {

    public static final int MAX_UPDATE_NUM_IN_GSI_DEFAULT = 10000;

    protected BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;
    protected String schemaName;

    public IndexExecutor(BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                         String schemaName) {
        this.executeFunc = executeFunc;
        this.schemaName = schemaName;
    }

    protected List<List<Object>> getSelectResults(List<RelNode> selects, ExecutionContext executionContext) {
        long selectLimit = executionContext.getParamManager().getLong(ConnectionParams.MAX_UPDATE_NUM_IN_GSI);

        List<Cursor> cursors = executeFunc.apply(selects, executionContext);
        List<List<Object>> selectedObjects = new ArrayList<>();
        try {
            for (int i = 0; i < cursors.size(); i++) {
                Cursor cursor = cursors.get(i);
                Row row;
                while ((row = cursor.next()) != null && selectedObjects.size() <= selectLimit) {
                    selectedObjects.add(row.getValues());
                }
                cursor.close(new ArrayList<>());
                cursors.set(i, null);
            }
        } finally {
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }

        if (selectedObjects.size() > selectLimit) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_UPDATE_NUM_EXCEEDED,
                String.valueOf(selectLimit));
        }

        return selectedObjects;
    }
}
