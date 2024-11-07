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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.function.BiFunction;

public abstract class IndexExecutor {
    protected BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;
    protected String schemaName;

    public IndexExecutor(BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                         String schemaName) {
        this.executeFunc = executeFunc;
        this.schemaName = schemaName;
    }
}
