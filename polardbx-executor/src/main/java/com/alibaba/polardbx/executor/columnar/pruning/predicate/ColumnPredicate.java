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

package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;

/**
 * @author fangwu
 */
public abstract class ColumnPredicate implements ColumnPredicatePruningInf {

    // column type, might be null
    protected SqlTypeName type;
    protected int colId;

    public ColumnPredicate(SqlTypeName type, int colId) {
        this.type = type;
        this.colId = colId;
    }

    protected Object getArg(@NotNull DataType dataType, SqlTypeName typeName, int paramIndex, Object constant,
                            @NotNull IndexPruneContext ipc) {
        if (constant == null) {
            return ipc.acquireFromParameter(paramIndex, dataType, typeName);
        } else {
            return constant;
        }
    }

    protected Object[] getArgs(@NotNull DataType dataType, SqlTypeName typeName, int paramIndex,
                               @NotNull IndexPruneContext ipc) {
        return ipc.acquireArrayFromParameter(paramIndex, dataType, typeName);
    }

}
