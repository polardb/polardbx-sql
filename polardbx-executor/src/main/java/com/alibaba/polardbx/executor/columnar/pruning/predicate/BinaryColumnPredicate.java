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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;

/**
 * @author fangwu
 */
public class BinaryColumnPredicate extends ColumnPredicate {

    private final SqlKind operator;
    private final int paramIndex;
    private final Object param;

    public BinaryColumnPredicate(SqlTypeName type, int colId, SqlKind operator, int paramIndex) {
        super(type, colId);
        checkOperator(operator);
        this.operator = operator;
        this.paramIndex = paramIndex;
        this.param = null;
    }

    public BinaryColumnPredicate(SqlTypeName type, int colId, SqlKind operator, Object param) {
        super(type, colId);
        checkOperator(operator);
        this.operator = operator;
        this.param = param;
        this.paramIndex = -1;
    }

    private void checkOperator(SqlKind operator) {
        switch (operator) {
        case EQUALS:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            return;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BINARY_PREDICATE,
                "not support operator for BinaryColumnPredicate");
        }
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        Preconditions.checkArgument(columns != null && columns.length > colId, "error column meta");
        // get arg
        Object arg = getArg(DataTypes.StringType, SqlTypeName.VARCHAR, paramIndex, param, ipc);

        StringBuilder sb = new StringBuilder();
        sb.append(columns[colId])
            .append("_")
            .append(colId)
            .append(" " + operator.name() + " ")
            .append(arg);
        return sb;
    }

    @Override
    public void sortKey(@NotNull SortKeyIndex sortKeyIndex,
                        IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!sortKeyIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg = getArg(sortKeyIndex.getColumnDataType(colId), type, paramIndex, param, ipc);

        if (arg == null) {
            return;
        }
        switch (operator) {
        case EQUALS:
            sortKeyIndex.pruneRange(arg, arg, cur);
            return;
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
            sortKeyIndex.pruneRange(null, arg, cur);
            return;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            sortKeyIndex.pruneRange(arg, null, cur);
            return;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BINARY_PREDICATE,
                "not support operator for BinaryColumnPredicate");
        }

    }

    @Override
    public void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                       @NotNull RoaringBitmap cur) {
        if (!bitMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg = getArg(bitMapIndex.getColumnDataType(colId), type, paramIndex, param, ipc);

        if (arg == null) {
            return;
        }
        switch (operator) {
        case EQUALS:
            bitMapIndex.pruneEquals(colId, arg, cur);
            return;
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
            bitMapIndex.between(colId, null, true, arg, true, cur);
            return;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            bitMapIndex.between(colId, arg, true, null, true, cur);
            return;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BINARY_PREDICATE,
                "not support operator for BinaryColumnPredicate");
        }
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!zoneMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg = getArg(zoneMapIndex.getColumnDataType(colId), type, paramIndex, param, ipc);

        if (arg == null) {
            return;
        }
        switch (operator) {
        case EQUALS:
            zoneMapIndex.prune(colId, arg, true, arg, true, cur);
            return;
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
            zoneMapIndex.prune(colId, null, true, arg, true, cur);
            return;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
            zoneMapIndex.prune(colId, arg, true, null, true, cur);
            return;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_BINARY_PREDICATE,
                "not support operator for BinaryColumnPredicate");
        }
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex,
                            IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
    }

    public int getParamIndex() {
        return paramIndex;
    }
}
