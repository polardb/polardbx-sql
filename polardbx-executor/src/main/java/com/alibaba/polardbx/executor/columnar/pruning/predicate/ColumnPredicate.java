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
