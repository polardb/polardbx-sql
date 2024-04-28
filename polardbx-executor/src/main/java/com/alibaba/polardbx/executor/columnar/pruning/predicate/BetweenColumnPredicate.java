package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;

/**
 * @author fangwu
 */
public class BetweenColumnPredicate extends ColumnPredicate {

    private final int paramIndex1;
    private final Object paramObj1;
    private final int paramIndex2;
    private final Object paramObj2;

    public BetweenColumnPredicate(SqlTypeName type, int colId, int paramIndex1, int paramIndex2, Object paramObj1,
                                  Object paramObj2) {
        super(type, colId);
        this.paramIndex1 = paramIndex1;
        this.paramIndex2 = paramIndex2;
        this.paramObj1 = paramObj1;
        this.paramObj2 = paramObj2;
    }

    @Override
    public void sortKey(@NotNull SortKeyIndex sortKeyIndex, @NotNull IndexPruneContext ipc,
                        @NotNull RoaringBitmap cur) {
        if (!sortKeyIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg1 = getArg(sortKeyIndex.getColumnDataType(colId), type, paramIndex1, paramObj1, ipc);
        Object arg2 = getArg(sortKeyIndex.getColumnDataType(colId), type, paramIndex2, paramObj2, ipc);

        if (arg1 == null && arg2 == null) {
            return;
        }
        sortKeyIndex.pruneRange(arg1, arg2, cur);
    }

    @Override
    public void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                       @NotNull RoaringBitmap cur) {
        if (!bitMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg1 = getArg(bitMapIndex.getColumnDataType(colId), type, paramIndex1, paramObj1, ipc);
        Object arg2 = getArg(bitMapIndex.getColumnDataType(colId), type, paramIndex2, paramObj2, ipc);

        if (arg1 == null && arg2 == null) {
            return;
        }
        bitMapIndex.between(colId, arg1, true, arg2, true, cur);
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!zoneMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object arg1 = getArg(zoneMapIndex.getColumnDataType(colId), type, paramIndex1, paramObj1, ipc);
        Object arg2 = getArg(zoneMapIndex.getColumnDataType(colId), type, paramIndex2, paramObj2, ipc);

        if (arg1 == null && arg2 == null) {
            return;
        }
        zoneMapIndex.prune(colId, arg1, true, arg2, true, cur);
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                            @NotNull RoaringBitmap cur) {
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        Preconditions.checkArgument(columns != null && columns.length > colId, "error column meta");
        // get args
        Object arg1 = getArg(DataTypes.StringType, SqlTypeName.VARCHAR, paramIndex1, null, ipc);
        Object arg2 = getArg(DataTypes.StringType, SqlTypeName.VARCHAR, paramIndex2, null, ipc);
        StringBuilder sb = new StringBuilder();
        sb.append(columns[colId])
            .append("_")
            .append(colId)
            .append(" BETWEEN ")
            .append(arg1)
            .append(" ")
            .append(arg2);

        return sb;
    }

}
