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
public class InColumnPredicate extends ColumnPredicate {

    private final int paramIndex;

    public InColumnPredicate(SqlTypeName type, int colId, int paramIndex) {
        super(type, colId);
        this.paramIndex = paramIndex;
    }

    @Override
    public StringBuilder display(String[] columns, IndexPruneContext ipc) {
        Preconditions.checkArgument(columns != null && columns.length > colId, "error column meta");
        // get arg
        Object arg = getArg(DataTypes.StringType, SqlTypeName.VARCHAR, paramIndex, null, ipc);

        StringBuilder sb = new StringBuilder();
        sb.append(columns[colId])
            .append("_")
            .append(colId)
            .append(" IN ")
            .append("(" + arg + ")");
        return sb;
    }

    @Override
    public void sortKey(@NotNull SortKeyIndex sortKeyIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!sortKeyIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object[] args = getArgs(sortKeyIndex.getColumnDataType(colId), type, paramIndex, ipc);

        if (args == null) {
            return;
        }
        RoaringBitmap rb = new RoaringBitmap();
        for (Object arg : args) {
            RoaringBitmap tmp = cur.clone();
            sortKeyIndex.pruneEqual(arg, tmp);
            rb.or(tmp);
        }
        cur.and(rb);
    }

    @Override
    public void bitmap(@Nonnull @NotNull BitMapRowGroupIndex bitMapIndex, IndexPruneContext ipc,
                       @NotNull RoaringBitmap cur) {
        if (!bitMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object[] args = getArgs(bitMapIndex.getColumnDataType(colId), type, paramIndex, ipc);

        if (args == null) {
            return;
        }

        RoaringBitmap rb = new RoaringBitmap();
        for (Object arg : args) {
            RoaringBitmap tmp = cur.clone();
            bitMapIndex.pruneEquals(colId, arg, tmp);
            rb.or(tmp);
        }
        cur.and(rb);
    }

    @Override
    public void zoneMap(@NotNull ZoneMapIndex zoneMapIndex, IndexPruneContext ipc, @NotNull RoaringBitmap cur) {
        if (!zoneMapIndex.checkSupport(colId, type)) {
            return;
        }
        // get args
        Object[] args = getArgs(zoneMapIndex.getColumnDataType(colId), type, paramIndex, ipc);

        if (args == null) {
            return;
        }

        RoaringBitmap rb = new RoaringBitmap();
        for (Object arg : args) {
            RoaringBitmap tmp = cur.clone();
            zoneMapIndex.prune(colId, arg, true, arg, true, tmp);
            rb.or(tmp);
        }
        cur.and(rb);
    }

    @Override
    public void bloomFilter(@NotNull BloomFilterIndex bloomFilterIndex, IndexPruneContext ipc,
                            @NotNull RoaringBitmap cur) {
        // TODO support bloom filter
    }

}
