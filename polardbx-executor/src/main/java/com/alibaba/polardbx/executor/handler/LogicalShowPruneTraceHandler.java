package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.ColumnarPruneRecord;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

/**
 * @author jilong.ljl
 */
public class LogicalShowPruneTraceHandler extends HandlerCommon {
    public LogicalShowPruneTraceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("TRACE");
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("FILTER", DataTypes.StringType);
        result.addColumn("INIT_TIME(NS)", DataTypes.StringType);
        result.addColumn("PRUNE_TIME(NS)", DataTypes.StringType);
        result.addColumn("FILE_NUM", DataTypes.StringType);
        result.addColumn("STRIPE_NUM", DataTypes.StringType);
        result.addColumn("RG_NUM", DataTypes.StringType);
        result.addColumn("RG_LEFT_NUM", DataTypes.StringType);
        result.addColumn("SORT_KEY_PRUNE_NUM", DataTypes.StringType);
        result.addColumn("ZONE_MAP_PRUNE_NUM", DataTypes.StringType);
        result.addColumn("BITMAP_PRUNE_NUM", DataTypes.StringType);

        result.initMeta();

        Collection<ColumnarPruneRecord> ops = null;
        if (executionContext.getColumnarTracer() != null) {
            ops = executionContext.getColumnarTracer().pruneRecords();
            for (ColumnarPruneRecord op : ops) {
                result.addRow(new Object[] {
                    op.getTableName(),
                    op.getFilter(),
                    op.initIndexTime,
                    op.indexPruneTime,
                    op.fileNum,
                    op.stripeNum,
                    op.rgNum,
                    op.rgLeftNum,
                    op.sortKeyPruneNum,
                    op.zoneMapPruneNum,
                    op.bitMapPruneNum});
            }
        }
        return result;
    }
}
