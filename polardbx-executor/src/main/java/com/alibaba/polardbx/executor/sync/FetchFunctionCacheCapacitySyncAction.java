package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class FetchFunctionCacheCapacitySyncAction implements ISyncAction{
    public FetchFunctionCacheCapacitySyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("FUNCTION_CACHE_CAPACITY");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("USED_SIZE", DataTypes.LongType);
        result.addColumn("TOTAL_SIZE", DataTypes.LongType);

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            StoredFunctionManager.getInstance().getUsedSize(),
            StoredFunctionManager.getInstance().getTotalSize()
        });

        return result;
    }
}
