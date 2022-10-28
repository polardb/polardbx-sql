package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Map;

public class FetchFunctionCacheSyncAction implements ISyncAction {
    public FetchFunctionCacheSyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("FUNCTION_CACHE");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("FUNCTION", DataTypes.StringType);
        result.addColumn("SIZE", DataTypes.LongType);

        Map<String, Long> loadedFunctions = StoredFunctionManager.getInstance().getFunctions();
        for (Map.Entry<String, Long> function : loadedFunctions.entrySet()) {
            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                function.getKey(),
                function.getValue()
            });
        }
        return result;
    }
}
