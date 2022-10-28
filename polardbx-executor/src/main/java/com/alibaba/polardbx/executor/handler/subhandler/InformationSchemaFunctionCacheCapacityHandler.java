package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchFunctionCacheCapacitySyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFunctionCacheCapacity;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaFunctionCacheCapacityHandler extends BaseVirtualViewSubClassHandler{
    public InformationSchemaFunctionCacheCapacityHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaFunctionCacheCapacity;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new FetchFunctionCacheCapacitySyncAction());

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                final String host = DataTypes.StringType.convertFrom(row.get("ID"));
                final long usedSize = DataTypes.LongType.convertFrom(row.get("USED_SIZE"));
                final long totalSize = DataTypes.LongType.convertFrom(row.get("TOTAL_SIZE"));

                cursor.addRow(new Object[] {
                    host,
                    usedSize,
                    totalSize
                });
            }
        }
        return cursor;
    }
}
