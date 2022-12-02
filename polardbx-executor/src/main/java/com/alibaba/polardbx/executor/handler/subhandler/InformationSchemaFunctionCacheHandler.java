package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchFunctionCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFunctionCache;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

public class InformationSchemaFunctionCacheHandler extends BaseVirtualViewSubClassHandler{
    public InformationSchemaFunctionCacheHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaFunctionCache;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new FetchFunctionCacheSyncAction(),
            TddlConstants.INFORMATION_SCHEMA);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                final String host = DataTypes.StringType.convertFrom(row.get("ID"));
                final String function = DataTypes.StringType.convertFrom(row.get("FUNCTION"));
                final Long size = DataTypes.LongType.convertFrom(row.get("SIZE"));
                cursor.addRow(new Object[] {
                    host,
                    function,
                    size
                });
            }
        }
        return cursor;
    }
}
