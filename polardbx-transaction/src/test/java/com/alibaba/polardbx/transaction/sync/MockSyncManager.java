package com.alibaba.polardbx.transaction.sync;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockSyncManager extends AbstractLifecycle implements ISyncManager {
    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                                boolean throwExceptions) {
        return Collections.singletonList(ExecUtils.resultSetToList((ResultCursor) action.sync()));
    }

    @Override
    public void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
                     boolean throwExceptions) {
        action.sync();
    }

    @Override
    public List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {
        return ExecUtils.resultSetToList((ResultCursor) action.sync());
    }
}
