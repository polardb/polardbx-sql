package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;

/**
 * @author wumu
 */
public class FailPointClearSyncAction implements ISyncAction {
    @Override
    public ResultCursor sync() {
        FailPoint.clear();
        return null;
    }
}
