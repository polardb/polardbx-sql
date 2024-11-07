package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;

/**
 * @author wumu
 */
public class FailPointDisableSyncAction implements ISyncAction {
    private String fpKey;

    public FailPointDisableSyncAction(String fpKey) {
        this.fpKey = fpKey;
    }

    @Override
    public ResultCursor sync() {
        FailPoint.disable(fpKey);
        return null;
    }

    public String getFpKey() {
        return fpKey;
    }

    public void setFpKey(String fpKey) {
        this.fpKey = fpKey;
    }
}
