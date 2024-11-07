package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;

/**
 * @author wumu
 */
public class FailPointEnableSyncAction implements ISyncAction {

    private String fpKey;

    private String value;

    public FailPointEnableSyncAction(String fpKey, String value) {
        this.fpKey = fpKey;
        this.value = value;
    }

    @Override
    public ResultCursor sync() {
        FailPoint.enable(fpKey, value);
        return null;
    }

    public String getFpKey() {
        return fpKey;
    }

    public String getValue() {
        return value;
    }

    public void setFpKey(String fpKey) {
        this.fpKey = fpKey;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
