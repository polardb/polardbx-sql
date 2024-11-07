package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class BaselineDeleteHotEvolvedSyncAction implements ISyncAction {

    String schema;

    public BaselineDeleteHotEvolvedSyncAction(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {
        PlanManager.getInstance().deleteBaselineEvolved(schema);
        return null;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

}

