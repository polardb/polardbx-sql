package com.alibaba.polardbx.executor.ddl.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class ClearPlanCacheSyncAction implements ISyncAction {

    private String schemaName;

    public ClearPlanCacheSyncAction() {
    }

    public ClearPlanCacheSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        if (optimizerContext != null) {
            PlanManager planManager = optimizerContext.getPlanManager();
            if (planManager != null) {
                planManager.cleanCache();
            }
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}

