package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESSING;
import static com.alibaba.polardbx.gms.module.Module.SPM;

public class BaselineDeleteSyncAction implements ISyncAction {

    private String schemaName;

    private Integer baselineId;

    private Integer planInfoId;

    public BaselineDeleteSyncAction(String schemaName, Integer baselineId) {
        this.schemaName = schemaName;
        this.baselineId = baselineId;
        this.planInfoId = null;
    }

    public BaselineDeleteSyncAction(String schemaName, Integer baselineId, int planInfoId) {
        this.schemaName = schemaName;
        this.baselineId = baselineId;
        this.planInfoId = planInfoId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        if (planInfoId != null && planInfoId != 0) {
            ModuleLogInfo.getInstance()
                .logRecord(SPM, PROCESSING, new String[] {"delete plan " + planInfoId, baselineId + ""}, NORMAL);
            PlanManager.getInstance().deleteBaselinePlan(schemaName, baselineId, planInfoId);
        } else {
            ModuleLogInfo.getInstance()
                .logRecord(SPM, PROCESSING, new String[] {"delete baseline ", baselineId + ""}, NORMAL);
            PlanManager.getInstance().deleteBaseline(schemaName, baselineId);
        }
        return null;
    }

    public Integer getPlanInfoId() {
        return planInfoId;
    }

    public void setPlanInfoId(Integer planInfoId) {
        this.planInfoId = planInfoId;
    }

    public Integer getBaselineId() {
        return baselineId;
    }

    public void setBaselineId(Integer baselineId) {
        this.baselineId = baselineId;
    }
}

