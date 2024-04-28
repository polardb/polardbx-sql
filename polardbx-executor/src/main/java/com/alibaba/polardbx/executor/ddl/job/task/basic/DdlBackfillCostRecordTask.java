package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import lombok.Getter;
import lombok.Setter;

@TaskName(name = "DdlBackfillCostRecordTask")
@Getter
@Setter
public final class DdlBackfillCostRecordTask extends BaseDdlTask implements CostEstimableDdlTask {

    @JSONCreator
    public DdlBackfillCostRecordTask(String schemaName) {
        super(schemaName);
        setExceptionAction(DdlExceptionAction.ROLLBACK);
    }

    @Override
    public String remark() {
        String costInfoStr = "";
        if (costInfo != null) {
            costInfoStr = String.format("|estimated rows:%s, estimated size:%s", costInfo.rows, costInfo.dataSize);
        }
        return costInfoStr;
    }

    public static String getTaskName() {
        return "DdlBackfillCostRecordTask";
    }

    private transient volatile CostInfo costInfo;

    @Override
    public void setCostInfo(CostInfo costInfo) {
        this.costInfo = costInfo;
    }

    @Override
    public CostInfo getCostInfo() {
        return costInfo;
    }
}
