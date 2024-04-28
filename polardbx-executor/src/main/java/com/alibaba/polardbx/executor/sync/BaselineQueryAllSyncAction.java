package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

public class BaselineQueryAllSyncAction implements ISyncAction {

    public BaselineQueryAllSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        String jsonString = PlanManager.getBaselineAsJson(PlanManager.getInstance().getBaselineMap());

        ArrayResultCursor result = new ArrayResultCursor("baselines");
        result.addColumn("inst_id", DataTypes.StringType);
        result.addColumn("baselines", DataTypes.StringType);
        result.initMeta();
        String instId = ServerInstIdManager.getInstance().getInstId();

        result.addRow(new Object[] {instId, jsonString});
        return result;
    }
}

