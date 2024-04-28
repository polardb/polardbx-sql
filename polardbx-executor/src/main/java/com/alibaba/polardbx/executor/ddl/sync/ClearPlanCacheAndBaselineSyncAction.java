package com.alibaba.polardbx.executor.ddl.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import org.apache.commons.lang.StringUtils;

public class ClearPlanCacheAndBaselineSyncAction implements ISyncAction {

    private String schema;
    private String table;

    public ClearPlanCacheAndBaselineSyncAction() {
    }

    public ClearPlanCacheAndBaselineSyncAction(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public ResultCursor sync() {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(table)) {
            return null;
        }
        PlanManager.getInstance().invalidateTable(schema, table);
        return null;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
