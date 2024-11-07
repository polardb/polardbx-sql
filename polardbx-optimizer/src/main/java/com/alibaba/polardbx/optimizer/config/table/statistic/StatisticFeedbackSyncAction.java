package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

/**
 * @author jilong.ljl
 */
public class StatisticFeedbackSyncAction implements IGmsSyncAction {

    private String schema;

    private String table;

    public StatisticFeedbackSyncAction(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public Object sync() {
        if (LeaderStatusBridge.getInstance().hasLeadership()) {
            StatisticManager.getInstance().feedback(schema, table);
        }
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

