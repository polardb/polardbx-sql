package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * Initialize physical database for partition-table
 * 1. Create physical database
 * 2. Copy existed broadcast tables
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionInitPartitionDb implements BalanceAction {

    private String schema;

    public ActionInitPartitionDb(String schema) {
        this.schema = schema;
    }

    public static String getActionName() {
        return "ActionInitPartitionDb";
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return getActionName();
    }

    @Override
    public String getStep() {
        return "refresh topology";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        final String sqlRefreshTopology = "refresh topology";
        return ActionUtils.convertToDelegatorJob(ec, schema, sqlRefreshTopology);
    }
}
