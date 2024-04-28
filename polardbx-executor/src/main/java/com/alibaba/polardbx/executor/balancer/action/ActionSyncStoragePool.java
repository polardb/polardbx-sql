package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.model.privilege.DbInfo;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDrainNode;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.ddl.job.task.rebalance.SyncStoragePoolTask;
import com.alibaba.polardbx.executor.ddl.job.task.rebalance.WriteDataDistLogTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Objects;

/**
 * Action that just lock some resource
 *
 * @author taokun
 * @since 2021/10
 */
public class ActionSyncStoragePool implements BalanceAction {

    private PolicyDrainNode.DrainNodeInfo drainNodeInfo;

    public ActionSyncStoragePool(PolicyDrainNode.DrainNodeInfo drainNodeInfo) {
        if (drainNodeInfo != null) {
            this.drainNodeInfo = drainNodeInfo;
        } else {
            this.drainNodeInfo = new PolicyDrainNode.DrainNodeInfo();
        }
    }

    @Override
    public String getSchema() {
        return DefaultDbSchema.NAME;
    }

    @Override
    public String getName() {
        return "ActionSyncStoragePool";
    }

    @Override
    public String getStep() {
        return "ActionSyncStoragePool";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        SyncStoragePoolTask task = new SyncStoragePoolTask(drainNodeInfo.getDnInstIdList());
        job.addTask(task);
        job.labelAsHead(task);
        job.labelAsTail(task);
        return job;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionSyncStoragePool)) {
            return false;
        }
        ActionSyncStoragePool that = (ActionSyncStoragePool) o;
        return Objects.equals(drainNodeInfo, that.drainNodeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(drainNodeInfo);
    }

    @Override
    public String toString() {
        return "ActionSyncStoragePool{" +
            "schema='" + getSchema() + '\'' +
            ", drainNodeInfo='" + JSON.toJSONString(drainNodeInfo) + '\'' +
            '}';
    }
}