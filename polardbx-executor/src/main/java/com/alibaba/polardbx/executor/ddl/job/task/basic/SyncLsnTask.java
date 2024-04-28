package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;

/**
 *
 */
@Getter
@TaskName(name = "SyncLsnTask")
public class SyncLsnTask extends BaseGmsTask {

    final Map<String, String> targetGroupAndStorageIdMap;
    final Map<String, String> sourceGroupAndStorageIdMap;

    @JSONCreator
    public SyncLsnTask(String schemaName, Map<String, String> sourceGroupAndStorageIdMap,
                       Map<String, String> targetGroupAndStorageIdMap) {
        super(schemaName, "");
        this.sourceGroupAndStorageIdMap = sourceGroupAndStorageIdMap;
        this.targetGroupAndStorageIdMap = targetGroupAndStorageIdMap;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        //make sure the table schema in the follower node is the same as source leader node
        PhysicalBackfillUtils.waitLsn(schemaName, sourceGroupAndStorageIdMap, false, executionContext);
        //make sure the create table/discard tablespace has been executed in follower/learner node
        PhysicalBackfillUtils.waitLsn(schemaName, targetGroupAndStorageIdMap, false, executionContext);
    }

    protected void onRollbackSuccess(ExecutionContext executionContext) {

    }
}

