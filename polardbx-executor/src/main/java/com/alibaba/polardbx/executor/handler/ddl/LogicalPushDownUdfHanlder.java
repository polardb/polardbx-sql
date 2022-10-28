package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.PushDownUdfTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class LogicalPushDownUdfHanlder extends LogicalCommonDdlHandler{
    public LogicalPushDownUdfHanlder(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext){
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(new PushDownUdfTask(logicalDdlPlan.getSchemaName()));
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.addExcludeResources(getPushDownLock());
        return executableDdlJob;
    }

    private Set<String> getPushDownLock() {
        TreeSet<String> pushDownLock = new TreeSet<String>();
        pushDownLock.add(PlConstants.PUSH_DOWN_UDF);
        return pushDownLock;
    }
}
