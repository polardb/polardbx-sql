package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "TopologySyncThenReleaseXLockTask")
public class TopologySyncThenReleaseXLockTask extends TopologySyncTask {

    private String schemaXLockToRelease;
    public TopologySyncThenReleaseXLockTask(String schemaName, String schemaXLockToRelease) {
        super(schemaName);
        this.schemaXLockToRelease = schemaXLockToRelease;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        super.executeImpl(executionContext);
        try (Connection connection = MetaDbUtil.getConnection()) {
            DdlJobManager ddlJobManager = new DdlJobManager();
            boolean downGradeResult =
                ddlJobManager.getResourceManager().downGradeWriteLock(connection, getJobId(), schemaXLockToRelease);
            if (downGradeResult) {
                LOGGER.info(String.format("DownGrade Persistent Write Lock [%s] Success", schemaXLockToRelease));
            }
        } catch (Exception e) {
            LOGGER.error(String.format(
                "error occurs while TopologySyncThenReleaseXLockTask, schemaName:%s, schemaXLockToRelease:%s", schemaName, schemaXLockToRelease));
            throw GeneralUtil.nestedException(e);
        }
    }
}