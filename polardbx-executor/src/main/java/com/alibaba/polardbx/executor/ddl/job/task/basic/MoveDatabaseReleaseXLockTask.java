package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/***
 * @author chenghui.lch
 */

@Getter
@TaskName(name = "MoveDatabaseReleaseXLockTask")
public class MoveDatabaseReleaseXLockTask extends BaseSyncTask {

    protected String targetSchemaName;
    public MoveDatabaseReleaseXLockTask(String schema, String targetSchemaName) {
        super(schema);
        this.targetSchemaName = targetSchemaName;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            DdlJobManager ddlJobManager = new DdlJobManager();
            boolean downGradeResult =
                ddlJobManager.getResourceManager().downGradeWriteLock(connection, getJobId(), targetSchemaName);
            if (downGradeResult) {
                LOGGER.info(String.format("DownGrade Persistent Write Lock [%s] Success", targetSchemaName));
            }
        } catch (Exception e) {
            LOGGER.error(String.format(
                "error occurs while MoveDatabaseReleaseXLock, schemaName:%s, schemaXLockToRelease:%s", schemaName, targetSchemaName));
            throw GeneralUtil.nestedException(e);
        }
    }
}
