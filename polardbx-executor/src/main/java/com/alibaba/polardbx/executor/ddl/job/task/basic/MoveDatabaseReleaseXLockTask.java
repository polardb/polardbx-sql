/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
                "error occurs while MoveDatabaseReleaseXLock, schemaName:%s, schemaXLockToRelease:%s", schemaName,
                targetSchemaName));
            throw GeneralUtil.nestedException(e);
        }
    }
}
