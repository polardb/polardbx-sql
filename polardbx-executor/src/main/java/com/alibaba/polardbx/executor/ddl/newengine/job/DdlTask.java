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

package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.util.List;

public interface DdlTask {

    Long getJobId();

    void setJobId(Long jobId);

    Long getTaskId();

    void setTaskId(Long taskId);

    Long getRootJobId();

    void setRootJobId(Long rootJobId);

    /**
     * Get the task name.
     * used for Serialize/DeSerialize.
     * Don't ever even try to change its value
     *
     * @return The task name
     */
    String getName();

    String getSchemaName();

    void setSchemaName(String schemaName);

    /**
     * Execute the DDL task.
     */
    void execute(ExecutionContext executionContext);

    /**
     * Rollback the DDL task.
     */
    void rollback(ExecutionContext executionContext);

    /**
     * handle error after DDL task execute failed
     */
    void handleError(ExecutionContext executionContext);

    /**
     * Get the exception action that DDL Engine should do after an exception occurs.
     *
     * @return An exception action
     */
    DdlExceptionAction getExceptionAction();

    /**
     * Set an exception action that DDL Engine should do after an exception occurs.
     *
     * @param exceptionAction An exception action
     */
    void setExceptionAction(DdlExceptionAction exceptionAction);

    /**
     * another form of 'setExceptionAction'
     *
     * @return current task
     */
    DdlTask onExceptionTryRecoveryThenPause();

    /**
     * another form of 'setExceptionAction'
     *
     * @return current task
     */
    DdlTask onExceptionTryRollback();

    /**
     * another form of 'setExceptionAction'
     *
     * @return current task
     */
    DdlTask onExceptionTryRecoveryThenRollback();

    /**
     * @param ddlTaskState DdlTaskState
     * @see DdlTaskState
     */
    void setState(DdlTaskState ddlTaskState);

    DdlTaskState getState();

    /**
     * for visualization
     */
    String nodeInfo();

    List<String> explainInfo();

    String executionInfo();

    /**
     * set progress for current DDL JOB
     */
    void updateProgress(int progress, Connection connection);

    /**
     * set supported commands for current DDL JOB
     *
     * @param supportContinue whether support: continue ddl ....
     * @param supportCancel whether support: cancel ddl ...
     * @param connection create a new txn if connection is null
     */
    void updateSupportedCommands(boolean supportContinue,
                                 boolean supportCancel,
                                 Connection connection);
}
