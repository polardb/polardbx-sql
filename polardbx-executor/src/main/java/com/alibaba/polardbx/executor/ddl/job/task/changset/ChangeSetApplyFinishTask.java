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

package com.alibaba.polardbx.executor.ddl.job.task.changset;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutor;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutorMap;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author wumu
 */
@TaskName(name = "ChangeSetApplyFinishTask")
@Getter
public class ChangeSetApplyFinishTask extends BaseBackfillTask {

    private String msg;

    public ChangeSetApplyFinishTask(String schemaName, String msg) {
        super(schemaName);
        this.msg = msg;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        ChangeSetApplyExecutor changeSetApplyExecutor = ChangeSetApplyExecutorMap.get(schemaName, jobId);
        if (changeSetApplyExecutor == null) {
            SQLRecorderLogger.scaleOutTaskLogger.info(
                "ChangeSetApplyFinishTask: The job '" + jobId + "' can not find the changeset apply executor");
        } else if (!changeSetApplyExecutor.waitAndStop()) {
            String msg = changeSetApplyExecutor.getException().get().getMessage();
            String interruptMsg = "The job '" + jobId + "' has been cancelled";
            if (msg.contains(interruptMsg)) {
                SQLRecorderLogger.scaleOutTaskLogger.info(
                    "The job '" + jobId
                        + "' has been cancelled, so apply task will not add to changeset apply executor");
                return;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' stop changeset apply executor failed, " + msg);
        }

        ChangeSetApplyExecutorMap.remove(schemaName, jobId);

        Date currentTime = Calendar.getInstance().getTime();
        SQLRecorderLogger.scaleOutTaskLogger.info(String.format("jobId: %s; msg: %s; timestamp: %s; time: %s",
            this.getJobId(), msg, currentTime.getTime(),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentTime)));
    }
}
