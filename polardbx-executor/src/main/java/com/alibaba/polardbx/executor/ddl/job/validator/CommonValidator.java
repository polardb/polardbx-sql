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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

public class CommonValidator {

    public static void validateDdlJob(String schemaName, String logicalTableName, DdlJob ddlJob, Logger logger,
                                      ExecutionContext executionContext) {
        if (ddlJob == null) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job: The Job is null");
        }
        if (!ddlJob.isValid()) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job in which the task topology has cycles");
        }
        if (ddlJob.getTaskCount() == 0) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job: The Job has no task");
        }

        // Check the capacity of the DDL Engine system table.
        validateDdlJobCapacity(schemaName);
    }

    private static void validateDdlJobCapacity(String schemaName) {
        DdlJobManager engineManager = new DdlJobManager();
        int leftJobCount = engineManager.countAll(schemaName);
        if (leftJobCount > DdlConstants.MAX_LEFT_DDL_JOBS_IN_SYS_TABLE) {
            throw new TddlRuntimeException(ErrorCode.ERR_PENDING_DDL_JOBS_EXCEED_LIMIT,
                String.valueOf(DdlConstants.MAX_LEFT_DDL_JOBS_IN_SYS_TABLE));
        }
    }

}
