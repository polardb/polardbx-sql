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

package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlContinueDdlJob;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class DdlEngineContinueJobsHandler extends DdlEngineJobsHandler {

    public DdlEngineContinueJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlContinueDdlJob command = (SqlContinueDdlJob) logicalPlan.getNativeSqlNode();

        if (command.isAll()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on multi ddl jobs is not allowed");
        }

        if (command.getJobIds() == null || command.getJobIds().isEmpty()) {
            return new AffectRowCursor(0);
        }

        if (command.getJobIds().size() > 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on multi ddl jobs is not allowed");
        }

        return doContinue(command.getJobIds().get(0), executionContext);
    }

    public Cursor doContinue(Long jobId, ExecutionContext executionContext) {
        boolean enableOperateSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OPERATE_SUBJOB);
        DdlEngineRecord record = schedulerManager.fetchRecordByJobId(jobId);
        if (record == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "The ddl job does not exist");
        }

        DdlState state = DdlState.valueOf(record.state);
        if (!(state == DdlState.PAUSED || state == DdlState.ROLLBACK_PAUSED)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                "Only PAUSED/ROLLBACK_PAUSED jobs can be continued, but job %s is in %s state", record.jobId,
                record.state));
        }
        if (!record.isSupportContinue()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                "Continue/recover is not supported for job %s. Please try: cancel ddl %s", record.jobId, record.jobId));
        }
        if (record.isSubJob() && !enableOperateSubJob) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on subjob is not allowed");
        }

        int countDone = 0;
        List<Long> jobIdList = new ArrayList<>();
        if (DdlState.PAUSED == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.PAUSED,
                DdlState.RUNNING)) {
                jobIdList.add(record.jobId);
                countDone++;
            }
        } else if (DdlState.ROLLBACK_PAUSED == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.ROLLBACK_PAUSED,
                DdlState.ROLLBACK_RUNNING)) {
                jobIdList.add(record.jobId);
                countDone++;
            }
        }

        DdlEngineRequester.removeResponses(jobIdList);
        DdlEngineRequester.notifyLeader(executionContext.getSchemaName(), jobIdList);

        boolean asyncMode = executionContext.getParamManager().getBoolean(ConnectionParams.PURE_ASYNC_DDL_MODE);
        boolean checkResponseInMemory
            = executionContext.getParamManager().getBoolean(ConnectionParams.CHECK_RESPONSE_IN_MEM);
        if (!asyncMode) {
            respond(record.schemaName, record.jobId, executionContext, checkResponseInMemory, true);
        }

        return new AffectRowCursor(countDone);
    }

}
