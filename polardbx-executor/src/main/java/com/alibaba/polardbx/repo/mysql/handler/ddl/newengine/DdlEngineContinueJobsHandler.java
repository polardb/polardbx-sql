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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DdlEngineContinueJobsHandler extends DdlEngineJobsHandler {

    public DdlEngineContinueJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlContinueDdlJob command = (SqlContinueDdlJob) logicalPlan.getNativeSqlNode();
        return doContinue(command.isAll(), command.getJobIds(), executionContext);
    }

    public Cursor doContinue(boolean isAll, List<Long> jobIds, ExecutionContext executionContext) {
        boolean enableOperateSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OPERATE_SUBJOB);
        List<DdlEngineRecord> records =
            fetchRecords(executionContext.getSchemaName(), isAll, jobIds);
        records.stream().forEach(e -> {
            DdlState state = DdlState.valueOf(e.state);
            if (!(state == DdlState.PAUSED || state == DdlState.ROLLBACK_PAUSED)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                    "Only PAUSED/ROLLBACK_PAUSED jobs can be continued, job %s is in %s state", e.jobId, e.state));
            }
            if (!e.isSupportContinue()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                    "continue/recover is not supported for job %s. please try: cancel ddl %s", e.jobId, e.jobId));
            }
            if (e.isSubJob() && !enableOperateSubJob) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on subjob is not allowed");
            }
        });

        int countDone = 0;
        List<Long> jobIdList = new ArrayList<>();
        for (DdlEngineRecord record : records) {
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
        }

        DdlEngineRequester.notifyLeader(executionContext.getSchemaName(), jobIdList);

        boolean asyncMode = executionContext.getParamManager().getBoolean(ConnectionParams.PURE_ASYNC_DDL_MODE);
        if (!asyncMode && CollectionUtils.isNotEmpty(records) && CollectionUtils.size(records) == 1) {
            DdlEngineRecord record = records.get(0);
            respond(record.schemaName, record.jobId, executionContext, true);
        }

        return new AffectRowCursor(new int[] {countDone});
    }

}
