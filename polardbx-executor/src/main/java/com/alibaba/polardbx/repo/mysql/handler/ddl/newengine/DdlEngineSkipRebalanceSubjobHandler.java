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

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlSkipRebalanceSubjob;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP;
import static com.alibaba.polardbx.common.ddl.newengine.DdlType.MOVE_DATABASE;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils.updateSupportedCommands;

/**
 * @author wumu
 */
public class DdlEngineSkipRebalanceSubjobHandler extends DdlEngineCancelJobsHandler {

    public DdlEngineSkipRebalanceSubjobHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlSkipRebalanceSubjob command = (SqlSkipRebalanceSubjob) logicalPlan.getNativeSqlNode();

        if (command.isAll()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Operation with skip all rebalance subjob is not allowed");
        }

        if (command.getJobIds() == null || command.getJobIds().isEmpty()) {
            return new AffectRowCursor(0);
        }

        if (command.getJobIds().size() > 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on multi ddl jobs is not allowed");
        }

        return doSkipSubjob(command.getJobIds().get(0), executionContext);
    }

    public Cursor doSkipSubjob(Long jobId, ExecutionContext executionContext) {
        int countDone = 0;
        DdlEngineRecord record = schedulerManager.fetchRecordByJobId(jobId);
        if (record == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "The ddl job does not exist");
        }

        DdlState state = DdlState.valueOf(record.state);

        if (!record.isSubJob()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Operation on non-subjob is not allowed");
        }

        if (!ALTER_TABLEGROUP.name().equalsIgnoreCase(record.ddlType)
            && !MOVE_DATABASE.name().equalsIgnoreCase(record.ddlType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Operation on non-rebalance subjob is not allowed");
        }

        if (!(state == DdlState.RUNNING || state == DdlState.PAUSED || state == DdlState.ROLLBACK_PAUSED)) {
            String errMsg = String.format(
                "Only RUNNING/PAUSED/ROLLBACK_PAUSE jobs can be cancelled, but job %s is in %s state. ",
                record.jobId, record.state);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, errMsg);
        }

        if (!record.isSupportCancel()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("The subjob %s can not be cancelled", jobId));
        }

        // change state
        if (DdlState.RUNNING == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.RUNNING,
                DdlState.ROLLBACK_TO_READY)) {

                updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
            }
        } else if (DdlState.PAUSED == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.PAUSED,
                DdlState.ROLLBACK_TO_READY)) {

                updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
            }
        } else if (DdlState.ROLLBACK_PAUSED == DdlState.valueOf(record.state)) {
            if (schedulerManager.tryUpdateDdlState(
                record.schemaName,
                record.jobId,
                DdlState.ROLLBACK_PAUSED,
                DdlState.ROLLBACK_TO_READY)) {

                updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
            }
        }

        DdlHelper.waitToContinue(DdlConstants.MEDIAN_WAITING_TIME);
        DdlHelper.interruptJobs(record.schemaName, Collections.singletonList(jobId));
        DdlHelper.killActivePhyDDLs(record.schemaName, record.traceId);
        DdlEngineRequester.notifyLeader(record.schemaName, Collections.singletonList(jobId));

        boolean asyncPause = executionContext.getParamManager().getBoolean(ConnectionParams.PURE_ASYNC_DDL_MODE)
            || executionContext.getParamManager().getBoolean(ConnectionParams.ASYNC_PAUSE);
        if (!asyncPause) {
            try {
                respond(record.schemaName, record.jobId, executionContext, false, true);
            } catch (Exception e) {
                // ignore
            }
        }

        return new AffectRowCursor(countDone);
    }
}
