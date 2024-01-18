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

        List<Long> allJobIds = command.getJobIds();

        return doSkipSubjob(allJobIds, executionContext);
    }

    public Cursor doSkipSubjob(List<Long> jobIds, ExecutionContext executionContext) {
        int countDone = 0;
        List<DdlEngineRecord> records =
            fetchRecords(executionContext.getSchemaName(), false, jobIds);
        List<Long> skipSubjobs = new ArrayList<>();

        for (DdlEngineRecord record : records) {
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
                continue;
            }

            // change state
            if (DdlState.RUNNING == DdlState.valueOf(record.state)) {
                if (schedulerManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.RUNNING,
                    DdlState.ROLLBACK_TO_READY)) {

                    updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
                    skipSubjobs.add(record.jobId);
                }
            } else if (DdlState.PAUSED == DdlState.valueOf(record.state)) {
                if (schedulerManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.PAUSED,
                    DdlState.ROLLBACK_TO_READY)) {

                    updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
                    skipSubjobs.add(record.jobId);
                }
            } else if (DdlState.ROLLBACK_PAUSED == DdlState.valueOf(record.state)) {
                if (schedulerManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.ROLLBACK_PAUSED,
                    DdlState.ROLLBACK_TO_READY)) {

                    updateSupportedCommands(record.jobId, record.isSupportContinue(), record.isSupportCancel(), true);
                    skipSubjobs.add(record.jobId);
                }
            }
        }

        DdlHelper.waitToContinue(DdlConstants.MEDIAN_WAITING_TIME);
        DdlHelper.interruptJobs(executionContext.getSchemaName(), skipSubjobs);
        DdlEngineRequester.notifyLeader(executionContext.getSchemaName(), skipSubjobs);

        boolean asyncPause = executionContext.getParamManager().getBoolean(ConnectionParams.ASYNC_PAUSE);
        if (!asyncPause && CollectionUtils.isNotEmpty(records) && CollectionUtils.size(records) == 1) {
            DdlEngineRecord record = records.get(0);

            try {
                respond(record.schemaName, record.jobId, executionContext, false, true);
            } catch (Exception e) {
                // ignore
            }
        }

        return new AffectRowCursor(new int[] {countDone});
    }
}
