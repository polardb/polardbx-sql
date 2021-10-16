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
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlCancelDdlJob;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DdlEngineCancelJobsHandler extends DdlEngineJobsHandler {

    public DdlEngineCancelJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlCancelDdlJob command = (SqlCancelDdlJob) logicalPlan.getNativeSqlNode();
        return doCancel(command.isAll(), command.getJobIds(), executionContext);
    }

    public Cursor doCancel(boolean isAll, List<Long> jobIds, ExecutionContext executionContext) {
        List<DdlEngineRecord> records =
            fetchRecords(executionContext.getSchemaName(), isAll, jobIds);
        records.stream().forEach(e -> {
            DdlState state = DdlState.valueOf(e.state);
            if (!(state == DdlState.RUNNING || state == DdlState.PAUSED)) {
                String errMsg = String.format(
                    "Only RUNNING/PAUSED jobs can be cancelled, job %s is in %s state. ", e.jobId, e.state);
                if (StringUtils.equalsIgnoreCase(e.state, DdlState.ROLLBACK_PAUSED.name())) {
                    errMsg += String.format("you may want to try command: continue ddl %s", e.jobId);
                }
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, errMsg);
            }
            if (!e.isSupportCancel()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                    "cancel/rollback is not supported for job %s. please try: continue ddl %s", e.jobId, e.jobId));
            }
        });

        int countDone = 0;
        for (DdlEngineRecord record : records) {
            if (DdlState.RUNNING == DdlState.valueOf(record.state)) {
                if (schedulerManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.RUNNING,
                    DdlState.ROLLBACK_RUNNING)) {
                    countDone++;
                    interruptJob(record.schemaName, record.jobId);
                }
            } else if (DdlState.PAUSED == DdlState.valueOf(record.state)) {
                if (schedulerManager.tryUpdateDdlState(
                    record.schemaName,
                    record.jobId,
                    DdlState.PAUSED,
                    DdlState.ROLLBACK_RUNNING)) {
                    countDone++;
                    interruptJob(record.schemaName, record.jobId);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(records) && CollectionUtils.size(records) == 1) {
            DdlEngineRecord record = records.get(0);
            respond(record.schemaName, record.jobId, executionContext, false);
        }

        return new AffectRowCursor(new int[] {countDone});
    }

}
