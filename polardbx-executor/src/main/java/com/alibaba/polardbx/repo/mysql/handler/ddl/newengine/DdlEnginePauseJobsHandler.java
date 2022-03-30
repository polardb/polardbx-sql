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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlPauseDdlJob;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DdlEnginePauseJobsHandler extends DdlEngineJobsHandler {

    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    public DdlEnginePauseJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlPauseDdlJob command = (SqlPauseDdlJob) logicalPlan.getNativeSqlNode();
        return doPause(command.isAll(), command.getJobIds(), executionContext);
    }

    public Cursor doPause(boolean isAll, List<Long> jobIds, ExecutionContext executionContext) {
        boolean enableOperateSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OPERATE_SUBJOB);
        List<DdlEngineRecord> records = fetchRecords(executionContext.getSchemaName(), isAll, jobIds);

        int countDone = 0;
        for (DdlEngineRecord record : records) {
            if (record.isSubJob() && !enableOperateSubJob) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "Operation on subjob is not allowed");
            }
            List<Long> pausedJobs = new ArrayList<>();
            pauseJobs(record, executionContext, true, pausedJobs);
            interruptJob(record.schemaName, pausedJobs);
            countDone += pausedJobs.size();
        }

        return new AffectRowCursor(new int[] {countDone});
    }

    private void pauseJobs(DdlEngineRecord record, ExecutionContext ec, boolean subJob,
                           List<Long> pausedJobs) {
        DdlState before = DdlState.valueOf(record.state);
        DdlState after = DdlState.PAUSE_JOB_STATE_TRANSFER.get(before);
        if (!(before == DdlState.RUNNING || before == DdlState.ROLLBACK_RUNNING)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "Only RUNNING/ROLLBACK_RUNNING jobs can be paused, job %s is in %s state", record.jobId, before));
        }

        if (schedulerManager.tryPauseDdl(record.jobId, before, after)) {
            LOG.info(String.format("pause job %d", record.jobId));
            pausedJobs.add(record.jobId);
            if (subJob) {
                pauseSubJobs(record.jobId, ec, pausedJobs);
            }
        }
    }

    private void pauseSubJobs(long jobId, ExecutionContext ec, List<Long> pausedJobs) {
        List<SubJobTask> subJobs = schedulerManager.fetchSubJobsRecursive(jobId);
        List<Long> subJobIds = GeneralUtil.emptyIfNull(subJobs)
            .stream().flatMap(x -> x.fetchAllSubJobs().stream()).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(subJobIds)) {
            return;
        }
        List<DdlEngineRecord> records = schedulerManager.fetchRecords(subJobIds);
        for (DdlEngineRecord record : GeneralUtil.emptyIfNull(records)) {
            pauseJobs(record, ec, false, pausedJobs);
        }
    }

}
