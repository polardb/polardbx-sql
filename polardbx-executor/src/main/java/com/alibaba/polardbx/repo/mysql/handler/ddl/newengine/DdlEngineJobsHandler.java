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

import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlInterruptSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public abstract class DdlEngineJobsHandler extends HandlerCommon {

    protected final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();

    public DdlEngineJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        return doHandle((LogicalDal) logicalPlan, executionContext);
    }

    /**
     * Do a specific handling.
     *
     * @param logicalPlan A logical plan
     * @param executionContext The execution context
     * @return Result cursor
     */
    protected abstract Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext);

    protected void validate(boolean isAll, List<Long> jobIds) {
        boolean isInvalid = !isAll && (jobIds == null || jobIds.isEmpty());
        if (isInvalid) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "no job id provided");
        }
    }

    protected List<DdlEngineRecord> fetchRecords(String schemaName, boolean isAll, List<Long> jobIds) {
        validate(isAll, jobIds);
        List<DdlEngineRecord> records;
        if (isAll) {
            records = schedulerManager.fetchRecords(schemaName);
        } else {
            records = schedulerManager.fetchRecords(jobIds);
        }
        return records;
    }

    protected List<DdlEngineTaskRecord> fetchTasks(long jobId) {
        return schedulerManager.fetchTaskRecord(jobId);
    }

    protected void interruptJob(String schemaName, List<Long> jobIds) {
        DdlRequest ddlRequest = new DdlRequest(schemaName, jobIds);
        GmsSyncManagerHelper.sync(new DdlInterruptSyncAction(ddlRequest), schemaName);
    }

    protected void respond(String schemaName,
                           long jobId,
                           ExecutionContext executionContext,
                           boolean checkResponseInMemory) {
        DdlRequest ddlRequest = new DdlRequest(schemaName.toLowerCase(), Lists.newArrayList(jobId));
        DdlEngineRequester.respond(ddlRequest, new DdlJobManager(), executionContext, checkResponseInMemory);
    }
}
