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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlPauseDdlJob;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

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

        boolean enableContinueRunningSubJob =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_CONTINUE_RUNNING_SUBJOB);

        List<DdlEngineRecord> records = fetchRecords(executionContext.getSchemaName(), isAll, jobIds);

        int countDone =
            DdlEngineRequester.pauseJobs(records, enableOperateSubJob, enableContinueRunningSubJob, executionContext);

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
