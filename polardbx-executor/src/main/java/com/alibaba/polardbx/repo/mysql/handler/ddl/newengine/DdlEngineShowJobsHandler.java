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
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowProcesslistHandler;
import org.apache.calcite.sql.SqlShowDdlJobs;
import org.apache.calcite.sql.SqlShowProcesslist;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ENGINE_TYPE_DAG;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.PERCENTAGE;

public class DdlEngineShowJobsHandler extends DdlEngineJobsHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineShowJobsHandler.class);

    private final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();

    private GsiBackfillManager gsiBackfillManager;

    public DdlEngineShowJobsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlShowDdlJobs showDdlJobs = (SqlShowDdlJobs) logicalPlan.getNativeSqlNode();

        boolean isFull = showDdlJobs.isFull();
        List<Long> jobIds = showDdlJobs.getJobIds();

        // Try new DDL engine first.
        List<DdlEngineRecord> records;
        if (jobIds != null && jobIds.size() > 0) {
            records = schedulerManager.fetchRecords(jobIds);
        } else {
            records = schedulerManager.fetchRecords(executionContext.getSchemaName());
        }

        ArrayResultCursor resultCursor = buildResultCursor(isFull);
        if (CollectionUtils.isNotEmpty(records)) {
            gsiBackfillManager = new GsiBackfillManager(executionContext.getSchemaName());
            // If the jobs on new DDL engine, then show them.
            for (DdlEngineRecord record : records) {
                resultCursor.addRow(buildRow(record, isFull));
            }
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor(boolean isFull) {
        ArrayResultCursor resultCursor = new ArrayResultCursor("DDL_ENGINE");

        resultCursor.addColumn("JOB_ID", DataTypes.StringType);
        resultCursor.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        resultCursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        resultCursor.addColumn("ENGINE", DataTypes.StringType);
        resultCursor.addColumn("DDL_TYPE", DataTypes.StringType);
        resultCursor.addColumn("STATE", DataTypes.StringType);
        resultCursor.addColumn("BACKFILL_PROGRESS", DataTypes.StringType);
        resultCursor.addColumn("PHY_DDL_PROGRESS", DataTypes.StringType);
        resultCursor.addColumn("PROGRESS", DataTypes.StringType);
        resultCursor.addColumn("START_TIME", DataTypes.StringType);
        resultCursor.addColumn("END_TIME", DataTypes.StringType);
        resultCursor.addColumn("ELAPSED_TIME(MS)", DataTypes.StringType);
        resultCursor.addColumn("PHY_PROCESS", DataTypes.StringType);
        resultCursor.addColumn("CANCELABLE", DataTypes.StringType);
        if (isFull) {
            resultCursor.addColumn("PARENT_JOB_ID", DataTypes.StringType);
            resultCursor.addColumn("RESPONSE_NODE", DataTypes.StringType);
            resultCursor.addColumn("EXECUTION_NODE", DataTypes.StringType);
            resultCursor.addColumn("TRACE_ID", DataTypes.StringType);
            resultCursor.addColumn("DDL_STMT", DataTypes.StringType);
            resultCursor.addColumn("REMARK", DataTypes.StringType);
            resultCursor.addColumn("LEGACY_ENGINE_INFO", DataTypes.StringType);
        }

        resultCursor.initMeta();

        return resultCursor;
    }

    private static int MAX_SHOW_LEN = 5000;

    private Object[] buildRow(DdlEngineRecord record, boolean isFull) {
        String phyProcess = checkPhyProcess(record);
        if (phyProcess != null && phyProcess != StringUtils.EMPTY) {
            phyProcess = phyProcess.substring(0, Math.min(phyProcess.length(), MAX_SHOW_LEN));
        }
        String backfillProgress = getBackfillProgress(record.jobId);
        String totalProgress = getTaskProgress(record.jobId);
        String cancelable = Boolean.valueOf(record.isSupportCancel()).toString();

        String gmtCreated = DdlHelper.convertTimestamp(record.gmtCreated);
        String gmtModified = DdlHelper.convertTimestamp(record.gmtModified);
        long gmtCurrent = System.currentTimeMillis();

        long elapsedTime;
        if (DdlHelper.isActiveState(DdlState.valueOf(record.state))) {
            elapsedTime = gmtCurrent - record.gmtCreated;
        } else {
            elapsedTime = record.gmtModified - record.gmtCreated;
        }

        if (isFull) {
            return new Object[] {
                record.jobId, record.schemaName, record.objectName, ENGINE_TYPE_DAG, record.ddlType, record.state,
                backfillProgress,
                record.progress + PERCENTAGE,
                totalProgress,
                gmtCreated, gmtModified, elapsedTime, phyProcess,
                cancelable,
                NONE, record.responseNode, record.executionNode, record.traceId, record.ddlStmt, NONE, NONE
            };
        } else {
            return new Object[] {
                record.jobId, record.schemaName, record.objectName, ENGINE_TYPE_DAG, record.ddlType, record.state,
                backfillProgress,
                record.progress + PERCENTAGE,
                totalProgress,
                gmtCreated, gmtModified, elapsedTime, phyProcess, cancelable
            };
        }
    }

    private String checkPhyProcess(DdlEngineRecord record) {
        StringBuilder phyProcess = new StringBuilder();
        if (DdlHelper.isActiveState(DdlState.valueOf(record.state))) {
            try {
                LogicalShowProcesslistHandler showProcesslistHandler = new LogicalShowProcesslistHandler(repo);
                ArrayResultCursor rc = showProcesslistHandler.doPhysicalShow(record.schemaName,
                    SqlShowProcesslist.create(SqlParserPos.ZERO, true, true, null, null, null, null));

                for (Row row : rc.getRows()) {
                    String group = row.getString(0);
                    String atom = row.getString(1);
                    String id = row.getString(2);
                    String db = row.getString(4);
                    String state = row.getString(7);
                    String info = row.getString(8);

                    String sessionId = group + DdlConstants.HYPHEN + atom + DdlConstants.HYPHEN + id;

                    boolean matched = false;
                    if (TStringUtil.containsIgnoreCase(info, record.traceId)) {
                        matched = true;
                    } else if (TStringUtil.containsIgnoreCase(info, record.objectName)
                        && TStringUtil.contains(info, record.ddlType.split(DdlConstants.UNDERSCORE)[0])) {
                        matched = true;
                    }

                    if (matched) {
                        phyProcess.append(DdlConstants.SEMICOLON);
                        phyProcess.append("ID: ").append(sessionId).append(", DB: ").append(db);
                        phyProcess.append(", STATE: ").append(state).append(", INFO: ").append(info);
                    }
                }

                if (phyProcess.length() > 0) {
                    phyProcess.deleteCharAt(0);
                }
            } catch (Throwable t) {
                LOGGER.error("Failed to get full physical processes. Caused by: " + t.getMessage(), t);
            }
        }
        return phyProcess.toString();
    }

    private String getTaskProgress(long jobId) {
        List<DdlEngineTaskRecord> taskRecordList = schedulerManager.fetchTaskRecord(jobId);
        if (CollectionUtils.isEmpty(taskRecordList)) {
            return 100 + PERCENTAGE;
        }
        int totalCount = taskRecordList.size();
        int finishedCount = 0;
        for (DdlEngineTaskRecord record : taskRecordList) {
            if (StringUtils.equalsIgnoreCase(DdlTaskState.SUCCESS.name(), record.getState())) {
                finishedCount++;
            }
        }
        int progress = finishedCount * 100 / totalCount;
        return progress + PERCENTAGE;
    }

    private String getBackfillProgress(long jobId) {
        Integer backfillProgress = gsiBackfillManager.loadBackfillMeta(jobId).getProgress();
        return Optional.ofNullable(backfillProgress).map(p -> p + PERCENTAGE).orElse(NONE);
    }

}
