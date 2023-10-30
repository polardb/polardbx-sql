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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlShowDdlJobs;
import org.apache.calcite.sql.SqlShowProcesslist;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ENGINE_TYPE_DAG;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.PERCENTAGE;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.BACKFILL_PROGRESS;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.DDL_JOB_ID;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.DETAIL;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SOURCE_SCHEMA;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SQL_DST;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SQL_SRC;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.STAGE;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.STATUS;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.TABLE_SEQ;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.TARGET_SCHEMA;

public class DdlEngineShowJobsHandler extends DdlEngineJobsHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineShowJobsHandler.class);

    private final DdlJobManager ddlJobManager = new DdlJobManager();

    private GsiBackfillManager gsiBackfillManager;

    public DdlEngineShowJobsHandler(IRepository repo) {
        super(repo);
    }

    public static List<DdlEngineRecord> inspectDdlJobs(Pair<List<Long>, String> schemaOrJobsIds,
                                                       DdlEngineSchedulerManager dsm) {
        if (CollectionUtils.isNotEmpty(schemaOrJobsIds.getKey())) {
            List<Long> jobIds = schemaOrJobsIds.getKey();
            return dsm.fetchRecords(jobIds);
        } else if (StringUtils.isNotEmpty(schemaOrJobsIds.getValue())) {
            String schema = schemaOrJobsIds.getValue();
            return dsm.fetchRecords(schema);
        } else {
            throw new IllegalArgumentException("either schema or jobIds should be exist");
        }
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlShowDdlJobs showDdlJobs = (SqlShowDdlJobs) logicalPlan.getNativeSqlNode();

        boolean isFull = showDdlJobs.isFull();
        List<Long> jobIds = showDdlJobs.getJobIds();

        List<DdlEngineRecord> records =
            inspectDdlJobs(Pair.of(jobIds, executionContext.getSchemaName()), ddlJobManager);

        ArrayResultCursor resultCursor = buildResultCursor(isFull);
        if (CollectionUtils.isNotEmpty(records)) {
            gsiBackfillManager = new GsiBackfillManager(executionContext.getSchemaName());
            // If the jobs on new DDL engine, then show them.
            for (DdlEngineRecord record : records) {
                if (!isFull && record.isSubJob()) {
                    continue;
                }
                if (record.ddlType.equalsIgnoreCase("CREATE_DATABASE_LIKE_AS")) {
                    List<Object[]> createDatabaseRows = processCreateDatabaseLikeAsJob(record, isFull);
                    createDatabaseRows.forEach(
                        row -> resultCursor.addRow(row)
                    );
                } else {
                    resultCursor.addRow(buildRow(record, isFull));
                }
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
        resultCursor.addColumn("TOTAL_BACKFILL_PROGRESS", DataTypes.StringType);
        resultCursor.addColumn("CURRENT_PHY_DDL_PROGRESS", DataTypes.StringType);
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
        Pair<String, String> taskAndBackfillProgress = getTaskAndBackfillProgress(record.jobId);
        String totalProgress = taskAndBackfillProgress.getKey();
        String backfillProgress = taskAndBackfillProgress.getValue();
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
            String ddlResult = NONE;
            if (StringUtils.isNotEmpty(record.result)) {
                ddlResult = StringUtils.substring(record.result, 0, MAX_SHOW_LEN);
            }
            return new Object[] {
                record.jobId, record.schemaName, record.objectName, ENGINE_TYPE_DAG, record.ddlType, record.state,
                backfillProgress,
                record.progress + PERCENTAGE,
                totalProgress,
                gmtCreated, gmtModified, elapsedTime, phyProcess,
                cancelable,
                NONE, record.responseNode, record.executionNode, record.traceId, record.ddlStmt, ddlResult, NONE
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

    private List<Object[]> processCreateDatabaseLikeAsJob(DdlEngineRecord record, boolean isFull) {
        int i = 0;
        final Map<String, Integer> columnIndexForShowFull =
            ImmutableMap.<String, Integer>builder()
                .put("JOB_ID", i++)
                .put("OBJECT_SCHEMA", i++)
                .put("OBJECT_NAME", i++)
                .put("ENGINE", i++)
                .put("DDL_TYPE", i++)
                .put("STATE", i++)
                .put("TOTAL_BACKFILL_PROGRESS", i++)
                .put("CURRENT_PHY_DDL_PROGRESS", i++)
                .put("PROGRESS", i++)
                .put("START_TIME", i++)
                .put("END_TIME", i++)
                .put("ELAPSED_TIME(MS)", i++)
                .put("PHY_PROCESS", i++)
                .put("CANCELABLE", i++)
                .put("PARENT_JOB_ID", i++)
                .put("RESPONSE_NODE", i++)
                .put("EXECUTION_NODE", i++)
                .put("TRACE_ID", i++)
                .put("DDL_STMT", i++)
                .put("REMARK", i++)
                .put("LEGACY_ENGINE_INFO", i++)
                .build();

        List<Object[]> result = new ArrayList<>();
        if (!isFull) {
            Object[] baseRow = buildRow(record, isFull);
            List<Map<String, Object>> createDatabaseResult = queryCreateDatabaseTaskResultFromViewByJobId(record.jobId);
            String schemaSrc = null, schemaDst = null;
            for (Map<String, Object> createDatabaseResultItem : createDatabaseResult) {
                if (createDatabaseResultItem.get(SOURCE_SCHEMA) != null) {
                    schemaSrc = ((Slice) createDatabaseResultItem.get(SOURCE_SCHEMA)).toStringUtf8();
                }
                if (createDatabaseResultItem.get(TARGET_SCHEMA) != null) {
                    schemaDst = ((Slice) createDatabaseResultItem.get(TARGET_SCHEMA)).toStringUtf8();
                }
                if (schemaSrc != null && schemaDst != null) {
                    break;
                }
            }
            if (schemaSrc != null) {
                baseRow[columnIndexForShowFull.get("OBJECT_SCHEMA")] = schemaSrc;
            }
            if (schemaDst != null) {
                baseRow[columnIndexForShowFull.get("OBJECT_NAME")] = schemaDst;
            }
            result.add(baseRow);
            return result;
        }

        List<Map<String, Object>> createDatabaseResult = queryCreateDatabaseTaskResultFromViewByJobId(record.jobId);
        Object[] baseRow = buildRow(record, isFull);
        String schemaSrc = null, schemaDst = null;
        for (Map<String, Object> createDatabaseResultItem : createDatabaseResult) {
            Object[] subRow = baseRow.clone();
            if (createDatabaseResultItem.get(SOURCE_SCHEMA) != null) {
                schemaSrc = ((Slice) createDatabaseResultItem.get(SOURCE_SCHEMA)).toStringUtf8();
            }

            String objectSchema = "";
            if (createDatabaseResultItem.get(TARGET_SCHEMA) != null) {
                objectSchema = ((Slice) createDatabaseResultItem.get(TARGET_SCHEMA)).toStringUtf8();
                schemaDst = objectSchema;
            }
            String objectName = "";
            if (createDatabaseResultItem.get(TABLE_SEQ) != null) {
                objectName = ((Slice) createDatabaseResultItem.get(TABLE_SEQ)).toStringUtf8();
            }

            String toTalBackFillProgress = "";
            if (createDatabaseResultItem.get(BACKFILL_PROGRESS) != null) {
                toTalBackFillProgress = ((Slice) createDatabaseResultItem.get(BACKFILL_PROGRESS)).toStringUtf8();
            }
            String progress = "-";
            String state = "";
            if (createDatabaseResultItem.get(STATUS) != null) {
                state = ((Slice) createDatabaseResultItem.get(STATUS)).toStringUtf8();
            }
            String stage = "";
            if (createDatabaseResultItem.get(STAGE) != null) {
                stage = ((Slice) createDatabaseResultItem.get(STAGE)).toStringUtf8();
            }
            String detail = "";
            if (createDatabaseResultItem.get(DETAIL) != null) {
                detail = ((Slice) createDatabaseResultItem.get(DETAIL)).toStringUtf8();
            }
            String sqlDst = "";
            if (createDatabaseResultItem.get(SQL_DST) != null) {
                sqlDst = ((Slice) createDatabaseResultItem.get(SQL_DST)).toStringUtf8();
            }
            String sqlSrc = "";
            if (createDatabaseResultItem.get(SQL_SRC) != null) {
                sqlSrc = ((Slice) createDatabaseResultItem.get(SQL_SRC)).toStringUtf8();
            }

            subRow[columnIndexForShowFull.get("OBJECT_SCHEMA")] = objectSchema;
            subRow[columnIndexForShowFull.get("OBJECT_NAME")] = objectName;
            subRow[columnIndexForShowFull.get("TOTAL_BACKFILL_PROGRESS")] = toTalBackFillProgress;
            subRow[columnIndexForShowFull.get("PROGRESS")] = progress;
            subRow[columnIndexForShowFull.get("DDL_TYPE")] = "CREATE_TABLE";
            if (state.equalsIgnoreCase("FAIL")) {
                String ddlEngineState = (String) subRow[columnIndexForShowFull.get("STATE")];
                if (ddlEngineState.equalsIgnoreCase("RUNNING")) {
                    subRow[columnIndexForShowFull.get("STATE")] = state;
                }
            }
            subRow[columnIndexForShowFull.get("DDL_STMT")] = sqlDst;

            Map<String, Object> extraData = ImmutableMap.<String, Object>builder()
                .put("STAGE", stage)
                .put("SQL_SRC", sqlSrc)
                .put("DETAIL", detail)
                .build();
            subRow[columnIndexForShowFull.get("REMARK")] = JSONObject.toJSONString(extraData);
            result.add(subRow);
        }
        if (schemaSrc != null) {
            baseRow[columnIndexForShowFull.get("OBJECT_SCHEMA")] = schemaSrc;
        }
        if (schemaDst != null) {
            baseRow[columnIndexForShowFull.get("OBJECT_NAME")] = schemaDst;
        }
        result.add(0, baseRow);
        return result;
    }

    private List<Map<String, Object>> queryCreateDatabaseTaskResultFromViewByJobId(Long jobId) {
        final String querySql = "select * from INFORMATION_SCHEMA.CREATE_DATABASE where "
            + " `" + DDL_JOB_ID + "` = " + jobId;
        return DdlHelper.getServerConfigManager().executeQuerySql(
            querySql,
            DEFAULT_DB_NAME,
            null
        );
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

    /**
     * left: taskProgress
     * right: BackFillProgress
     */
    private Pair<String, String> getTaskAndBackfillProgress(long jobId) {
        List<DdlEngineTaskRecord> taskRecordList = ddlJobManager.fetchTaskRecord(jobId);
        if (CollectionUtils.isEmpty(taskRecordList)) {
            return Pair.of(NONE, NONE);
        }
        final String taskProgress = getTaskProgress(jobId, taskRecordList);
        final String backfillProgress = getBackfillProgress(jobId, taskRecordList);
        return Pair.of(taskProgress, backfillProgress);
    }

    private String getTaskProgress(long jobId, List<DdlEngineTaskRecord> taskRecordList) {
        try {
            int totalCount = taskRecordList.size();
            int finishedCount = 0;
            for (DdlEngineTaskRecord record : taskRecordList) {
                if (StringUtils.equalsIgnoreCase(DdlTaskState.SUCCESS.name(), record.getState())) {
                    finishedCount++;
                }
            }
            if (totalCount == 0) {
                return NONE;
            }
            int progress = finishedCount * 100 / totalCount;
            return progress + PERCENTAGE;
        } catch (Exception e) {
            LOGGER.error("get task progress error, jobId:" + jobId, e);
            return NONE;
        }
    }

    private String getBackfillProgress(long jobId, List<DdlEngineTaskRecord> taskRecordList) {
        if (CollectionUtils.isEmpty(taskRecordList)) {
            return NONE;
        }
        try {
            List<Long> candidate = taskRecordList.stream()
                .filter(e -> (StringUtils.containsIgnoreCase(e.getName(), "BackFill") ||
                    StringUtils.containsIgnoreCase(e.getName(), "ArchiveOSSTableData")))
                .map(e -> e.taskId)
                .collect(Collectors.toList());
            candidate.add(jobId);

            int totalCount = 0;
            int backfillProgress = 0;
            for (Long backfillId : candidate) {
                List<GsiBackfillManager.BackfillObjectRecord> rList =
                    gsiBackfillManager.queryBackfillProgress(backfillId);
                if (CollectionUtils.isEmpty(rList)) {
                    continue;
                }
                totalCount += rList.size();
                for (GsiBackfillManager.BackfillObjectRecord r : rList) {
                    backfillProgress += safeParseLong(r.getLastValue());
                }
            }
            if (totalCount == 0) {
                return NONE;
            }
            return Optional.ofNullable(backfillProgress / totalCount).map(p -> p + PERCENTAGE).orElse(NONE);
        } catch (Exception e) {
            LOGGER.error("get backfill progress error, jobId:" + jobId, e);
            return NONE;
        }
    }

    private long safeParseLong(String str) {
        try {
            if (StringUtils.isEmpty(str)) {
                return 0L;
            }
            return Long.valueOf(str);
        } catch (Exception e) {
            LOGGER.error("parse backfill progress error. str:" + str, e);
            return 0L;
        }
    }

}
