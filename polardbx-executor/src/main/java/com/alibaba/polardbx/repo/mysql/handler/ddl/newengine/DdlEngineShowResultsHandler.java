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
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponseCollectSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlShowDdlResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.DDL_JOB_ID;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.DETAIL;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SOURCE_SCHEMA;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SQL_DST;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.SQL_SRC;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.STAGE;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.STATUS;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.TABLE_SEQ;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaCreateDatabase.TARGET_SCHEMA;

/**
 * Implements `show ddl result [jobId]` command
 */
public class DdlEngineShowResultsHandler extends DdlEngineJobsHandler {

    public DdlEngineShowResultsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlShowDdlResults showDdlResults = (SqlShowDdlResults) logicalPlan.getNativeSqlNode();

        String schemaName = executionContext.getSchemaName();
        List<Long> jobIds = showDdlResults.getJobIds();

        // Try to inspect caches from new DDL engine.
        ArrayResultCursor resultCursor = DdlResponseCollectSyncAction.buildResultCursor();
        List<Object[]> rowList = new ArrayList<>();

        //new engine
        IGmsSyncAction syncAction = new DdlResponseCollectSyncAction(schemaName, jobIds);
        GmsSyncManagerHelper.sync(syncAction, schemaName, SyncScope.MASTER_ONLY, results -> {
            if (results == null) {
                return;
            }
            for (Pair<GmsNode, List<Map<String, Object>>> result : results) {
                if (result == null || result.getValue() == null) {
                    continue;
                }
                for (Map<String, Object> row : result.getValue()) {
                    if (row == null) {
                        continue;
                    }
                    if (row.get("DDL_TYPE") != null && ((String) row.get("DDL_TYPE")).equalsIgnoreCase(
                        "CREATE_DATABASE_LIKE_AS")) {
                        List<Object[]> createDatabaseRows = processCreateDatabaseRow(row);
                        rowList.addAll(createDatabaseRows);
                    } else {
                        rowList.add(buildRow(row));
                    }
                }
            }
        });

        // try to inspect running ddl jobs
        List<DdlEngineRecord> engineRecords =
            DdlEngineShowJobsHandler.inspectDdlJobs(
                Pair.of(jobIds, executionContext.getSchemaName()),
                new DdlEngineSchedulerManager());
        for (DdlEngineRecord record : GeneralUtil.emptyIfNull(engineRecords)) {
            if (!record.isSubJob()) {
                rowList.add(convertEngineRecordToRow(record));
            }
        }

        Collections.sort(rowList, (o1, o2) -> {
            String s1 = String.valueOf(o1[0]);
            String s2 = String.valueOf(o2[0]);
            return s2.compareTo(s1);
        });

        for (Object[] row : rowList) {
            resultCursor.addRow(row);
        }

        return resultCursor;
    }

    private Object[] convertEngineRecordToRow(DdlEngineRecord record) {
        return new Object[] {
            String.valueOf(record.jobId),
            record.schemaName,
            record.objectName,
            record.ddlType,
            record.state,
            null,
        };
    }

    private Object[] convertJobToRow(Job job) {
        return new Object[] {
            String.valueOf(job.getId()),
            job.getObjectSchema(),
            job.getObjectName(),
            job.getType(),
            job.getState(),
            null,
        };
    }

    private Object[] buildRow(Map<String, Object> row) {
        return new Object[] {
            row.get("JOB_ID"),
            row.get("SCHEMA_NAME"),
            row.get("OBJECT_NAME"),
            row.get("DDL_TYPE"),
            row.get("RESULT_TYPE"),
            row.get("RESULT_CONTENT")
        };
    }

    private List<Object[]> processCreateDatabaseRow(Map<String, Object> row) {
        int i = 0;
        final Map<String, Integer> columnIndex =
            ImmutableMap.<String, Integer>builder()
                .put("JOB_ID", i++)
                .put("SCHEMA_NAME", i++)
                .put("OBJECT_NAME", i++)
                .put("DDL_TYPE", i++)
                .put("RESULT_TYPE", i++)
                .put("RESULT_CONTENT", i++)
                .build();

        List<Object[]> result = new ArrayList<>();
        List<Map<String, Object>> createDatabaseResult =
            queryCreateDatabaseTaskResultFromViewByJobId(Long.parseLong((String) row.get("JOB_ID")));

        boolean allSuccess = true;
        Object[] baseRow = buildRow(row);
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
            String state = "";
            if (createDatabaseResultItem.get(STATUS) != null) {
                state = ((Slice) createDatabaseResultItem.get(STATUS)).toStringUtf8();
                if (!state.equalsIgnoreCase("SUCCESS")) {
                    allSuccess = false;
                }
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

            subRow[columnIndex.get("SCHEMA_NAME")] = objectSchema;
            subRow[columnIndex.get("OBJECT_NAME")] = objectName;
            subRow[columnIndex.get("RESULT_TYPE")] = state;
            subRow[columnIndex.get("DDL_TYPE")] = "CREATE_TABLE";
            Map<String, Object> extraData = ImmutableMap.<String, Object>builder()
                .put("STAGE", stage)
                .put("SQL_SRC", sqlSrc)
                .put("SQL_DST", sqlDst)
                .put("DETAIL", detail)
                .build();
            subRow[columnIndex.get("RESULT_CONTENT")] = JSONObject.toJSONString(extraData);
            result.add(subRow);
        }
        if (!allSuccess && ((String) baseRow[columnIndex.get("RESULT_TYPE")]).equalsIgnoreCase("SUCCESS")) {
            baseRow[columnIndex.get("RESULT_TYPE")] = "PARTIAL_SUCCESS";
        }
        baseRow[columnIndex.get("RESULT_CONTENT")] = "";
        if (schemaSrc != null) {
            baseRow[columnIndex.get("SCHEMA_NAME")] = schemaSrc;
        }
        if (schemaDst != null) {
            baseRow[columnIndex.get("OBJECT_NAME")] = schemaDst;
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

}
