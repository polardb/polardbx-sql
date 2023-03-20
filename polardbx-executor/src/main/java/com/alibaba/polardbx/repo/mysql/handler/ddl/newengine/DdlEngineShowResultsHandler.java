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

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponseCollectSyncAction;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlShowDdlResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        GmsSyncManagerHelper.sync(syncAction, schemaName, results -> {
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
                    rowList.add(buildRow(row));
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

}
