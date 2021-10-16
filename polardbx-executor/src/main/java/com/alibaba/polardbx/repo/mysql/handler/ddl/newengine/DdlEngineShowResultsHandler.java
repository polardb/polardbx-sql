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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponseCollectSyncAction;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlShowDdlResults;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            if(results==null){
                return;
            }
            for (Pair<NodeInfo, List<Map<String, Object>>> result : results) {
                if(result==null || result.getValue()==null){
                    continue;
                }
                for (Map<String, Object> row : result.getValue()) {
                    if(row==null){
                        continue;
                    }
                    rowList.add(buildRow(row));
                }
            }
        });

        Collections.sort(rowList, (o1, o2) -> {
            String s1 = (String) o1[0];
            String s2 = (String) o2[0];
            return s2.compareTo(s1);
        });

        for(Object[] row: rowList){
            resultCursor.addRow(row);
        }

        return resultCursor;
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
