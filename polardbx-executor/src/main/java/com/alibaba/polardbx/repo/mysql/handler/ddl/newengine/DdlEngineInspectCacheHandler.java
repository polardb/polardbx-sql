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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlCacheCollectionSyncAction;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlInspectDdlJobCache;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ENGINE_TYPE_DAG;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;

public class DdlEngineInspectCacheHandler extends DdlEngineJobsHandler {

    public DdlEngineInspectCacheHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor doHandle(final LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlNode sqlNode = logicalPlan.getNativeSqlNode();

        if (!(sqlNode instanceof SqlInspectDdlJobCache)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "Unknown SQL Node: "
                + sqlNode.getKind().name());
        }

        String schemaName = executionContext.getSchemaName();
        ArrayResultCursor resultCursor = DdlCacheCollectionSyncAction.buildResultCursor();

        IGmsSyncAction syncAction = new DdlCacheCollectionSyncAction(schemaName);
        GmsSyncManagerHelper.sync(syncAction, schemaName, results -> {
            if (results != null) {
                for (Pair<NodeInfo, List<Map<String, Object>>> result : results) {
                    if (result != null && result.getValue() != null) {
                        for (Map<String, Object> row : result.getValue()) {
                            if (row != null) {
                                resultCursor.addRow(buildRow(row));
                            }
                        }
                    }
                }
            }
        });

        return resultCursor;
    }

    private Object[] buildRow(Map<String, Object> row) {
        return new Object[] {
            ENGINE_TYPE_DAG,
            row.get("SERVER"),
            row.get("JOB_ID"),
            row.get("SCHEMA_NAME"),
            row.get("DDL_STMT"),
            row.get("STATE"),
            row.get("INTERRUPTED"),
            NONE
        };
    }

}
