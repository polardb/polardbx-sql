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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchSPMSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSPM;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author fangwu
 */
public class InformationSchemaSPMHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaSPMHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaSPM;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        for (String schemaName : schemaNames) {
            List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new FetchSPMSyncAction(schemaName),
                schemaName);
            for (List<Map<String, Object>> nodeRows : results) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    final String baselineId = DataTypes.StringType.convertFrom(row.get("BASELINE_ID"));
                    final String planId = DataTypes.StringType.convertFrom(row.get("PLAN_ID"));
                    final Integer fixed = DataTypes.BooleanType.convertFrom(row.get("FIXED"));
                    final Integer accepted = DataTypes.BooleanType.convertFrom(row.get("ACCEPTED"));
                    final Long chooseCount = DataTypes.LongType.convertFrom(row.get("CHOOSE_COUNT"));
                    final String selectivitySpace = DataTypes.StringType.convertFrom(row.get("SELECTIVITY_SPACE"));
                    final String params = DataTypes.StringType.convertFrom(row.get("PARAMS"));
                    final String recentlyChooseRate = DataTypes.StringType.convertFrom(row.get("RECENTLY_CHOOSE_RATE"));
                    final Long expectedRows = DataTypes.LongType.convertFrom(row.get("EXPECTED_ROWS"));
                    final Long maxRowsFeedback = DataTypes.LongType.convertFrom(row.get("MAX_ROWS_FEEDBACK"));
                    final Long minRowsFeedback = DataTypes.LongType.convertFrom(row.get("MIN_ROWS_FEEDBACK"));
                    final String origin = DataTypes.StringType.convertFrom(row.get("ORIGIN"));
                    final String parameterizedSql = DataTypes.StringType.convertFrom(row.get("PARAMETERIZED_SQL"));
                    final String externalizedPlan = DataTypes.StringType.convertFrom(row.get("EXTERNALIZED_PLAN"));
                    final String isRebuildAtLoad = DataTypes.StringType.convertFrom(row.get("IS_REBUILD_AT_LOAD"));
                    final String hint = DataTypes.StringType.convertFrom(row.get("HINT"));
                    final String usePostPlanner = DataTypes.StringType.convertFrom(row.get("USE_POST_PLANNER"));

                    cursor.addRow(new Object[] {
                        baselineId,
                        schemaName,
                        planId,
                        fixed,
                        accepted,
                        chooseCount,
                        selectivitySpace,
                        params,
                        recentlyChooseRate,
                        expectedRows,
                        maxRowsFeedback,
                        minRowsFeedback,
                        origin,
                        parameterizedSql,
                        externalizedPlan,
                        isRebuildAtLoad,
                        hint,
                        usePostPlanner
                    });
                }
            }
        }
        return cursor;
    }
}

