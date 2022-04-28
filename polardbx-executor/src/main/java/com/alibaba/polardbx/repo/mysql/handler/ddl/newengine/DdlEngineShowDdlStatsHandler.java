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
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implements `show ddl stats` command
 *
 * @author moyi
 * @since 2021/11
 */
public class DdlEngineShowDdlStatsHandler extends DdlEngineJobsHandler {

    public DdlEngineShowDdlStatsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor cursor = DdlEngineStats.Metric.buildCursor();

        DdlStatsSyncAction sync = new DdlStatsSyncAction();
        Map<String, DdlEngineStats.Metric> metrics = new TreeMap<>();

        // Merge stats from all nodes
        GmsSyncManagerHelper.sync(sync, executionContext.getSchemaName(), results -> {
            if (results == null) {
                return;
            }
            for (Pair<NodeInfo, List<Map<String, Object>>> result : results) {
                if (CollectionUtils.isEmpty(result.getValue())) {
                    continue;
                }
                for (Map<String, Object> row : result.getValue()) {
                    DdlEngineStats.Metric m = DdlEngineStats.Metric.fromMap(row);
                    metrics.merge(m.getName(), m, DdlEngineStats.Metric::merge);
                }
            }
        });

        for (DdlEngineStats.Metric m : metrics.values()) {
            cursor.addRow(m.toRow());
        }
        return cursor;
    }

    public static class DdlStatsSyncAction implements IGmsSyncAction {

        public DdlStatsSyncAction() {
        }

        @Override
        public Object sync() {
            ArrayResultCursor result = DdlEngineStats.Metric.buildCursor();
            for (DdlEngineStats.Metric metric : DdlEngineStats.getAllMetrics().values()) {
                result.addRow(metric.toRow());
            }
            return result;
        }
    }

}
