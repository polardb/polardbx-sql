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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowDbStatusHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlShowChangeSetStats;

import java.util.List;
import java.util.Map;

public class LogicalShowChangesetStatsHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowChangesetStatsHandler.class);

    public LogicalShowChangesetStatsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowChangeSetStats showChangeSetStats = (SqlShowChangeSetStats) show.getNativeSqlNode();

        String schemaName = showChangeSetStats.getSchema();
        if (schemaName == null || schemaName.equalsIgnoreCase("information_schema")) {
            return getChangeSetResultCursor();
        }

        return handleChangeSetResult(executionContext, schemaName);
    }

    private ArrayResultCursor getChangeSetResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("CHANGESET_STATS");
        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);
        result.addColumn("PHY_SCHEMA_NAME", DataTypes.StringType);
        result.addColumn("PHY_TABLE_NAME", DataTypes.StringType);
        result.addColumn("NUM_INSERTS", DataTypes.IntegerType);
        result.addColumn("NUM_UPDATES", DataTypes.IntegerType);
        result.addColumn("NUM_DELETES", DataTypes.IntegerType);
        result.addColumn("NUM_FILES", DataTypes.IntegerType);
        result.addColumn("MEMORY_SIZE", DataTypes.DoubleType);
        result.initMeta();
        return result;
    }

    private ArrayResultCursor handleChangeSetResult(ExecutionContext executionContext, String schemaName) {
        ArrayResultCursor result = getChangeSetResultCursor();

        Map<String, String> storageInstIdGroupNames = StatsUtils.queryGroupNameAndInstId(schemaName);

        for (Map.Entry<String, String> item : storageInstIdGroupNames.entrySet()) {
            String storageInstId = item.getKey();
            String groupName = item.getValue();

            List<List<Object>> res = ChangeSetUtils.queryGroup(executionContext, schemaName, groupName,
                ChangeSetUtils.SQL_CALL_CHANGESET_STATS);

            for (List<Object> row : res) {
                row.add(0, storageInstId);
                result.addRow(row.toArray());
            }
        }

        return result;
    }

}
