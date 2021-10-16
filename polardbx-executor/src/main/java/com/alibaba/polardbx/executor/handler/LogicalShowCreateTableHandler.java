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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateTable;

/**
 * @author mengshi
 */
public class LogicalShowCreateTableHandler extends HandlerCommon {

    private final LogicalShowCreateTablesForPartitionDatabaseHandler partitionDatabaseHandler;
    private final LogicalShowCreateTablesForShardingDatabaseHandler shardingDatabaseHandler;

    public LogicalShowCreateTableHandler(IRepository repo) {
        super(repo);
        partitionDatabaseHandler = new LogicalShowCreateTablesForPartitionDatabaseHandler(repo);
        shardingDatabaseHandler = new LogicalShowCreateTablesForShardingDatabaseHandler(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;

        String schemaName = show.getSchemaName();

        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return partitionDatabaseHandler.handle(logicalPlan, executionContext);
        } else {
            return shardingDatabaseHandler.handle(logicalPlan, executionContext);
        }
    }

}
