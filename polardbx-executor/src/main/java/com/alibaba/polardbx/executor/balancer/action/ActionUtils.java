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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMergePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupSplitPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.MoveDatabasesJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMoveDatabases;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRebalance;

/**
 * @since 2021/03
 */

public class ActionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ActionUtils.class);

    private static DDL parseDdl(ExecutionContext ec, String sql) {
        SqlNode sqlNode = new FastsqlParser().parse(sql, ec).get(0);
        SqlDdl stmt = (SqlDdl) sqlNode;
        SqlConverter converter = SqlConverter.getInstance(ec);
        stmt = (SqlDdl) converter.validate(stmt);

        return (DDL) converter.toRel(stmt);
    }

    /**
     * Convert a DDL sql to ddl job
     */
    public static ExecutableDdlJob convertToDDLJob(ExecutionContext ec, String sql) {
        DDL ddl = parseDdl(ec, sql);

        if (ddl instanceof AlterTableGroupSplitPartition) {
            LogicalAlterTableGroupSplitPartition splitPartition = LogicalAlterTableGroupSplitPartition.create(ddl);
            splitPartition.preparedData();
            return AlterTableGroupSplitPartitionJobFactory.create(ddl, splitPartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMergePartition) {
            LogicalAlterTableGroupMergePartition mergePartition = LogicalAlterTableGroupMergePartition.create(ddl);
            mergePartition.preparedData();
            return AlterTableGroupMergePartitionJobFactory.create(ddl, mergePartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMovePartition) {
            LogicalAlterTableGroupMovePartition movePartition = LogicalAlterTableGroupMovePartition.create(ddl);
            movePartition.preparedData();
            return AlterTableGroupMovePartitionJobFactory.create(ddl, movePartition.getPreparedData(), ec);
        } else if (ddl instanceof MoveDatabase) {
            LogicalMoveDatabases moveDatabase = LogicalMoveDatabases.create(ddl);
            moveDatabase.preparedData();
            return MoveDatabasesJobFactory.create(ddl, moveDatabase.getPreparedData(), ec);
        } else {
            throw new UnsupportedOperationException("unknown ddl: " + ddl);
        }

    }

    public static String genRebalanceResourceName(SqlRebalance.RebalanceTarget target, String name) {
        return "rebalance_" + target.toString() + "_" + TStringUtil.backQuote(name);
    }

    public static String genRebalanceClusterName() {
        return "rebalance_" + SqlRebalance.RebalanceTarget.CLUSTER;
    }
}
