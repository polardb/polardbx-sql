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
import com.alibaba.polardbx.executor.ddl.job.MockDdlJob;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupDropPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMergePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupSplitPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.MoveDatabasesJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.RefreshTopologyFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMoveDatabases;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRefreshTopology;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.rel.ddl.RefreshTopology;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.lang3.StringUtils;

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
    public static ExecutableDdlJob convertToDDLJob(ExecutionContext ec, String schema, String sql) {
        DDL ddl = parseDdl(ec, sql);
        DdlContext restored = ec.getDdlContext();
        DdlContext tmp = restored == null ? new DdlContext() : restored.copy();

        try {
            // create a faked DdlContext, to pass some variables to low-level tasks
            tmp.setDdlStmt(sql);
            ec.setDdlContext(tmp);

            return convertJob(ec, tmp, schema, ddl);
        } finally {
            ec.setDdlContext(restored);
        }

    }

    public static ExecutableDdlJob convertToDelegatorJob(ExecutionContext ec, String schema, String sql) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        SubJobTask delegator = new SubJobTask(schema, sql, null);
        job.addTask(delegator);
        job.labelAsHead(delegator);
        job.labelAsTail(delegator);
        return job;
    }

    /**
     * Parse sql to ddl job, and save some essential state in the DDLContext
     */
    public static ExecutableDdlJob parseDdlJob(ExecutionContext ec,
                                               DdlContext dc,
                                               String schema,
                                               String sql) {
        if (FailPoint.isKeyEnable(FailPointKey.FP_HIJACK_DDL_JOB)
            && StringUtils.equalsIgnoreCase(sql, FailPointKey.FP_INJECT_SUBJOB)) {
            return new MockDdlJob(5, 5, 30, false).create();
        }
        DDL ddl = parseDdl(ec, sql);
        dc.setDdlStmt(sql);
        return convertJob(ec, dc, schema, ddl);
    }

    private static ExecutableDdlJob convertJob(ExecutionContext ec, DdlContext ddlContext,
                                               String schema, DDL ddl) {
        if (ddl instanceof AlterTableGroupSplitPartition) {
            LogicalAlterTableGroupSplitPartition splitPartition = LogicalAlterTableGroupSplitPartition.create(ddl);
            splitPartition.setSchemaName(schema);
            splitPartition.preparedData();
            ddlContext.setDdlType(splitPartition.getDdlType());
            return AlterTableGroupSplitPartitionJobFactory.create(ddl, splitPartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMergePartition) {
            LogicalAlterTableGroupMergePartition mergePartition = LogicalAlterTableGroupMergePartition.create(ddl);
            mergePartition.setSchemaName(schema);
            mergePartition.preparedData();
            ddlContext.setDdlType(mergePartition.getDdlType());
            return AlterTableGroupMergePartitionJobFactory.create(ddl, mergePartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMovePartition) {
            LogicalAlterTableGroupMovePartition movePartition = LogicalAlterTableGroupMovePartition.create(ddl);
            movePartition.setSchemaName(schema);
            movePartition.preparedData();
            ddlContext.setDdlType(movePartition.getDdlType());
            return AlterTableGroupMovePartitionJobFactory.create(ddl, movePartition.getPreparedData(), ec);
        } else if (ddl instanceof MoveDatabase) {
            LogicalMoveDatabases moveDatabase = LogicalMoveDatabases.create(ddl);
            moveDatabase.setSchemaName(schema);
            moveDatabase.preparedData();
            ddlContext.setDdlType(moveDatabase.getDdlType());
            return MoveDatabasesJobFactory.create(ddl, moveDatabase.getPreparedData(), ec);
        } else if (ddl instanceof RefreshTopology) {
            LogicalRefreshTopology refreshTopology = LogicalRefreshTopology.create(ddl);
            refreshTopology.preparedData(ec);
            ddlContext.setDdlType(refreshTopology.getDdlType());
            return RefreshTopologyFactory.create(refreshTopology.relDdl, refreshTopology.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupDropPartition) {
            LogicalAlterTableGroupDropPartition dropPartition = LogicalAlterTableGroupDropPartition.create(ddl);
            dropPartition.setSchemaName(schema);
            dropPartition.preparedData();
            ddlContext.setDdlType(dropPartition.getDdlType());
            return AlterTableGroupDropPartitionJobFactory.create(ddl, dropPartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTable) {
            AlterTableBuilder alterTableBuilder =
                AlterTableBuilder.createAlterTableBuilder(schema, (AlterTable) ddl, ec);
            DdlPhyPlanBuilder builder = alterTableBuilder.build();
            PhysicalPlanData clusterIndexPlan = builder.genPhysicalPlanData();
            return new AlterTableJobFactory(
                clusterIndexPlan,
                alterTableBuilder.getPreparedData(),
                alterTableBuilder.getLogicalAlterTable(),
                ec).create();
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
