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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
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
import com.alibaba.polardbx.executor.ddl.job.factory.oss.UnArchiveJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.factory.oss.UnArchiveJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.resource.ResourceContainer;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupDropPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMergePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalMoveDatabases;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRefreshTopology;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalUnArchive;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupDropPartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMergePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartition;
import org.apache.calcite.rel.ddl.MoveDatabase;
import org.apache.calcite.rel.ddl.RefreshTopology;
import org.apache.calcite.rel.ddl.UnArchive;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.MOVE_PARTITION_BEFORE_CHECK;

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

    public static ExecutableDdlJob convertToDelegatorJob(String schema, String sql) {
        return convertToDelegatorJob(schema, sql, null, null, null, null);
    }

    public static ExecutableDdlJob convertToDelegatorJob(String schema, String sql,
                                                         CostEstimableDdlTask.CostInfo costInfo) {
        return convertToDelegatorJob(schema, sql, null, null, null, costInfo);
    }

    public static DdlEngineResources buildResourceRequired(String schema, String tableGroupName,
                                                           List<String> partitionNames
        , List<String> resources, String sql) {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner = String.format("schema:%s, sql:%s", schema, sql);
        for (String resource : resources) {
            if (resource.equalsIgnoreCase(MOVE_PARTITION_BEFORE_CHECK)) {
                String subJobOwner = DdlEngineResources.concatSubJobOwner(schema, tableGroupName, partitionNames);
                resourceRequired.requestPhaseLock(resource, 100L, subJobOwner, ResourceContainer.PHASE_LOCK_START);
            } else {
                resourceRequired.requestForce(resource, 100L, owner);
            }
        }
        return resourceRequired;
    }

    public static ExecutableDdlJob convertToDelegatorJob(String schema, String sql, List<String> resources,
                                                         String tableGroupName, List<String> partitionNames,
                                                         CostEstimableDdlTask.CostInfo costInfo) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        SubJobTask delegator = new SubJobTask(schema, sql, null);
        delegator.setCostInfo(costInfo);
        if (resources != null) {
            DdlEngineResources ddlEngineResources =
                buildResourceRequired(schema, tableGroupName, partitionNames, resources, sql);
            delegator.setResourceAcquired(ddlEngineResources);
        }
        job.addTask(delegator);
        job.labelAsHead(delegator);
        job.labelAsTail(delegator);
        delegator.setParentAcquireResource(true);
        return job;
    }

    private static ExecutableDdlJob convertJob(ExecutionContext ec, DdlContext ddlContext,
                                               String schema, DDL ddl) {
        if (ddl instanceof AlterTableGroupSplitPartition) {
            LogicalAlterTableGroupSplitPartition splitPartition = LogicalAlterTableGroupSplitPartition.create(ddl);
            splitPartition.setSchemaName(schema);
            splitPartition.preparedData(ec);
            ddlContext.setDdlType(splitPartition.getDdlType());
            return AlterTableGroupSplitPartitionJobFactory.create(ddl, splitPartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMergePartition) {
            LogicalAlterTableGroupMergePartition mergePartition = LogicalAlterTableGroupMergePartition.create(ddl);
            mergePartition.setSchemaName(schema);
            mergePartition.preparedData(ec);
            ddlContext.setDdlType(mergePartition.getDdlType());
            return AlterTableGroupMergePartitionJobFactory.create(ddl, mergePartition.getPreparedData(), ec);
        } else if (ddl instanceof AlterTableGroupMovePartition) {
            LogicalAlterTableGroupMovePartition movePartition = LogicalAlterTableGroupMovePartition.create(ddl);
            movePartition.setSchemaName(schema);
            boolean usePhysicalBackfill = PhysicalBackfillUtils.isSupportForPhysicalBackfill(schema, ec);
            movePartition.preparedData(ec, usePhysicalBackfill);
            ddlContext.setDdlType(movePartition.getDdlType());
            return AlterTableGroupMovePartitionJobFactory.create(ddl, movePartition.getPreparedData(), ec);
        } else if (ddl instanceof UnArchive) {
            LogicalUnArchive unArchive = LogicalUnArchive.create(ddl);
            unArchive.setSchemaName(schema);
            unArchive.preparedData();
            ddlContext.setDdlType(unArchive.getDdlType());
            return UnArchiveJobFactory.create(unArchive.getPreparedData(), ec);
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
            dropPartition.preparedData(ec);
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

    public static String genRebalanceResourceName(RebalanceTarget target, String name) {
        return LockUtil.genRebalanceResourceName(target, name);
    }

    public static String genRebalanceClusterName() {
        return LockUtil.genRebalanceClusterName();
    }

    public static String genRebalanceTenantResourceName(String tenantName) {
        return LockUtil.genRebalanceResourceName(RebalanceTarget.TENANT, tenantName);
    }

    public static void addConcurrentMovePartitionsTasksBetween(ExecutableDdlJob job, DdlTask tailTask,
                                                               List<BalanceAction> movePartitionsActions,
                                                               ExecutionContext ec) {
        DdlTask head = job.getTail();
        for (BalanceAction action : movePartitionsActions) {
            ExecutableDdlJob subJob = action.toDdlJob(ec);
            job.combineTasks(subJob);
            job.addTaskRelationship(head, subJob.getHead());
            job.addTaskRelationship(subJob.getTail(), tailTask);
        }
        job.labelAsTail(tailTask);
    }

    public static Boolean isMovePartitionAction(BalanceAction action) {
        return action instanceof ActionMovePartition || action instanceof ActionMovePartitions;
    }

    public static ActionMovePartitions mergeActionMovePartitions(String schema,
                                                                 List<BalanceAction> actionMovePartitions) {
        List<Pair<String, List<ActionMovePartition>>> actionMovePartitionsMap = new ArrayList<>();
        for (BalanceAction action : actionMovePartitions) {
            if (!action.getSchema().equalsIgnoreCase(schema)) {
                throw new RuntimeException("unsupported action in schema: " + action);
            }
            if (action instanceof ActionMovePartition) {
                actionMovePartitionsMap.add(Pair.of(((ActionMovePartition) action).getTableGroupName(),
                    Lists.newArrayList((ActionMovePartition) action)));
            } else if (action instanceof ActionMovePartitions) {
                ActionMovePartitions actionMovePartition = (ActionMovePartitions) action;
                actionMovePartitionsMap.addAll(actionMovePartition.getActions());
            } else {
                throw new RuntimeException("unsupported action here, expected move partition: " + action);
            }
        }
        ActionMovePartitions actionMovePartitionsResult = new ActionMovePartitions(schema, actionMovePartitionsMap);
        return actionMovePartitionsResult;
    }
}
