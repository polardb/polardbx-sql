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

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.FirstThenOtherCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupSequentialCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.GenericPhyObjectRecorder;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.spi.PlanHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.utils.NewGroupKey;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalUpsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.GroupConnId;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyDdlTableCursor;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.util.RexMemoryLimitHelper;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by chuanqin on 17/7/7.
 */
public abstract class HandlerCommon implements PlanHandler {

    protected final static IdGenerator JOB_ID_GENERATOR = IdGenerator.getIdGenerator();

    protected IRepository repo;

    protected static final int MAX_FK_DEPTH = 15;

    public HandlerCommon() {
    }

    public HandlerCommon(IRepository repo) {
        this.repo = repo;
    }

    @Override
    public Cursor handlePlan(RelNode logicalPlan, ExecutionContext executionContext) {
        Cursor cursor = handle(logicalPlan, executionContext);
        RuntimeStatHelper.registerCursorStatForPlan(logicalPlan, executionContext, cursor);
        return cursor;
    }

    public abstract Cursor handle(RelNode logicalPlan, ExecutionContext executionContext);

    private void executeSubNodesBlockConcurrent(ExecutionContext executionContext, List<RelNode> subNodes,
                                                List<Cursor> subCursors, String schemaName) {
        if (subNodes != null && subNodes.isEmpty()) {
            return;
        }
        checkExecMemCost(executionContext, subNodes);
        RelNode firstOp = subNodes.get(0);
        boolean isPhyTblOp = firstOp instanceof PhyTableOperation;
        boolean isSystemDb = SystemDbHelper.isDBBuildIn(schemaName);

        /**
         *  zigzag inputs by (dnInst,group,connId)
         *    dn1        dn2        dn3 ...
         * { grp1.conn1,grp2.conn2,grp1.conn2,grp2.conn2 ... }
         */
        List<GroupConnId> groupConnIdSet = new ArrayList<>();
        List<List<RelNode>> groupedInputs = null;
        List<RelNode> newInputsAfterZigzag = new ArrayList<>();
        if (isPhyTblOp && !isSystemDb) {
            newInputsAfterZigzag =
                ExecUtils.zigzagInputsByBothDnInstAndGroupConnId(subNodes, schemaName, executionContext, groupConnIdSet,
                    groupedInputs);
        } else {
            newInputsAfterZigzag = ExecUtils.zigzagInputsByMysqlInst(subNodes, schemaName, executionContext);
        }

        int prefetch = executionContext.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS);
        if (prefetch < 0) {
            // By default, #prefetch_shards = MIN( #involved_groups , #cores * 4 )
            //long groupCount = subNodes.stream().map(q -> ((BaseQueryOperation) q).getDbIndex()).distinct().count();
            long maxParallelism = groupConnIdSet.size();
            if (!isPhyTblOp) {
                maxParallelism = subNodes.stream().map(q -> ((BaseQueryOperation) q).getDbIndex()).distinct().count();
                ;
            }
            prefetch = ExecUtils.getPrefetchNumForLogicalView((int) maxParallelism);
        }

        List<Future<Cursor>> futures = new ArrayList<>(newInputsAfterZigzag.size());
        List<Throwable> exceptions = new ArrayList<>();

        // For DDL only
        Map<String, GenericPhyObjectRecorder> phyObjectRecorderMap = new ConcurrentHashMap<>();

        /*
         * Execute and collect cursors in a sliding window:
         *
         * For example, assuming prefetch = 4,
         *
         *    RelNodes: 0 1 2 3 4 5 6 7
         *                        ^ execute: Execute this RelNode asynchronously
         *                ^ collect: Get the result from future
         *
         */
        for (int execute = 0, collect = -prefetch; collect < newInputsAfterZigzag.size(); execute++, collect++) {
            if (execute < newInputsAfterZigzag.size()) {
                final RelNode subNode = newInputsAfterZigzag.get(execute);
                GenericPhyObjectRecorder phyObjectRecorder =
                    CrossEngineValidator.getPhyObjectRecorder(subNode, executionContext);
                if (!phyObjectRecorder.checkIfDone()) {
                    Future<Cursor> rcfuture = ExecutorContext.getContext(schemaName)
                        .getTopologyExecutor()
                        .execByExecPlanNodeFuture(subNode, executionContext, null);
                    futures.add(rcfuture);
                    if (subNode instanceof PhyDdlTableOperation) {
                        String phyTableKey = DdlHelper.genPhyTableInfo(subNode, executionContext.getDdlContext());
                        if (TStringUtil.isNotBlank(phyTableKey)) {
                            phyObjectRecorderMap.put(phyTableKey, phyObjectRecorder);
                        }
                    }
                } else {
                    // Add null as placeholder to skip recording during recovery below.
                    futures.add(null);
                }
            }
            if (collect >= 0) {
                GenericPhyObjectRecorder phyObjectRecorder = null;
                final Future<Cursor> future = futures.get(collect);
                if (future == null) {
                    // The shard has been done.
                    continue;
                }
                try {
                    Cursor cursor = future.get();
                    subCursors.add(cursor);
                    if (cursor instanceof MyPhyDdlTableCursor) {
                        RelNode subNode = ((MyPhyDdlTableCursor) cursor).getRelNode();
                        String phyTableKey = DdlHelper.genPhyTableInfo(subNode, executionContext.getDdlContext());
                        if (TStringUtil.isNotBlank(phyTableKey)) {
                            phyObjectRecorder = phyObjectRecorderMap.get(phyTableKey);
                            if (phyObjectRecorder != null) {
                                phyObjectRecorder.recordDone();
                            }
                        }
                    }
                } catch (Exception e) {
                    if (phyObjectRecorder == null || !phyObjectRecorder.checkIfIgnoreException(e)) {
                        exceptions.add(new TddlException(e));
                    }
                }
            }
        }

        if (!GeneralUtil.isEmpty(exceptions)) {
            throw GeneralUtil.mergeException(exceptions);
        }
    }

    private void executeFirstThenBlockConcurrent(ExecutionContext executionContext, List<RelNode> subNodes,
                                                 List<Cursor> subCursors, String schemaName) {

        List<RelNode> newSubNodes = ExecUtils.zigzagInputsByMysqlInst(subNodes, schemaName, executionContext);
        HashSet<Integer> cursorIndexSet = new HashSet<>();
        for (int i = 0; i < newSubNodes.size(); i++) {
            cursorIndexSet.add(i);
        }
        FirstThenOtherCursor.Synchronizer synchronizer =
            new FirstThenOtherCursor.Synchronizer(schemaName, newSubNodes, cursorIndexSet, executionContext);
        for (int i = 0; i < newSubNodes.size(); i++) {
            FirstThenOtherCursor firstAndOtherCursorsAdapter =
                new FirstThenOtherCursor(synchronizer, i);
            subCursors.add(firstAndOtherCursorsAdapter);
        }
    }

    /**
     * "group" here stands for groups in concurrent execution, not the group in
     * three layer data source.
     */
    protected void concurrentGroups(ExecutionContext executionContext, List<Cursor> subCursors,
                                    Map<String, List<RelNode>> groupAndPlans, String schemaName) {
        int index = 0;
        while (true) {
            List<RelNode> oneConcurrentGroup = new ArrayList<>();
            for (List<RelNode> qcs : groupAndPlans.values()) {
                if (index >= qcs.size()) {
                    continue;
                }
                oneConcurrentGroup.add(qcs.get(index));
            }

            if (oneConcurrentGroup.isEmpty()) {
                break;
            }

            GroupConcurrentUnionCursor groupConcurrentUnionCursor = new GroupConcurrentUnionCursor(schemaName,
                oneConcurrentGroup,
                executionContext);
            subCursors.add(groupConcurrentUnionCursor);
            // Eager initialization was removed here
            index++;
        }
    }

    protected void executeGroupConcurrent(ExecutionContext executionContext, List<RelNode> subNodes,
                                          List<Cursor> subCursors, String schemaName) {
        /**
         * Serialized execution in group，concurrent execution among groups
         *
         * <pre>
         *             group1    group2   group3
         * ---------------------------------------
         * cursor0      t10       t20      t30
         * ---------------------------------------
         * cursor1      t11       t21      t31
         * ---------------------------------------
         * cursor2      t12       t22      t32
         * ---------------------------------------
         * </pre>
         */

        /**
         * Grouping the execution plan according to data node
         */
        Map<String, List<RelNode>> groupAndQcs = new HashMap<>();
        List<RelNode> execSubNodes = new ArrayList<>();
        for (RelNode q : subNodes) {

            String groupName = ((BaseQueryOperation) q).getDbIndex();
            List<RelNode> qcs = groupAndQcs.get(groupName);
            if (qcs == null) {
                qcs = new ArrayList<>();
                groupAndQcs.put(groupName, qcs);
                execSubNodes.add(q);
            }

            qcs.add(q);
        }
        checkExecMemCost(executionContext, execSubNodes);
        concurrentGroups(executionContext, subCursors, groupAndQcs, schemaName);
    }

    protected void executeRelaxedGroupConcurrent(ExecutionContext executionContext, List<RelNode> subNodes,
                                                 List<Cursor> subCursors, String schemaName) {
        /**
         * parallel execution intra one group:
         *
         * <pre>
         *          execNum            grp1-1(dn1)    grp2-1(dn2)    |    grp1-2(dn1)    grp2-2(dn2)
         * ---------------------------------------------------------------------------------------------
         * GrpIntraParallelCursor0      p1(dn1)         p2(dn2)              p3(dn1)        p4(dn2)
         *      (preFetchSize=4)
         * ---------------------------------------------------------------------------------------------
         * GrpIntraParallelCursor1      p5(dn1)         p6(dn2)              p7(dn1)        p8(dn2)
         *      (preFetchSize=4)
         * ---------------------------------------------------------------------------------------------
         * GrpIntraParallelCursor2      p9(dn1)         p10(dn2)            p11(dn1)       p12(dn2)
         *      (preFetchSize=4)
         * ---------------------------------------------------------------------------------------------
         *
         * , so  p1(dn1) and  p3(dn1) will perform parallel execution in Grp1(grp1 is in dn1).
         *
         * </pre>
         */

        /**
         * Grouping the execution plan according to data node
         */
        List<GroupConnId> groupConnIdList = new ArrayList<>();
        List<List<RelNode>> phyInputByGrpConnId = new ArrayList<>();
        if (subNodes.isEmpty()) {
            return;
        }
        RelNode firstOp = subNodes.get(0);
        boolean isPhyTblOp = firstOp instanceof PhyTableOperation;
        boolean isSystemDb = SystemDbHelper.isDBBuildIn(schemaName);
        if (isPhyTblOp && !isSystemDb) {
            List<RelNode> allSubNodes =
                ExecUtils.zigzagInputsByBothDnInstAndGroupConnId(subNodes, schemaName, executionContext,
                    groupConnIdList, phyInputByGrpConnId);
            checkExecMemCost(executionContext, allSubNodes);
            concurrentByGroupIntraParallel(executionContext, subCursors, phyInputByGrpConnId, schemaName);
        } else {
            executeGroupConcurrent(executionContext, subNodes, subCursors, schemaName);
        }

    }

    /**
     * "group" here stands for groups in concurrent execution, not the group in
     * three layer data source.
     */
    protected void concurrentByGroupIntraParallel(ExecutionContext executionContext, List<Cursor> subCursors,
                                                  List<List<RelNode>> groupedInputByGrpConnId, String schemaName) {

        int maxPhyOpList = 0;
        for (int i = 0; i < groupedInputByGrpConnId.size(); i++) {
            if (groupedInputByGrpConnId.get(i).size() > maxPhyOpList) {
                maxPhyOpList = groupedInputByGrpConnId.get(i).size();
            }
        }
        for (int j = 0; j < maxPhyOpList; j++) {
            List<RelNode> oneConcurrentGroup = new ArrayList<>();
            for (int i = 0; i < groupedInputByGrpConnId.size(); i++) {
                List<RelNode> inputs = groupedInputByGrpConnId.get(i);
                if (j < inputs.size()) {
                    oneConcurrentGroup.add(inputs.get(j));
                }
            }
            if (oneConcurrentGroup.isEmpty()) {
                break;
            }
            GroupConcurrentUnionCursor groupConcurrentUnionCursor = new GroupConcurrentUnionCursor(schemaName,
                oneConcurrentGroup,
                executionContext);
            subCursors.add(groupConcurrentUnionCursor);
        }
    }

    private void executeByInstance(Map<String, List<RelNode>> plansByInstance, List<Cursor> subCursors,
                                   ExecutionContext ec, String schemaName, List<Throwable> exceptions) {
        GroupSequentialCursor groupSequentialCursor = new GroupSequentialCursor(plansByInstance,
            ec,
            schemaName,
            exceptions);
        subCursors.add(groupSequentialCursor);
        groupSequentialCursor.init();
    }

    /**
     * 实例间并行. 同一实例中串行,实例以ip,port为划分
     */
    protected void executeInstanceConcurrent(List<RelNode> subNodes, List<Cursor> subCursors,
                                             ExecutionContext ec, String schemaName, List<Throwable> exceptions) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        IRepository myRepo = executorContext.getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.name());

        Map<String, List<RelNode>> plansByInstance = new HashMap<>();
        for (RelNode subNode : subNodes) {
            Object possibleTGroupDataSource = myRepo.getGroupExecutor(optimizerContext.getMatrix()
                .getGroup(((BaseQueryOperation) subNode).getDbIndex())).getDataSource();
            if (!(possibleTGroupDataSource instanceof TGroupDataSource)) {
                throw new TddlNestableRuntimeException("Unsupported datasource for INSTANCE_CONCURRENT");
            }

            // ip + port = id
            String instanceId = null;
            for (Map.Entry<TAtomDataSource, Weight> weightEntry : ((TGroupDataSource) possibleTGroupDataSource)
                .getAtomDataSourceWeights()
                .entrySet()) {
                if (weightEntry.getValue().w != 0) {
                    instanceId = weightEntry.getKey().getHost() + ":" + weightEntry.getKey().getPort();
                    break;
                }
            }
            if (instanceId == null) {
                throw new TddlNestableRuntimeException("Unsupported group without writable database");
            }

            List<RelNode> plans = plansByInstance.get(instanceId);
            if (plans == null) {
                plans = new ArrayList<>();
                plansByInstance.put(instanceId, plans);
            }
            plans.add(subNode);
        }

        executeByInstance(plansByInstance, subCursors, ec, schemaName, exceptions);
    }

    protected void executeWithConcurrentPolicy(ExecutionContext executionContext, List<RelNode> inputs,
                                               QueryConcurrencyPolicy queryConcurrencyPolicy,
                                               List<Cursor> inputCursors, String schemaName) {
        switch (queryConcurrencyPolicy) {
        case GROUP_CONCURRENT_BLOCK:
            executeGroupConcurrent(executionContext, inputs, inputCursors, schemaName);
            break;
        case CONCURRENT:
            // full concurrent
            executeSubNodesBlockConcurrent(executionContext, inputs, inputCursors, schemaName);
            break;
        case RELAXED_GROUP_CONCURRENT:
            // relaxed group concurrent
            executeRelaxedGroupConcurrent(executionContext, inputs, inputCursors, schemaName);
            break;
        case FIRST_THEN_CONCURRENT:
            // for broadcast table write
            executeFirstThenBlockConcurrent(executionContext, inputs, inputCursors, schemaName);
            break;
        default: // sequential
            ITopologyExecutor executor = ExecutorContext.getContext(schemaName).getTopologyExecutor();
            for (RelNode relNode : inputs) {
                GenericPhyObjectRecorder phyObjectRecorder =
                    CrossEngineValidator.getPhyObjectRecorder(relNode, executionContext);
                if (!phyObjectRecorder.checkIfDone()) {
                    try {
                        inputCursors.add(executor.execByExecPlanNode(relNode, executionContext));
                        phyObjectRecorder.recordDone();
                    } catch (Throwable t) {
                        if (!phyObjectRecorder.checkIfIgnoreException(t)) {
                            throw t;
                        }
                    }
                }
            }
        }
    }

    private void checkExecMemCost(ExecutionContext executionContext, List<RelNode> subNodes) {
        long memCost = 0;
        for (RelNode relNode : subNodes) {
            if (relNode instanceof PhyTableOperation) {
                PhyTableOperation phyTableOp = (PhyTableOperation) relNode;
                memCost += phyTableOp.getExecMemCost();
            }
        }

        // change to MBs from bytes
        memCost >>= 20;

        // 单位: MB
        long maxExecuteMem = executionContext.getParamManager().getLong(ConnectionParams.MAX_EXECUTE_MEMORY);
        if (memCost > maxExecuteMem) {
            if (RexMemoryLimitHelper.ENABLE_REX_MEMORY_LIMIT) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXCEED_MAX_EXECUTE_MEMORY,
                    String.valueOf(memCost),
                    String.valueOf(maxExecuteMem));
            }
        }
    }

    protected int execute(DistinctWriter writer, RowSet rowSet, ExecutionContext executionContext) {
        return execute(writer, rowSet::distinctRowSetWithoutNull, executionContext);
    }

    protected int execute(DistinctWriter writer,
                          Function<DistinctWriter, List<List<Object>>> rowGenerator,
                          ExecutionContext executionContext) {
        final RelOptTable targetTable = writer.getTargetTable();

        List<RelNode> inputs = writer.getInput(executionContext, rowGenerator);
        final List<RelNode> primaryPhyPlan =
            inputs.stream().filter(o -> !((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());
        final List<RelNode> allPhyPlan = new ArrayList<>(primaryPhyPlan);
        final List<RelNode> replicatePlans =
            inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                Collectors.toList());
        allPhyPlan.addAll(replicatePlans);

        boolean isBroadcast = false;
        if (writer instanceof BroadcastWriter) {
            isBroadcast = true;
        }
        int affectedRows =
            executePhysicalPlan(allPhyPlan, executionContext, RelUtils.getSchemaName(targetTable), isBroadcast);

        boolean multiWriteWithoutBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && !isBroadcast;
        boolean multiWriteWithBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && isBroadcast;

        if (multiWriteWithoutBroadcast) {
            return primaryPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows())
                .sum();
        } else if (multiWriteWithBroadcast) {
            return ((BaseQueryOperation) primaryPhyPlan.get(0)).getAffectedRows();
        } else {
            return affectedRows;
        }
    }

    protected List<List<Object>> execute(RelocateWriter relocateWriter, RowSet rowSet,
                                         ExecutionContext executionContext) {
        final ExecutionContext insertEc = executionContext.copy();
        final String schemaName = RelUtils.getSchemaName(relocateWriter.getTargetTable());

        final BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> skComparator =
            (w, p) -> {
                // Use PartitionField to compare in new partition table
                final List<Object> row = p.getKey();
                final RelocateWriter rw = w.unwrap(RelocateWriter.class);
                final boolean usePartFieldChecker = rw.isUsePartFieldChecker() &&
                    executionContext.getParamManager().getBoolean(ConnectionParams.DML_USE_NEW_SK_CHECKER);
                final boolean checkJsonByStringCompare =
                    executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);

                final List<Object> skSources = Mappings.permute(row, rw.getIdentifierKeySourceMapping());
                final List<Object> skTargets = Mappings.permute(row, rw.getIdentifierKeyTargetMapping());

                if (usePartFieldChecker) {
                    final List<ColumnMeta> sourceColMetas =
                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeySourceMapping());
//                    final List<ColumnMeta> targetColMetas =
//                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeyTargetMapping());

                    try {
                        final NewGroupKey skSourceKey = new NewGroupKey(skSources,
                            sourceColMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);
                        final NewGroupKey skTargetKey = new NewGroupKey(skTargets,
                            skTargets.stream().map(DataTypeUtil::getTypeOfObject).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);

                        return skSourceKey.equals(skTargetKey);
                    } catch (Throwable e) {
                        if (!relocateWriter.printed &&
                            executionContext.getParamManager().getBoolean(ConnectionParams.DML_PRINT_CHECKER_ERROR)) {
                            // Maybe value can not be cast, just use DELETE + INSERT to be safe
                            EventLogger.log(EventType.DML_ERROR,
                                executionContext.getTraceId() + " new sk checker failed, cause by " + e);
                            LoggerFactory.getLogger(HandlerCommon.class).warn(e);
                            relocateWriter.printed = true;
                        }
                    }
                    return false;
                } else {
                    final GroupKey skTargetKey = new GroupKey(skTargets.toArray(), rw.getIdentifierKeyMetas());
                    final GroupKey skSourceKey = new GroupKey(skSources.toArray(), rw.getIdentifierKeyMetas());

                    return skTargetKey.equalsForUpdate(skSourceKey, checkJsonByStringCompare);
                }
            };

        final List<RelNode> deletePlans = new ArrayList<>();
        final List<RelNode> insertPlans = new ArrayList<>();
        final List<RelNode> updatePlans = new ArrayList<>();
        final List<RelNode> replicateDeletePlans = new ArrayList<>();
        final List<RelNode> replicateInsertPlans = new ArrayList<>();
        final List<RelNode> replicateUpdatePlans = new ArrayList<>();
        final SourceRows distinctRows = relocateWriter.getInput(executionContext, insertEc,
            (w) -> SourceRows.createFromSelect(rowSet.distinctRowSetWithoutNull(w)),
            (writer, selectedRows, result) -> writer.classify(skComparator, selectedRows, executionContext, result),
            deletePlans, insertPlans, updatePlans, replicateDeletePlans, replicateInsertPlans, replicateUpdatePlans);

        long oldLastInsertId = executionContext.getConnection().getLastInsertId();

        // Execute update
        updatePlans.addAll(replicateUpdatePlans);
        executePhysicalPlan(updatePlans, executionContext, schemaName, false);

        // Execute delete
        deletePlans.addAll(replicateDeletePlans);
        executePhysicalPlan(deletePlans, executionContext, schemaName, false);

        // Execute insert
        insertPlans.addAll(replicateInsertPlans);
        final QueryConcurrencyPolicy queryConcurrencyPolicy = insertPlans.size() > 1 ? getQueryConcurrencyPolicy(
            insertEc) : QueryConcurrencyPolicy.SEQUENTIAL;

        Long phySqlId = executionContext.getPhySqlId();
        insertEc.setPhySqlId(phySqlId++);

        final List<Cursor> inputCursors = new ArrayList<>(insertPlans.size());
        executeWithConcurrentPolicy(insertEc,
            insertPlans,
            queryConcurrencyPolicy,
            inputCursors,
            schemaName);

        // Update physical sql id for next physical sql group
        executionContext.setPhySqlId(phySqlId);

        if (relocateWriter.getOperation() == TableModify.Operation.UPDATE && updatePlans.size() == 0) {
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
        }

        ExecUtils.getAffectRowsByCursors(inputCursors, false);

        return distinctRows.selectedRows;
    }

    protected List<Cursor> execute(List<RelNode> inputs, ExecutionContext ec, String schemaName) {
        QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(ec);

        // We are here for checking PK/UK/FK, so maybe primary is broadcast table, but tables for selections are not
        if (queryConcurrencyPolicy == QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT && !canUseFirstThenConcurrent(
            inputs)) {
            queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
        }
        if (inputs.size() == 1) {
            queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;
        }

        final List<Cursor> inputCursors = new ArrayList<>(inputs.size());
        executeWithConcurrentPolicy(ec, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
        return inputCursors;
    }

    /**
     * Execute physical plans for each table.
     *
     * @return affected rows
     */
    protected int executePhysicalPlan(List<RelNode> physicalPlans, ExecutionContext executionContext, String schemaName,
                                      boolean isBroadcast) {
        QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext);
        // If there's a broadcast table, the concurrency will be set to
        // FIRST_THEN. But when modifying multi tb, the concurrency can't be
        // FIRST_THEN, which causes concurrent transaction error.

        if (queryConcurrencyPolicy == QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT && (!isBroadcast
            || !canUseFirstThenConcurrent(physicalPlans))) {
            queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
            if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM)) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.RELAXED_GROUP_CONCURRENT;
            }
        }
        if (physicalPlans.size() == 1) {
            queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;
        }

        final List<Cursor> inputCursors = new ArrayList<>(physicalPlans.size());
        executeWithConcurrentPolicy(executionContext, physicalPlans, queryConcurrencyPolicy, inputCursors, schemaName);

        int affectRows = ExecUtils.getAffectRowsByCursors(inputCursors, isBroadcast);

        // Increase physical sql id
        executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);
        return affectRows;
    }

    protected boolean canUseFirstThenConcurrent(List<RelNode> physicalPlans) {
        // TODO(qianjing): Maybe we should check it in TConnection
        Set<String> groups = new HashSet<>();
        for (RelNode plan : physicalPlans) {
            String groupName = ((BaseQueryOperation) plan).getDbIndex();
            if (groups.contains(groupName)) {
                return false;
            }
            groups.add(groupName);
        }
        return true;
    }

    /**
     * Execute physical query for each table
     *
     * @return Query result
     */
    protected List<List<Object>> executePhysicalPlan(List<RelNode> physicalPlans, String schemaName,
                                                     ExecutionContext ec,
                                                     Consumer<Integer> memoryAllocator) {
        final List<Cursor> cursors = execute(physicalPlans, ec, schemaName);

        final List<List<Object>> duplicateValues = new ArrayList<>();

        try {
            for (Ord o : Ord.zip(cursors)) {
                final Cursor cursor = (Cursor) o.e;
                final List<List<Object>> queryResult = getQueryResult(cursor, memoryAllocator);

                duplicateValues.addAll(queryResult);

                cursor.close(new ArrayList<>());
                cursors.set(o.i, null);
            }
        } finally {
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }
        return duplicateValues;
    }

    /**
     * Query values by cursor
     *
     * @param cursor select cursor
     * @return received rows
     */
    protected static List<List<Object>> getQueryResult(Cursor cursor,
                                                       Consumer<Integer> memoryAllocator) {
        int rowCount = 0;
        final List<List<Object>> values = new ArrayList<>();
        Row rs = null;
        while ((rs = cursor.next()) != null) {
            // Allocator memory
            if ((++rowCount) % TddlConstants.DML_SELECT_BATCH_SIZE_DEFAULT == 0) {
                memoryAllocator.accept(rowCount);
                rowCount = 0;
            }

            final List<Object> rawValues = rs.getValues();
            final List<Object> outValues = new ArrayList<>(rawValues.size());
            final List<ColumnMeta> columnMetas = cursor.getReturnColumns();
            for (int i = 0; i < rawValues.size(); i++) {
                outValues.add(DataTypeUtil.toJavaObject(GeneralUtil.isNotEmpty(columnMetas) ? columnMetas.get(i) : null,
                    rawValues.get(i)));
            }
            values.add(outValues);
        }

        return values;
    }

    /**
     * Query values by cursor
     *
     * @param cursor select cursor
     * @param batchSize how many rows to receive
     * @return received rows
     */
    protected static List<List<Object>> selectForModify(Cursor cursor, long batchSize,
                                                        Consumer<Long> consumer, long memoryOfOneRow) {
        List<List<Object>> values = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            Row rs = cursor.next();
            if (rs == null) {
                break;
            }

            final List<Object> columnValues = rs.getValues();
            // Convert inner type to JDBC types
            List<ColumnMeta> columnMetas = cursor.getReturnColumns();
            final List<Object> convertedColumnValues = new ArrayList<>();
            for (int j = 0; j < columnValues.size(); j++) {
                convertedColumnValues.add(
                    DataTypeUtil.toJavaObject(GeneralUtil.isNotEmpty(columnMetas) ? columnMetas.get(j) : null,
                        columnValues.get(j)));
            }

            values.add(convertedColumnValues);

            try {
                long rowSize = rs.estimateSize();
                if (rowSize > 0) {
                    consumer.accept(rowSize);
                } else {
                    consumer.accept(memoryOfOneRow);
                }
            } catch (MemoryNotEnoughException t) {
                //return some values earlier, and the values will be do the update/delete operation
                break;
            }
        }

        return values;
    }

    protected static ExecutionContext clearSqlMode(ExecutionContext executionContext) {
        // fix the data truncate issue
        final Map<String, Object> serverVariables = executionContext.getServerVariables();
        executionContext.setServerVariables(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        if (null != serverVariables) {
            executionContext.getServerVariables().putAll(serverVariables);
        }
        executionContext.getServerVariables().put("sql_mode", "NO_AUTO_VALUE_ON_ZERO");
        return executionContext;
    }

    public static ExecutionContext setChangeSetApplySqlMode(ExecutionContext executionContext) {
        // fix the data truncate issue
        final Map<String, Object> serverVariables = executionContext.getServerVariables();
        executionContext.setServerVariables(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        if (null != serverVariables) {
            executionContext.getServerVariables().putAll(serverVariables);
        }
        String sqlMode = (String) executionContext.getServerVariables().get("sql_mode");
        if (sqlMode != null && !sqlMode.isEmpty() && (StringUtils.containsIgnoreCase(sqlMode, "STRICT_ALL_TABLES")
            || StringUtils.containsIgnoreCase(sqlMode, "STRICT_TRANS_TABLES"))) {
            sqlMode = "STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO";
        } else {
            sqlMode = "NO_AUTO_VALUE_ON_ZERO";
        }
        executionContext.getServerVariables().put("sql_mode", sqlMode);
        return executionContext;
    }

    public static void upgradeEncoding(ExecutionContext executionContext, String schemaName, String baseTableName) {

        final Map<String, CollationName> columnCharacterSet =
            getColumnCharacterSet(executionContext, schemaName, baseTableName);

        if (columnCharacterSet.isEmpty()) {
            return;
        }

        CollationName mixedCollationName = columnCharacterSet.values().iterator().next();
        for (CollationName collation : columnCharacterSet.values()) {
            final CollationName mixed = CollationName.getMixOfCollation0(mixedCollationName, collation);

            if (null == mixed) {
                final String charset1 = CollationName.getCharsetOf(mixedCollationName).getJavaCharset();
                final String charset2 = CollationName.getCharsetOf(collation).getJavaCharset();
                if (!TStringUtil.equalsIgnoreCase(charset1, charset2)) {
                    throw new UnsupportedOperationException(
                        "cannot get correct character for columns of " + baseTableName + ". " + mixedCollationName
                            .name() + ", " + collation.name());
                }
            } else {
                mixedCollationName = mixed;
            }
        }

        executionContext.setEncoding(CollationName.getCharsetOf(mixedCollationName).getJavaCharset());
    }

    protected static Map<String, CollationName> getColumnCharacterSet(
        ExecutionContext executionContext, String schemaName, String baseTableName) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        final Map<String, CollationName> columnCharacterSetMap = new HashMap<>();

        for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
            final String collationStr = columnMeta.getField().getCollationName();
            if (StringUtils.isEmpty(collationStr)) {
                continue;
            }
            final CollationName collationName = CollationName.of(collationStr);
            columnCharacterSetMap.put(columnMeta.getName(), collationName);
        }
        return columnCharacterSetMap;
    }

    public int doParallelExecute(ParallelExecutor parallelExecutor, List<List<Object>> someValues, long someValuesSize,
                                 Cursor selectCursor, long batchSize, long memoryOfOneRow) {
        if (parallelExecutor == null) {
            return 0;
        }
        int affectRows;
        try {
            parallelExecutor.start();

            //试探values时，获取的someValues
            if (someValues != null && !someValues.isEmpty()) {
                someValues.add(Collections.singletonList(someValuesSize));
                parallelExecutor.getSelectValues().put(someValues);
            }

            // Select and DML loop
            do {
                AtomicLong batchRowSize = new AtomicLong(0L);
                List<List<Object>> values =
                    selectForModify(selectCursor, batchSize, batchRowSize::getAndAdd, memoryOfOneRow);

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowSize.get());
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowSize.get()));
                parallelExecutor.getSelectValues().put(values);
            } while (true);

            //正常情况等待执行完
            affectRows = parallelExecutor.waitDone();

        } catch (Throwable t) {
            parallelExecutor.failed(t);

            throw GeneralUtil.nestedException(t);
        } finally {
            parallelExecutor.doClose();
        }
        return affectRows;
    }

    /**
     * 检查 UPDATE/DELETE limit m,n; 支持 limit 0,n; 默认关闭 limit m,n ( m > 0);
     */
    protected static void checkUpdateDeleteLimitLimitation(SqlNode originNode, ExecutionContext ec) {
        if (originNode != null) {
            SqlNode offset = null;
            if (originNode instanceof SqlDelete && ((SqlDelete) originNode).getOffset() != null) {
                offset = ((SqlDelete) originNode).getOffset();
            }
            if (originNode instanceof SqlUpdate && ((SqlUpdate) originNode).getOffset() != null) {
                offset = ((SqlUpdate) originNode).getOffset();
            }
            if (offset != null) {
                boolean offsetCanNotZero =
                    ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO);
                if ((offset instanceof SqlDynamicParam) && ec.getParams() != null) {
                    int index = ((SqlDynamicParam) offset).getIndex() + 1;
                    Object value = ec.getParams().getCurrentParameter().get(index).getValue();
                    if (offsetCanNotZero || (value instanceof Integer && value.equals(0))) {
                        return;
                    }
                    throw new UnsupportedOperationException(
                        "Default Forbid UPDATE/DELETE statement with offset not 0. You can use hint (ENABLE_MODIFY_LIMIT_OFFSET_NOT_ZERO=true) to execute.");
                }
                //其它未知类型
                throw new UnsupportedOperationException(
                    "Does not support UPDATE/DELETE statement with offset:" + offset);
            }
        }

    }

    protected List<List<Object>> getSelectValues(ExecutionContext executionContext,
                                                 String schemaName, TableMeta refTableMeta,
                                                 List<List<Object>> conditionValueList,
                                                 TableModify logicalModify, MemoryAllocatorCtx memoryAllocator,
                                                 PhysicalPlanBuilder builder,
                                                 Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults,
                                                 List<String> columns, boolean isUnion) {
        // query the data to be updated or deleted
        List<String> selectKeys =
            refTableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());

        List<String> selectKeysLower = selectKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> columnsLower = columns.stream().map(String::toLowerCase).collect(Collectors.toList());
        if (!new HashSet<>(selectKeysLower).containsAll(columnsLower)) {
            columnsLower.removeAll(selectKeysLower);
            throw new TddlRuntimeException(ErrorCode.ERR_FK_REFERENCING_COLUMN_NOT_EXIST, columnsLower.get(0),
                refTableMeta.getSchemaName(), refTableMeta.getTableName());
        }

        List<RelNode> phyTableOperations = !isUnion ?
            builder.buildSelectForCascade(refTableMeta, shardResults, selectKeys,
                logicalModify, columns, conditionValueList) :
            builder.buildSelectForCascadeUnionAll(refTableMeta, shardResults, columns,
                logicalModify, columns, conditionValueList);

        RelDataType selectRowType = getRowTypeForColumns(logicalModify,
            refTableMeta.getPhysicalColumns());

        return executePhysicalPlan(
            phyTableOperations,
            schemaName,
            executionContext,
            (rowCount) -> memoryAllocator
                .allocateReservedMemory(MemoryEstimator.calcSelectValuesMemCost(rowCount, selectRowType))
        );

    }

    protected List<List<Object>> getConditionValueList(ForeignKeyData data, TableMeta tableMeta,
                                                       List<List<Object>> values,
                                                       List<String> updateColumnList,
                                                       List<String> insertColumnList,
                                                       boolean isFront) {
        List<String> columns = isFront ? data.columns : data.refColumns;

        List<String> tableColumns = insertColumnList == null ?
            tableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList()) : insertColumnList;

        List<List<Object>> conditionValueList = new ArrayList<>();
        List<Integer> refColIndex = new ArrayList<>();
        for (Ord<String> o : Ord.zip(tableColumns)) {
            if (columns.stream().anyMatch(colName -> colName.equalsIgnoreCase(o.getValue()))) {
                if (updateColumnList == null) {
                    refColIndex.add(o.getKey());
                } else if (updateColumnList.stream().anyMatch(colName -> colName.equalsIgnoreCase(o.getValue()))) {
                    refColIndex.add(o.getKey());
                }
            }
        }
        for (List<Object> row : values) {
            List<Object> conditionValue = new ArrayList<>();
            for (Integer colIndex : refColIndex) {
                conditionValue.add(row.get(colIndex));
            }
            conditionValueList.add(conditionValue);
        }
        return conditionValueList;
    }

    // when check front, tableMeta is child table, check back, tableMeta is parent table
    protected List<String> getSortedColumns(boolean isFront, TableMeta tableMeta, ForeignKeyData data) {
        Map<String, String> columnMap = isFront ?
            IntStream.range(0, data.columns.size()).collect(TreeMaps::caseInsensitiveMap,
                (m, i) -> m.put(data.columns.get(i), data.refColumns.get(i)), Map::putAll) :
            IntStream.range(0, data.columns.size()).collect(TreeMaps::caseInsensitiveMap,
                (m, i) -> m.put(data.refColumns.get(i), data.columns.get(i)), Map::putAll);

        List<String> sortedColumns = new ArrayList<>();
        if (isFront) {
            tableMeta.getAllColumns().forEach(c -> {
                if (data.columns.stream().anyMatch(c.getName()::equalsIgnoreCase)) {
                    sortedColumns.add(columnMap.get(c.getName()));
                }
            });
        } else {
            tableMeta.getAllColumns().forEach(c -> {
                if (data.refColumns.stream().anyMatch(c.getName()::equalsIgnoreCase)) {
                    sortedColumns.add(columnMap.get(c.getName()));
                }
            });
        }
        return sortedColumns;
    }

    protected Map<String, Map<String, List<Pair<Integer, List<Object>>>>> getShardResults(ForeignKeyData data,
                                                                                          String schemaName,
                                                                                          String tableName,
                                                                                          TableMeta tableMeta,
                                                                                          TableMeta parentTableMeta,
                                                                                          List<List<Object>> values,
                                                                                          PhysicalPlanBuilder builder,
                                                                                          List<String> selectKeys,
                                                                                          boolean isFront,
                                                                                          boolean isInsert) {
        List<String> columns = isInsert ? selectKeys : isFront ? data.refColumns : data.columns;

        List<String> sortedColumns;
        if (!isInsert) {
            sortedColumns =
                isFront ? getSortedColumns(true, tableMeta, data) : getSortedColumns(false, parentTableMeta, data);
        } else {
            sortedColumns = new ArrayList<>(columns);
        }

        final List<Set<String>> tarCols = new ArrayList<>();
        final Set<String> tarColSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tarColSet.addAll(columns);
        tarCols.add(tarColSet);

        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        final boolean singleOrBroadcast =
            Optional.ofNullable(oc.getRuleManager()).map(rule -> !rule.isShard(tableName)).orElse(true);

        TableMeta currentTableMeta = isFront ? parentTableMeta : tableMeta;

        boolean fullTableScan =
            singleOrBroadcast || !GlobalIndexMeta.isEveryUkContainsTablePartitionKey(currentTableMeta, tarCols);

        return fullTableScan ?
            builder.getShardResultsFullTableScan(currentTableMeta, values.size()) :
            builder.getShardResults(currentTableMeta, values, sortedColumns.stream()
                    .map(String::toUpperCase)
                    .collect(Collectors.toList()),
                false);
    }

    protected RelDataType getRowTypeForColumns(TableModify modify, List<ColumnMeta> columns) {
        final List<RelDataType> fieldTypes = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        columns.forEach(cm -> {
            fieldTypes.add(cm.getField().getRelType());
            fieldNames.add(cm.getName());
        });
        return modify.getCluster().getTypeFactory().createStructType(fieldTypes, fieldNames);
    }

    protected void executeFkPlan(ExecutionContext executionContext, List<ColumnMeta> returnColumns,
                                 Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans,
                                 String schemaName, String tableName, String constraintName,
                                 List<List<Object>> selectValues) {

        RowSet rowSet = new RowSet(selectValues, returnColumns);
        RelNode plan = fkPlans.get(schemaName).get(tableName).get(constraintName).right;

        executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);
        if (plan instanceof LogicalModify) {
            LogicalModify modify = (LogicalModify) plan;
            // Handle primary
            modify.getPrimaryModifyWriters().forEach(w -> execute(w, rowSet, executionContext));
            // Handle index
            modify.getGsiModifyWriters().forEach(w -> execute(w, rowSet, executionContext));
        } else {
            LogicalRelocate relocate = (LogicalRelocate) plan;

            for (Integer tableIndex : relocate.getSetColumnMetas().keySet()) {
                for (RelocateWriter w : relocate.getRelocateWriterMap().get(tableIndex)) {
                    execute(w, rowSet, executionContext);
                }

                for (DistinctWriter w : relocate.getModifyWriterMap().get(tableIndex)) {
                    execute(w, rowSet, executionContext);
                }
            }
        }
    }

    protected void beforeUpdateFkCheck(TableModify tableModify, String schemaName, String targetTable,
                                       ExecutionContext executionContext, List<List<Object>> values) {
        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTable);

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final List<Pair<String, ForeignKeyData>> foreignKeysWithIndex =
            tableMeta.getForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());

        for (Pair<String, ForeignKeyData> data : foreignKeysWithIndex) {
            if (data.getValue().onUpdate == null) {
                continue;
            }

            schemaName = data.getValue().refSchema;
            String tableName = data.getValue().refTableName;

            PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);

            // parent table
            final TableMeta parentTableMeta = executionContext.getSchemaManager(schemaName).getTableWithNull(tableName);
            if (parentTableMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_UPDATE_FK_CONSTRAINT, schemaName, tableName,
                    data.getValue().toString());
            }

            List<List<Object>> updateValueList =
                ForeignKeyUtils.getUpdateValueList(data.getValue(), tableModify.getUpdateColumnList(), values,
                    tableMeta, true);

            if (updateValueList.get(0).isEmpty()) {
                return;
            }

            Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
                getShardResults(data.getValue(), schemaName, tableName, tableMeta, parentTableMeta, updateValueList,
                    builder, null,
                    true, false);

            List<String> sortedColumns = getSortedColumns(true, tableMeta, data.getValue());

            ExecutionContext selectEc = executionContext.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            List<List<Object>> selectValues = getSelectValues(selectEc, schemaName,
                parentTableMeta, updateValueList, tableModify, memoryAllocator, builder, shardResults,
                sortedColumns, false);

            if (selectValues.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_UPDATE_FK_CONSTRAINT, schemaName, tableName,
                    data.getValue().toString());
            }
        }
    }

    protected void beforeUpdateFkCascade(TableModify tableModify, String schemaName, String targetTable,
                                         ExecutionContext executionContext, List<List<Object>> values,
                                         List<List<Object>> updateValueList,
                                         Map<String, String> columnMap,
                                         Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans,
                                         int depth) {
        if (depth > MAX_FK_DEPTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_FK_EXCEED_MAX_DEPTH);
        }

        if (tableModify instanceof LogicalInsert && ((LogicalInsert) tableModify).isUpsert()) {
            DistinctWriter writer;
            if (!((LogicalUpsert) tableModify).isModifyPartitionKey()) {
                writer = ((LogicalUpsert) tableModify).getPrimaryUpsertWriter().getUpdaterWriter();
            } else {
                writer = ((LogicalUpsert) tableModify).getPrimaryRelocateWriter().getModifyWriter();
            }
            if (writer instanceof SingleModifyWriter) {
                tableModify = ((SingleModifyWriter) writer).getModify();
            } else if (writer instanceof BroadcastModifyWriter) {
                tableModify = ((BroadcastModifyWriter) writer).getModify();
            } else {
                tableModify = ((ShardingModifyWriter) writer).getModify();
            }
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTable);

        List<Pair<String, ForeignKeyData>> referencedForeignKeysWithIndex = new ArrayList<>();
        if (depth == 1) {
            referencedForeignKeysWithIndex =
                tableMeta.getReferencedForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());
        } else {
            for (Map.Entry<String, ForeignKeyData> e : tableMeta.getReferencedForeignKeys().entrySet()) {
                if (new HashSet<>(e.getValue().refColumns).containsAll(columnMap.values())) {
                    referencedForeignKeysWithIndex.add(new Pair<>(e.getKey(), e.getValue()));
                }
            }
        }

        if (referencedForeignKeysWithIndex.isEmpty() || values.isEmpty()) {
            return;
        }

        for (Pair<String, ForeignKeyData> data : referencedForeignKeysWithIndex) {
            if (data.getValue().onUpdate == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Cannot delete or update a parent row: a foreign key constraint fails");
            }

            if (depth == 1) {
                updateValueList =
                    ForeignKeyUtils.getUpdateValueList(data.getValue(), tableModify.getUpdateColumnList(), values,
                        tableMeta, false);

                if (!ForeignKeyUtils.containsUpdateFkColumns(tableModify, data.getValue())) {
                    return;
                }
            }

            PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);

            schemaName = data.getValue().schema;
            String tableName = data.getValue().tableName;
            String constraintName = data.getValue().constraint;

            if (!TableValidator.checkIfTableExists(schemaName, tableName)) {
                continue;
            }
            final TableMeta refTableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

            // get shardResults condition values
            List<List<Object>> shardConditionValueList = getConditionValueList(data.getValue(), tableMeta, values, null,
                null, false);

            Map<String, Map<String, List<Pair<Integer, List<Object>>>>> selectShardResults =
                getShardResults(data.getValue(), schemaName, tableName, refTableMeta, tableMeta,
                    shardConditionValueList, builder,
                    null,
                    false, false);

            columnMap = IntStream.range(0, data.getValue().columns.size()).collect(TreeMaps::caseInsensitiveMap,
                (m, i) -> m.put(data.getValue().refColumns.get(i), data.getValue().columns.get(i)),
                Map::putAll);

            final Map<String, String> columnMapFinal = columnMap;
            List<String> sortedColumns = new ArrayList<>();
            tableMeta.getAllColumns().forEach(c -> {
                if (data.getValue().refColumns.stream().anyMatch(c.getName()::equalsIgnoreCase)) {
                    sortedColumns.add(columnMapFinal.get(c.getName()));
                }
            });

            ExecutionContext selectEc = executionContext.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            List<List<Object>> selectValues = getSelectValues(selectEc, schemaName,
                refTableMeta, shardConditionValueList, tableModify, memoryAllocator, builder, selectShardResults,
                sortedColumns, false);

            if (selectValues.isEmpty()) {
                continue;
            }

            List<ColumnMeta> returnColumns = new ArrayList<>(refTableMeta.getAllColumns());

            switch (data.getValue().onUpdate) {
            case RESTRICT:
            case NO_ACTION:
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Cannot delete or update a parent row: a foreign key constraint fails");
            case CASCADE:
                // Cannot use self-referential ON UPDATE CASCADE or ON UPDATE SET NULL operations
                // https://dev.mysql.com/doc/mysql-reslimits-excerpt/8.0/en/ansi-diff-foreign-keys.html
                if (tableMeta.equals(refTableMeta)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Cannot delete or update a parent row: a foreign key constraint fails");
                }

                for (List<Object> selectValue : selectValues) {
                    selectValue.addAll(updateValueList.get(0));
                }

                for (ColumnMeta column : refTableMeta.getAllColumns()) {
                    if (data.getValue().columns.stream().anyMatch(c -> c.equalsIgnoreCase(column.getName()))) {
                        returnColumns.add(column);
                    }
                }

                if (!data.getValue().isPushDown()) {
                    executeFkPlan(executionContext, returnColumns, fkPlans, schemaName, tableName, constraintName,
                        selectValues);
                }

                break;

            case SET_NULL:
                for (List<Object> selectValue : selectValues) {
                    for (int i = 0; i < data.getValue().columns.size(); i++) {
                        selectValue.add(null);
                    }
                }

                for (ColumnMeta column : refTableMeta.getAllColumns()) {
                    if (data.getValue().columns.stream().anyMatch(c -> c.equalsIgnoreCase(column.getName()))) {
                        returnColumns.add(column);
                    }
                }

                if (!data.getValue().isPushDown()) {
                    executeFkPlan(executionContext, returnColumns, fkPlans, schemaName, tableName, constraintName,
                        selectValues);
                }

                break;

            case SET_DEFAULT:
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "Update Option SET_DEFAULT");
            }

            memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            beforeUpdateFkCascade(tableModify, schemaName, tableName, executionContext, selectValues,
                updateValueList, columnMap, fkPlans, depth + 1);
        }
    }

    protected void beforeDeleteFkCascade(TableModify logicalModify, String schemaName, String targetTable,
                                         ExecutionContext executionContext, List<List<Object>> values,
                                         Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> fkPlans,
                                         int depth) {
        if (depth > MAX_FK_DEPTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_FK_EXCEED_MAX_DEPTH);
        }

        final MemoryPool selectValuesPool = MemoryPoolUtils.createOperatorTmpTablePool(executionContext);
        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        final TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(targetTable);

        final List<Pair<String, ForeignKeyData>> foreignKeysWithIndex =
            tableMeta.getReferencedForeignKeys().entrySet().stream().map(Pair::of).collect(Collectors.toList());

        if (foreignKeysWithIndex.isEmpty() || values.isEmpty()) {
            return;
        }

        for (Pair<String, ForeignKeyData> data : foreignKeysWithIndex) {
            if (data.getValue().onDelete == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Cannot delete or update a parent row: a foreign key constraint fails");
            }

            schemaName = data.getValue().schema;
            String tableName = data.getValue().tableName;
            String constraintName = data.getValue().constraint;

            if (!TableValidator.checkIfTableExists(schemaName, tableName)) {
                continue;
            }

            PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);

            // child table
            final TableMeta refTableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

            // get select condition values
            List<List<Object>> conditionValueList = getConditionValueList(data.getValue(), tableMeta, values, null,
                null, false);

            conditionValueList = conditionValueList.stream().distinct().collect(Collectors.toList());

            boolean isBroadcast = OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(tableName);

            Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = isBroadcast ?
                BuildPlanUtils.buildResultForBroadcastTable(schemaName, tableName, conditionValueList, null,
                    executionContext, false) :
                getShardResults(data.getValue(), schemaName, tableName, refTableMeta, tableMeta, conditionValueList,
                    builder, null,
                    false, false);

            List<String> sortedColumns = getSortedColumns(false, tableMeta, data.getValue());

            ExecutionContext selectEc = executionContext.copy();
            selectEc.setParams(new Parameters(selectEc.getParams().getCurrentParameter(), false));
            List<List<Object>> selectValues = getSelectValues(selectEc, schemaName,
                refTableMeta, conditionValueList, logicalModify, memoryAllocator, builder, shardResults,
                sortedColumns, false);

            if (selectValues.isEmpty()) {
                continue;
            }

            if (tableMeta.equals(refTableMeta)) {
                selectValues.removeAll(values);
            }

            List<ColumnMeta> returnColumns = new ArrayList<>(refTableMeta.getAllColumns());

            switch (data.getValue().onDelete) {
            case RESTRICT:
            case NO_ACTION:
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Cannot delete or update a parent row: a foreign key constraint fails");
            case CASCADE:
                if (!data.getValue().isPushDown()) {
                    executeFkPlan(executionContext, returnColumns, fkPlans, schemaName, tableName, constraintName,
                        selectValues);
                }
                break;

            case SET_NULL:
                for (List<Object> selectValue : selectValues) {
                    for (int i = 0; i < data.getValue().columns.size(); i++) {
                        selectValue.add(null);
                    }
                }

                for (ColumnMeta column : refTableMeta.getAllColumns()) {
                    if (data.getValue().columns.stream().anyMatch(c -> c.equalsIgnoreCase(column.getName()))) {
                        returnColumns.add(column);
                    }
                }

                if (!data.getValue().isPushDown()) {
                    executeFkPlan(executionContext, returnColumns, fkPlans, schemaName, tableName, constraintName,
                        selectValues);
                }

                // turn to update cascade
                List<List<Object>> updateValueList = new ArrayList<>();
                for (List<Object> row : values) {
                    List<Object> updateValue = new ArrayList<>();
                    for (int i = 0; i < data.getValue().refColumns.size(); i++) {
                        updateValue.add(null);
                    }
                    updateValueList.add(updateValue);
                }

                Map<String, String> columnMap =
                    IntStream.range(0, data.getValue().columns.size()).collect(TreeMaps::caseInsensitiveMap,
                        (m, i) -> m.put(data.getValue().refColumns.get(i), data.getValue().columns.get(i)),
                        Map::putAll);

                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
                beforeUpdateFkCascade(logicalModify, schemaName, tableName, executionContext, selectValues,
                    updateValueList, columnMap, fkPlans, depth + 1);

                break;

            case SET_DEFAULT:
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "Delete Option SET_DEFAULT");
            }

            memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            beforeDeleteFkCascade(logicalModify, schemaName, tableName, executionContext, selectValues, fkPlans,
                depth + 1);
        }
    }
}
