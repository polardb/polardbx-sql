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
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.FirstThenOtherCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupConcurrentUnionCursor;
import com.alibaba.polardbx.executor.cursor.impl.GroupSequentialCursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.GenericPhyObjectRecorder;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.operator.Executor;
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
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.RelocateWriter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by chuanqin on 17/7/7.
 */
public abstract class HandlerCommon implements PlanHandler {

    protected final static IdGenerator JOB_ID_GENERATOR = IdGenerator.getIdGenerator();

    protected IRepository repo;

    public HandlerCommon() {
    }

    public HandlerCommon(IRepository repo) {
        this.repo = repo;
    }

    public IRepository getRepo() {
        return repo;
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
        final RelOptTable targetTable = writer.getTargetTable();

        List<RelNode> inputs = writer.getInput(executionContext, rowSet::distinctRowSetWithoutNull);
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

                final List<Object> skSources = Mappings.permute(row, rw.getIdentifierKeySourceMapping());
                final List<Object> skTargets = Mappings.permute(row, rw.getIdentifierKeyTargetMapping());

                if (usePartFieldChecker) {
                    final List<ColumnMeta> sourceColMetas =
                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeySourceMapping());
                    final List<ColumnMeta> targetColMetas =
                        Mappings.permute(rowSet.getMetas(), rw.getIdentifierKeyTargetMapping());

                    try {
                        final NewGroupKey skSourceKey = new NewGroupKey(skSources,
                            sourceColMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);
                        final NewGroupKey skTargetKey = new NewGroupKey(skTargets,
                            targetColMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList()),
                            rw.getIdentifierKeyMetas(), true, executionContext);

                        return skSourceKey.equals(skTargetKey);
                    } catch (Throwable e) {
                        // Maybe value can not be cast, just use DELETE + INSERT to be safe
                        LoggerFactory.getLogger(HandlerCommon.class).warn("new sk checker failed, cause by " + e);
                    }
                    return false;
                } else {
                    final GroupKey skTargetKey = new GroupKey(skTargets.toArray(), rw.getIdentifierKeyMetas());
                    final GroupKey skSourceKey = new GroupKey(skSources.toArray(), rw.getIdentifierKeyMetas());

                    return skTargetKey.equalsForUpdate(skSourceKey);
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

        ExecUtils.getAffectRowsByCursors(inputCursors, false);

        return distinctRows.selectedRows;
    }

    protected List<Cursor> execute(List<RelNode> inputs, ExecutionContext ec, String schemaName) {
        final QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(ec);
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

        // Increase physical sql id
        executionContext.setPhySqlId(executionContext.getPhySqlId() + 1);

        return ExecUtils.getAffectRowsByCursors(inputCursors, isBroadcast);
    }

    protected boolean canUseFirstThenConcurrent(List<RelNode> physicalPlans) {
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
            for (Object v : rawValues) {
                outValues.add(DataTypeUtil.toJavaObject(v));
            }
            values.add(outValues);
        }

        return values;
    }

    /**
     * Query values by cursor
     *
     * @param cursor select cursor
     * @param batchSize batchSize
     * @param allocator memory allocator
     * @param memoryOfOneRow estimated row size
     * @return values
     */
    public List<List<Object>> selectValues(
        Cursor cursor, long batchSize, MemoryAllocatorCtx allocator, long memoryOfOneRow) {
        List<List<Object>> values = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Row rs = cursor.next();
            if (rs == null) {
                break;
            }

            values.add(rs.getValues());
            long rowSize = rs.estimateSize();

            try {
                if (rowSize > 0) {
                    allocator.allocateReservedMemory(rowSize);
                } else {
                    allocator.allocateReservedMemory(memoryOfOneRow);
                }
            } catch (MemoryNotEnoughException t) {
                //return some values earlier, and the values will be do the insert operation
                break;
            }
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
                                                        MemoryAllocatorCtx allocator, long memoryOfOneRow) {
        List<List<Object>> values = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            Row rs = cursor.next();
            if (rs == null) {
                break;
            }

            final List<Object> columnValues = rs.getValues();
            // Convert inner type to JDBC types
            final List<Object> convertedColumnValues =
                columnValues.stream().map(DataTypeUtil::toJavaObject).collect(Collectors.toList());
            values.add(convertedColumnValues);

            try {
                long rowSize = rs.estimateSize();
                if (rowSize > 0) {
                    allocator.allocateReservedMemory(rowSize);
                } else {
                    allocator.allocateReservedMemory(memoryOfOneRow);
                }
            } catch (MemoryNotEnoughException t) {
                //return some values earlier, and the values will be do the update/delete operation
                break;
            }
        }

        return values;
    }

    /**
     * 返回 memorySize
     */
    public static long selectValuesFromCursor(
        Cursor cursor, long batchSize, List<List<Object>> values, long memoryOfOneRow) {
        long memorySize = 0;
        for (int i = 0; i < batchSize; i++) {
            Row rs = cursor.next();
            if (rs == null) {
                break;
            }

            values.add(rs.getValues());
            long rowSize = rs.estimateSize();

            if (rowSize > 0) {
                memorySize += rowSize;
            } else {
                memorySize += memoryOfOneRow;
            }

        }
        return memorySize;
    }

    /**
     * 返回 memorySize
     */
    public static long selectModifyValuesFromCursor(
        Cursor cursor, long batchSize, List<List<Object>> values, long memoryOfOneRow) {
        long memorySize = 0;
        for (int i = 0; i < batchSize; i++) {
            Row rs = cursor.next();
            if (rs == null) {
                break;
            }
            final List<Object> columnValues = rs.getValues();
            // Convert inner type to JDBC types
            final List<Object> convertedColumnValues =
                columnValues.stream().map(DataTypeUtil::toJavaObject).collect(Collectors.toList());
            values.add(convertedColumnValues);
            long rowSize = rs.estimateSize();

            if (rowSize > 0) {
                memorySize += rowSize;
            } else {
                memorySize += memoryOfOneRow;
            }

        }
        return memorySize;
    }

    /**
     * Query values by cursor
     *
     * @param executor select executor
     * @param batchSize how many rows to receive
     * @param isUpdate is UPDATE or DELETE
     * @return received rows
     */
    protected static List<Chunk> selectForModify(Executor executor, long batchSize, boolean isUpdate) {
        executor.open();

        final List<Chunk> result = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            final Chunk chunk = executor.nextChunk();
            if (null == chunk) {
                break;
            }
            result.add(chunk);
        }

        if (result.size() == batchSize) {
            if (executor.nextChunk() != null) {
                String type = isUpdate ? "update" : "delete";
                throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_SELECT_LIMIT_EXCEEDED,
                    type,
                    String.valueOf(batchSize));
            }
        }
        return result;
    }

    protected static ExecutionContext clearSqlMode(ExecutionContext executionContext) {
        // fix the data truncate issue
        final Map<String, Object> serverVariables = executionContext.getServerVariables();
        executionContext.setServerVariables(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        if (null != serverVariables) {
            executionContext.getServerVariables().putAll(serverVariables);
        }
        executionContext.getServerVariables().put("sql_mode", "");
        return executionContext;
    }

    protected static void upgradeEncoding(ExecutionContext executionContext, String schemaName, String baseTableName) {

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
                List<List<Object>> values = new ArrayList<>();

                long batchRowsSize = selectValuesFromCursor(selectCursor, batchSize, values, memoryOfOneRow);

                if (values.isEmpty()) {
                    parallelExecutor.finished();
                    break;
                }
                if (parallelExecutor.getThrowable() != null) {
                    throw GeneralUtil.nestedException(parallelExecutor.getThrowable());
                }
                parallelExecutor.getMemoryControl().allocate(batchRowsSize);
                //将内存大小附带到List最后面，方便传送内存大小
                values.add(Collections.singletonList(batchRowsSize));
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
}
