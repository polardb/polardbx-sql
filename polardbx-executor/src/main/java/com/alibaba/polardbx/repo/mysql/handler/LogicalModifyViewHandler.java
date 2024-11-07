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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

/**
 * @author lingce.ldm 2018-01-31 18:39
 */
public class LogicalModifyViewHandler extends HandlerCommon {

    public LogicalModifyViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalModifyView logicalModifyView = (LogicalModifyView) logicalPlan;
        TableModify logicalTableModify = logicalModifyView.getTableModify();
        if (logicalTableModify instanceof LogicalModify) {
            checkUpdateDeleteLimitLimitation(((LogicalModify) logicalTableModify).getOriginalSqlNode(),
                executionContext);
        }

        String schemaName = logicalModifyView.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        List<RexDynamicParam> scalarList = logicalModifyView.getScalarList();
        SubqueryUtils.buildScalarSubqueryValue(scalarList, executionContext);// handle scalar subquery

        boolean isBroadcast = true;
        final List<RelOptTable> tables =
            (logicalModifyView.getTableModify() != null) ? logicalModifyView.getTableModify().getTargetTables() : null;
        if (null != tables && tables.size() > 0) {
            if (logicalModifyView.getTableModify().isDelete()) {
                for (RelOptTable table : tables) {
                    if (!or.isBroadCast(Util.last(table.getQualifiedName()))) {
                        isBroadcast = false;
                        break;
                    }
                }
            } else if (logicalModifyView.getTableModify().isUpdate()) {
                for (String table : logicalModifyView.getTableModify().getTargetTableNames()) {
                    if (!or.isBroadCast(table)) {
                        isBroadcast = false;
                        break;
                    }
                }
            }
        } else {
            isBroadcast = or.isBroadCast(logicalModifyView.getLogicalTableName());
        }

        // For functions that deterministic or cannot be pushed down, calculate
        // them. TODO: not only gsi, all UPDATE / DELETE should be checked
        ReplaceCallWithLiteralVisitor visitor = null;
        if (!logicalModifyView.hasTargetTableHint() && executionContext.getParams() != null
            && (needConsistency(logicalModifyView, executionContext)
            || ComplexTaskPlanUtils.canWrite(executionContext.getSchemaManager(logicalModifyView.getSchemaName())
            .getTable(logicalModifyView.getLogicalTableName())))) {
            Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
            // TODO: replace sharding keys with literal
            // TODO: broadcast tables also need consistency
            visitor = new ReplaceCallWithLiteralVisitor(Lists.newArrayList(),
                params,
                RexUtils.getEvalFunc(executionContext),
                true);
        }
        // Dynamic functions will be calculated in buildSqlTemplate()
        SqlNode sqlTemplate = logicalModifyView.getSqlTemplate(visitor, executionContext);
        List<RelNode> inputs = logicalModifyView.getInput(sqlTemplate,
            logicalModifyView.optimizeModifyTopNByReturning(),
            executionContext);

        if (!executionContext.isAutoCommit() && inputs.size() > 1) {
            executionContext.setNeedAutoSavepoint(true);
        }

        if (!logicalModifyView.hasTargetTableHint()
            && executionContext.isModifyGsiTable()
            && CheckModifyLimitation.checkModifyGsi(logicalTableModify, executionContext)) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "Should not use LogicalModifyView for table with gsi");
        } else if (logicalModifyView.optimizeModifyTopNByReturning()) {
            final ExecutionContext modifyEc = executionContext.copy();
            return executeModifyTopN(executionContext, inputs, logicalModifyView, modifyEc);
        } else {
            return executePhysicalPlan(inputs, executionContext, isBroadcast, schemaName);
        }
    }

    protected AffectRowCursor executeModifyTopN(ExecutionContext executionContext,
                                                List<RelNode> inputs,
                                                LogicalModifyView lmv,
                                                ExecutionContext modifyEc) {
        final String logicalSchemaName = lmv.getSchemaName();
        final String logicalTableName = lmv.getLogicalTableName();
        final RexDynamicParam fetch = lmv.getModifyTopNInfo().getFetch();
        final int fetchIndex = fetch.getIndex();
        final boolean isDesc = lmv.getModifyTopNInfo().isDesc();

        // Cache current returning switch states
        final String currentReturning = modifyEc.getReturning();

        // Build returning columns and enable returning
        modifyEc.setReturning(String.join(",", lmv.getModifyTopNInfo().getPkColumnNames()));

        int affectedRows = 0;
        boolean partialFinished = false;
        try {
            // Sort physical schema and tables by partition order
            final List<Pair<String, String>> phySchemaAndPhyTables =
                inputs.stream().map(input -> (PhyTableOperation) input)
                    .map(pto -> Pair.of(pto.getDbIndex(), pto.getTableNames().get(0).get(0))).collect(
                        Collectors.toList());
            final List<Integer> partitions =
                PartitionUtils.sortByPartitionOrder(logicalSchemaName, logicalTableName, phySchemaAndPhyTables, isDesc);

            // Compute fetch size
            final long fetchSize = CBOUtil.getRexParam(fetch, modifyEc.getParams().getCurrentParameter());

            int inputIndex = 0;
            while (inputIndex < inputs.size() && affectedRows < fetchSize) {
                final PhyTableOperation currentRel = (PhyTableOperation) inputs.get(partitions.get(inputIndex++));

                // compute new fetchSize
                final long currentFetchSize = fetchSize - affectedRows;

                // modify fetchSize in LogicalModifyView
                final int fetchParamIndex = PhyTableOperationUtil.getPhysicalParamIndex(currentRel, fetchIndex);
                currentRel.getParam().compute(fetchParamIndex, (k, v) -> {
                    assert v != null;
                    return PlannerUtils.changeParameterContextValue(v, currentFetchSize);
                });

                final List<Cursor> currentCursor = new ArrayList<>();
                executeWithConcurrentPolicy(modifyEc,
                    ImmutableList.of(currentRel),
                    QueryConcurrencyPolicy.SEQUENTIAL,
                    currentCursor,
                    logicalSchemaName);

                partialFinished |= currentRel.isSuccessExecuted();

                final MyPhyTableModifyCursor modifyCursor = (MyPhyTableModifyCursor) currentCursor.get(0);
                try {
                    while (modifyCursor.next() != null) {
                        affectedRows++;
                    }
                } catch (Exception e) {
                    throw new TddlNestableRuntimeException(e);
                } finally {
                    modifyCursor.close(new ArrayList<>());
                }
            }

        } catch (Throwable t) {
            if (!executionContext.isAutoCommit() && partialFinished) {
                // In trx, if some plan is executed successfully,
                // we should forbid the trx continuing,
                // or rollback the statement by auto-savepoint.
                executionContext.getTransaction()
                    .setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL, t.getMessage());
            }
            throw new TddlNestableRuntimeException(t);

        } finally {
            modifyEc.setReturning(currentReturning);
        }

        return new AffectRowCursor(affectedRows);
    }

    /**
     * In broadcast tables and gsi tables, data must be consistent. That is,
     * some functions should be calculated in advance.
     */
    private boolean needConsistency(LogicalModifyView logicalModifyView, ExecutionContext executionContext) {
        // TODO: broadcast tables
        String schemaName = logicalModifyView.getSchemaName();
        String tableName = logicalModifyView.getLogicalTableName();
        TableMeta tableMeta =
            executionContext.getSchemaManager(schemaName).getTable(tableName);
        return GlobalIndexMeta.hasGsi(tableName, schemaName, executionContext) ||
            ComplexTaskPlanUtils.canWrite(tableMeta);
    }

    private Cursor executePhysicalPlan(List<RelNode> inputs, ExecutionContext executionContext,
                                       boolean isBroadcast, String schemaName) {
        QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext);
        List<Cursor> inputCursors = new ArrayList<>(inputs.size());
        int affectRows;
        try {
            executeWithConcurrentPolicy(executionContext, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

            affectRows = ExecUtils.getAffectRowsByCursors(inputCursors, isBroadcast);
        } catch (Throwable t) {
            if (!executionContext.isAutoCommit()) {
                // In trx, if some plan is executed successfully,
                // we should forbid the trx continuing,
                // or rollback the statement by auto-savepoint.
                for (RelNode input : inputs) {
                    if (input instanceof BaseTableOperation && ((BaseTableOperation) input).isSuccessExecuted()) {
                        executionContext.getTransaction()
                            .setCrucialError(ErrorCode.ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL, t.getMessage());
                        break;
                    }
                }
            }
            throw t;
        }

        return new AffectRowCursor(affectRows);
    }
}
