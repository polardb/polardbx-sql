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

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.gsi.UpdateDeleteIndexExecutor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
        if (!logicalModifyView.hasHint() && executionContext.getParams() != null
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
        List<RelNode> inputs = logicalModifyView.getInput(sqlTemplate, executionContext);

        if (!executionContext.isAutoCommit() && inputs.size() > 1) {
            executionContext.setNeedAutoSavepoint(true);
        }

        if (!logicalModifyView.hasHint() && executionContext.getParams() != null
            && GlobalIndexMeta.hasIndex(logicalModifyView.getLogicalTableName(), schemaName, executionContext)) {
            // TODO add this back
            executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 1);

            // If target column does not occur in any GSI index columns,
            // the index updating is not needed.
            if (sqlTemplate.getKind() == SqlKind.UPDATE) {
                if (!needUpdateGSI(logicalModifyView, (SqlUpdate) sqlTemplate, executionContext)) {
                    return executePhysicalPlan(inputs, executionContext, isBroadcast, schemaName);
                }
            }
            return executeIndex(logicalModifyView, inputs, sqlTemplate, executionContext, schemaName);
        } else {
            return executePhysicalPlan(inputs, executionContext, isBroadcast, schemaName);
        }
    }

    /**
     * If UPDATE target column does not occur in any GSI index columns, the
     * index updating is not needed.
     */
    private boolean needUpdateGSI(LogicalModifyView logicalModifyView, SqlUpdate sqlUpdate,
                                  ExecutionContext executionContext) {
        Map<String, List<TableMeta>> tableNameAndIndexMetas = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> allTables = logicalModifyView.getTableModify().getTargetTableNames();

        for (int i = 0; i < sqlUpdate.getTargetColumnList().size(); i++) {
            final SqlNode column = sqlUpdate.getTargetColumnList().get(i);
            final String columName = ((SqlIdentifier) column).getLastName();
            final String tableName = allTables.get(i);
            List<TableMeta> indexMetas = tableNameAndIndexMetas.get(tableName);
            if (indexMetas == null) {
                indexMetas = GlobalIndexMeta.getIndex(tableName, logicalModifyView.getSchemaName(), executionContext);
                tableNameAndIndexMetas.put(tableName, indexMetas);
            }
            for (TableMeta indexMeta : indexMetas) {
                if (indexMeta.getColumnIgnoreCase(columName) != null) {
                    return true;
                }
            }
        }
        return false;
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
        return GlobalIndexMeta.hasIndex(tableName, schemaName, executionContext) ||
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

        return new AffectRowCursor(new int[] {affectRows});
    }

    /**
     * Execute with global indexes.
     *
     * @param physicalPlan physical plans of logicalModifyView
     * @param sqlNode sqlNode of logicalModifyView
     */
    private Cursor executeIndex(LogicalModifyView logicalModifyView, List<RelNode> physicalPlan,
                                SqlNode sqlNode, ExecutionContext executionContext, String schemaName) {
        UpdateDeleteIndexExecutor executor = new UpdateDeleteIndexExecutor((List<RelNode> inputs,
                                                                            ExecutionContext executionContext1) -> {
            QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext1);
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return inputCursors;
        }, schemaName);

        try {
            int affectRows = executor.execute(logicalModifyView.getLogicalTableName(),
                sqlNode,
                physicalPlan,
                executionContext);
            return new AffectRowCursor(new int[] {affectRows});
        } catch (Throwable e) {
            // Can't commit
            executionContext.getTransaction()
                .setCrucialError(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL, e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }
}
