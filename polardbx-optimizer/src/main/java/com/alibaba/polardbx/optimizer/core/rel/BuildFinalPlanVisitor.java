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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Pair;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * 将计划中的 LogicalView / LogicalInsert 节点替换为 SingleTableOperation
 *
 * @author lingce.ldm 2017-11-20 19:44
 */
public class BuildFinalPlanVisitor extends RelShuttleImpl {

    private final PlannerContext pc;
    private SqlNode sqlTemplate;
    private boolean buildPlanForScaleOut = false;

    public BuildFinalPlanVisitor(SqlNode ast, PlannerContext pc) {
        this(ast, false, pc);
    }

    public BuildFinalPlanVisitor(SqlNode ast, boolean buildPlanForScaleOut, PlannerContext pc) {
        this.sqlTemplate = ast;
        this.buildPlanForScaleOut = buildPlanForScaleOut;
        this.pc = pc;
    }

    private void replaceTableNameWithQuestionMark(String defaultSchemaName) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(defaultSchemaName, true, pc.getExecutionContext());
        sqlTemplate = sqlTemplate.accept(visitor);
    }

    @Override
    public final RelNode visit(TableScan scan) {
        Preconditions.checkArgument(scan instanceof LogicalView);
        if (scan instanceof LogicalModifyView) {
            return buildNewPlanForUpdateDelete((LogicalModifyView) scan, pc.getExecutionContext());
        }

        RelNode newNode = null;
        LogicalView lv = (LogicalView) scan;
        OptimizerContext oc = OptimizerContext.getContext(lv.getSchemaName());
        TddlRuleManager or = oc.getRuleManager();
        List<String> tableNames = lv.getTableNames();
        /**
         * 如果仅下发至单库单表,替换为 SingleTableOperation
         */
        if (tableNames.size() == 1 && !lv.hasDynamicPruning()) {
            newNode = buildSingleTableScan(oc, lv, or, false);
        } else if (tableNames.size() > 1) {
            // ignore
        } else {
            // Impossible.
        }

        return newNode == null ? scan : newNode;
    }

    private RelNode buildSingleTableScan(OptimizerContext oc, LogicalView lv, TddlRuleManager or,
                                         boolean removeSchema) {

        if (removeSchema) {
            RemoveSchemaNameVisitor visitor = new RemoveSchemaNameVisitor(lv.getSchemaName());
            this.sqlTemplate = this.sqlTemplate.accept(visitor);
        }

        replaceTableNameWithQuestionMark(oc.getSchemaName());

        String tableName = lv.getLogicalTableName();
        String schemaName = lv.getSchemaName();

        ShardProcessor processor = null;
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            TableRule tableRule = or.getTableRule(tableName);
            if (tableRule == null) {
                return null;
            }
            List<String> shardColumns = tableRule.getShardColumns();
            Map<String, Comparative> comparatives = lv.getComparative();
            SchemaManager schemaManager = pc.getExecutionContext().getSchemaManager(schemaName);
            Map<String, DataType> dataTypeMap =
                PlannerUtils.buildDataType(shardColumns, schemaManager.getTable(tableName));
            processor = ShardProcessor.build(schemaName,
                tableName,
                shardColumns,
                tableRule,
                comparatives,
                dataTypeMap);
        } else {
            if (lv.useSelectPartitions()) {
                /**
                 * when select with partition selection, should not build simple plan
                 */
                return null;
            }

            // table is in new part db
            /**
             * lv may be LogicalView or LogicalModifyView
             */
            if (!lv.getPushDownOpt().couldDynamicPruning() && PartitionPrunerUtils.checkIfPointSelect(
                lv.getRelShardInfo(pc.getExecutionContext()).getPartPruneStepInfo(), pc.getExecutionContext())) {
                processor = ShardProcessor
                    .createPartTblShardProcessor(schemaName, tableName,
                        lv.getRelShardInfo(pc.getExecutionContext()).getPartPruneStepInfo(),
                        null,
                        true);
            } else {
                return null;
            }

        }

        if (processor instanceof EmptyShardProcessor) {
            return null;
        }

        /**
         * Scalar list means having uncorrelate subquery, that has every features
         * like constant value but needing to cal first in LogicalView handler,
         * which action should change the ast. One SingleTableOperation would like
         * to reuse ast,not changing it.
         */
        if (lv.getScalarList().size() > 0) {
            return null;
        }

        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlTemplate);
        SingleTableOperation singleTableOperation = new SingleTableOperation(lv,
            processor,
            tableName,
            RelUtils.toNativeBytesSql(sqlTemplate),
            paramIndex, SingleTableOperation.NO_AUTO_INC);

        singleTableOperation.setLockMode(lv.lockMode);
        singleTableOperation.setSchemaName(schemaName);
        singleTableOperation.setKind(sqlTemplate.getKind());
        singleTableOperation.setNativeSqlNode(sqlTemplate);
        singleTableOperation.setXTemplate(lv.getXPlan());
        singleTableOperation.setOriginPlan(lv.getPushedRelNode());
        singleTableOperation.setHintContext(lv.getHintContext());
        final BytesSql bytesSql = singleTableOperation.getBytesSql();
        singleTableOperation.setGalaxyPrepareDigest(lv.getGalaxyPrepareDigest(pc.getExecutionContext(), bytesSql));
        singleTableOperation.setSupportGalaxyPrepare(
            lv.isSupportGalaxyPrepare() && singleTableOperation.getGalaxyPrepareDigest() != null);
        // Init sql digest.
        try {
            singleTableOperation.setSqlDigest(bytesSql.digest());
        } catch (Exception ignore) {
        }

        return singleTableOperation;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalInsert) {
            LogicalInsert logicalInsert = (LogicalInsert) other;
            if (logicalInsert.isInsert() || logicalInsert.isReplace()) {
                return buildNewPlanForInsert((LogicalInsert) other, pc.getExecutionContext());
            }
        }
        return super.visit(other);
    }

    private RelNode buildNewPlanForInsert(LogicalInsert logicalInsert, ExecutionContext ec) {
        // insert select 或者 insert values(select id from ...) values里面包含子查询，有一些复杂数据来源可能无法转换为sqlNode
        // 主要问题：logicalInsert.getSqlTemplate(),这是提前生成sql语句，但是子查询LogicalCorrelate无法转换成sql，
        if (logicalInsert.isSourceSelect()) {
            // Some part of select node may not be able to convert to SqlNode
            // Like LogicalExpand or SemiJoin
            logicalInsert.initLiteralColumnIndex(this.buildPlanForScaleOut);
            logicalInsert.initAutoIncrementColumn();
            return logicalInsert;
        }

        String tableName = logicalInsert.getLogicalTableName();
        String schemaName = logicalInsert.getSchemaName();
        final TableMeta table = ec.getSchemaManager(schemaName).getTable(tableName);
        if (GlobalIndexMeta.hasIndex(tableName, schemaName, ec)) {
            return buildLogicalModify(logicalInsert);
        }

        // in delete_only status, the insert/insert ignore could push down
        final boolean canPushDownScaleOutPlan =
            !ComplexTaskPlanUtils.canWrite(table) || ComplexTaskPlanUtils.isDeleteOnly(table) && (
                logicalInsert.isSimpleInsert(true) && !logicalInsert.isReplace());
        if (!canPushDownScaleOutPlan) {
            return buildLogicalModify(logicalInsert);
        }

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        // broadcast? single table?
        if (or.isTableInSingleDb(tableName) || or.isBroadCast(tableName)) {
            return buildLogicalModify(logicalInsert);
        }

        // hot key?
        if (or.containExtPartitions(tableName)) {
            return buildLogicalModify(logicalInsert);
        }

        LogicalDynamicValues values = (LogicalDynamicValues) logicalInsert.getInput();
        if (logicalInsert.getBatchSize() > 1 || values.getTuples().size() > 1 || ec.isBatchPrepare()) {
            return buildLogicalModify(logicalInsert);
        }

        TableMeta tableMeta = pc.getExecutionContext().getSchemaManager(schemaName).getTable(tableName);

        // foreign key?
        if (ec.foreignKeyChecks() && !GeneralUtil.isEmpty(tableMeta.getForeignKeys())) {
            return buildLogicalModify(logicalInsert);
        }

        // generated column?
        if (table.getPhysicalColumns().stream().anyMatch(ColumnMeta::isLogicalGeneratedColumn)) {
            return buildLogicalModify(logicalInsert);
        }

        // all values can be pushed down?
        TreeSet<String> specialColumns = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        specialColumns.addAll(or.getSharedColumns(tableName));
        specialColumns.addAll(tableMeta.getAutoIncrementColumns());
        List<String> fieldNames = values.getRowType().getFieldNames();
        List<RexNode> row = values.getTuples().get(0);
        for (int i = 0; i < row.size(); i++) {
            String fieldName = fieldNames.get(i);
            RexNode node = row.get(i);

            // With RexCallParam cannot be pushdown
            if ((node instanceof RexDynamicParam
                && ((RexDynamicParam) node).getDynamicType() != RexDynamicParam.DYNAMIC_TYPE_VALUE.DEFAULT)) {
                return buildLogicalModify(logicalInsert);
            }

            // Sharding keys and autoInc columns can only be literal.
            // If the expression for the auto increment column is 1-1, it should
            // be replaced with new sequence value. Otherwise, it shouldn't.
            if (specialColumns.contains(fieldName)) {
                SqlKind sqlKind = node.getKind();
                if (sqlKind == SqlKind.LITERAL || (node instanceof RexDynamicParam
                    && ((RexDynamicParam) node).getDynamicType() == RexDynamicParam.DYNAMIC_TYPE_VALUE.DEFAULT)) {
                    continue;
                }
                return buildLogicalModify(logicalInsert);
            }

            // other columns need to be pushed down
            if (!canBePushDown(node, this.buildPlanForScaleOut)) {
                return buildLogicalModify(logicalInsert);
            }
        }

        // ON DUPLICATE KEY UPDATE can be pushed down?
        List<RexNode> updateList = logicalInsert.getDuplicateKeyUpdateList();
        if (updateList != null && !updateList.isEmpty()) {
            for (RexNode updateNode : updateList) {
                if (!canBePushDown(updateNode, this.buildPlanForScaleOut)) {
                    return buildLogicalModify(logicalInsert);
                }
            }
        }

        // We only rewrites INSERT / UPSERT here, so we only need to check whether UPSERT will modify partition key.
        // If UPSERT does modify partition key, then we need to use LogicalInsert, which has converted this UPSERT to
        // SELECT + DELETE + INSERT.
        if (logicalInsert instanceof LogicalUpsert) {
            LogicalUpsert logicalUpsert = (LogicalUpsert) logicalInsert;
            if (logicalUpsert.isModifyPartitionKey()) {
                return buildLogicalModify(logicalInsert);
            }
        }

        final boolean primaryKeyCheck = ec.getParamManager().getBoolean(ConnectionParams.PRIMARY_KEY_CHECK);
        if (primaryKeyCheck && !logicalInsert.isPushablePrimaryKeyCheck()) {
            return buildLogicalModify(logicalInsert);
        }

        boolean isColumnMultiWrite = TableColumnUtils.isModifying(schemaName, tableName, ec);
        // Insert source must be value here
        if (!buildPlanForScaleOut && (ComplexTaskPlanUtils.isDeleteOnly(tableMeta) && !logicalInsert.isInsertIgnore()
            || !ComplexTaskPlanUtils.canWrite(tableMeta)) && !isColumnMultiWrite) {
            RelNode singleTableInsert = buildSingleTableInsert(logicalInsert, ec);
            if (singleTableInsert != null) {
                return singleTableInsert;
            }
        }

        return buildLogicalModify(logicalInsert);
    }

    /**
     * Operands of RexCall could be RexLiteral/RexDynamicParam/RexInputRef/...
     */
    public static boolean canBePushDown(RexNode rexNode, boolean withScaleOut) {
        if (rexNode == null) {
            return true;
        }

        // function that can be pushed down
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (!withScaleOut) {
                if (!call.getOperator().canPushDown()) {
                    return false;
                }
                for (RexNode operand : call.getOperands()) {
                    if (!canBePushDown(operand, false)) {
                        return false;
                    }
                }
            } else {
                if (!call.getOperator().canPushDown(true)) {
                    return false;
                }
                for (RexNode operand : call.getOperands()) {
                    if (!canBePushDown(operand, true)) {
                        return false;
                    }
                }
            }
        } else if (rexNode instanceof RexCallParam) {
            return canBePushDown(((RexCallParam) rexNode).getRexCall(), withScaleOut);
        }

        return true;
    }

    private RelNode buildNewPlanForUpdateDelete(LogicalModifyView logicalModifyView, ExecutionContext ec) {
        String tableName = logicalModifyView.getLogicalTableName();
        String schemaName = logicalModifyView.getSchemaName();
        if (GlobalIndexMeta.hasIndex(tableName, schemaName, ec)) {
            return buildLogicalModifyViewGsi(logicalModifyView);
        }

        if (logicalModifyView.getTableNames().size() > 1) {
            return logicalModifyView;
        }

        if (ec.isBatchPrepare()) {
            return logicalModifyView;
        }

        boolean isColumnMultiWrite = TableColumnUtils.isModifying(schemaName, tableName, ec);
        if (logicalModifyView.isSingleGroup() && !buildPlanForScaleOut && !isColumnMultiWrite) {
            OptimizerContext context = OptimizerContext.getContext(schemaName);
            TddlRuleManager or = context.getRuleManager();
            RelNode tableScan = buildSingleTableScan(context, logicalModifyView, or, true);
            if (tableScan != null) {
                return tableScan;
            }
        }

        return logicalModifyView;
    }

    private RelNode buildLogicalModifyViewGsi(LogicalModifyView logicalModifyView) {
        return logicalModifyView;
    }

    private RelNode buildLogicalModify(LogicalInsert logicalInsert) {
        // build once and for all
        logicalInsert.getSqlTemplate();
        logicalInsert.initLiteralColumnIndex(this.buildPlanForScaleOut);
        logicalInsert.initAutoIncrementColumn();
        return logicalInsert;
    }

    private RelNode buildSingleTableInsert(LogicalInsert logicalInsert, ExecutionContext ec) {
        replaceTableNameWithQuestionMark(logicalInsert.getSchemaName());

        // process sequence
        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlTemplate);
        int[] autoIncResult = new int[1];
        logicalInsert = processAutoInc(logicalInsert, paramIndex, autoIncResult, ec);
        int autoIncParamIndex = autoIncResult[0];

        // build comparatives after processing sequence
        String tableName = logicalInsert.getLogicalTableName();
        String schemaName = logicalInsert.getSchemaName();

        ShardProcessor processor = ShardProcessor.buildSimpleShard(logicalInsert);

        // prepare mode
        if (processor instanceof EmptyShardProcessor) {
            return null;
        }

        // If this table's logical column order is different from physical column order and user does not specify
        // columns, then we can not use input's sqlTemplate since it may not contain column names
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        SqlNodeList columnList = ((SqlInsert) sqlTemplate).getTargetColumnList();

        // We must add column names if we are doing column multi-write
        boolean isColumnMultiWrite = TableColumnUtils.isModifying(schemaName, tableName, ec);
        if ((tableMeta.requireLogicalColumnOrder() || isColumnMultiWrite) && (columnList == null || GeneralUtil.isEmpty(
            columnList.getList()))) {
            SqlNodeList sqlNodeList = ((SqlInsert) logicalInsert.getSqlTemplate()).getTargetColumnList();
            ((SqlInsert) sqlTemplate).setOperand(3, sqlNodeList);
        }

        SingleTableOperation singleTableOperation = new SingleTableOperation(logicalInsert,
            processor,
            tableName,
            RelUtils.toNativeBytesSql(sqlTemplate),
            paramIndex,
            autoIncParamIndex);
        singleTableOperation.setSchemaName(schemaName);
        singleTableOperation.setKind(sqlTemplate.getKind());
        singleTableOperation.setNativeSqlNode(sqlTemplate);
        // set galaxy digest
        Planner.setGalaxyPrepareDigest(singleTableOperation, ImmutableList.of(tableName), ec, logicalInsert);

        return singleTableOperation;
    }

    /**
     * 1. Field for sequence is Literal: replace it with a dynamicParam. 2.
     * Field for sequence is DynamicParam: record its param index.
     *
     * @param logicalInsert original logicalInsert
     * @param paramIndexes paramIndexes of logicalInsert
     * @param autoIncResult autoIncParamIndex
     * @return the new logicalInsert
     */
    private LogicalInsert processAutoInc(LogicalInsert logicalInsert, List<Integer> paramIndexes, int[] autoIncResult,
                                         ExecutionContext ec) {
        String tableName = logicalInsert.getLogicalTableName();
        String schemaName = logicalInsert.getSchemaName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        List<String> autoIncColumns = tableMeta.getAutoIncrementColumns();
        LogicalDynamicValues values = (LogicalDynamicValues) logicalInsert.getInput();
        List<RelDataTypeField> fields = values.getRowType().getFieldList();
        RexNode autoIncNode = null;
        int autoIncParamIndex = SingleTableOperation.NO_AUTO_INC;
        int autoIncColumnIndex = -1;
        if (!autoIncColumns.isEmpty()) {
            String autoIncColumnName = autoIncColumns.get(0);
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equalsIgnoreCase(autoIncColumnName)) {
                    autoIncColumnIndex = i;
                    autoIncNode = values.getTuples().get(0).get(i);
                    break;
                }
            }
        }

        // it has been checked that autoIncNode will only be dynamicParam or
        // literal
        if (autoIncNode != null) {
            if (autoIncNode instanceof RexLiteral) {
                // it must be `null`
                if (((RexLiteral) autoIncNode).getTypeName().getFamily() == SqlTypeFamily.NULL) {
                    // replace it with a RexDynamicParam
                    Integer maxIndex = -1;
                    for (Integer index : paramIndexes) {
                        if (index > maxIndex) {
                            maxIndex = index;
                        }
                    }
                    autoIncParamIndex = maxIndex + 1;
                    autoIncNode = new RexDynamicParam(fields.get(autoIncColumnIndex).getType(), autoIncParamIndex);
                    List<RexNode> row = Lists.newArrayList(values.getTuples().get(0));
                    row.set(autoIncColumnIndex, autoIncNode);
                    values = LogicalDynamicValues.createDrdsValues(values.getCluster(),
                        values.getTraitSet(),
                        values.getRowType(),
                        ImmutableList.of(ImmutableList.copyOf(row)));
                    logicalInsert = new LogicalInsert(logicalInsert.getCluster(),
                        logicalInsert.getTraitSet(),
                        logicalInsert.getTable(),
                        logicalInsert.getCatalogReader(),
                        values,
                        logicalInsert.getOperation(),
                        logicalInsert.isFlattened(),
                        logicalInsert.getInsertRowType(),
                        logicalInsert.getKeywords(),
                        logicalInsert.getDuplicateKeyUpdateList(),
                        logicalInsert.getBatchSize(),
                        logicalInsert.getAppendedColumnIndex(),
                        logicalInsert.getHints(),
                        logicalInsert.getTableInfo());
                    // add the sequence field, or change `null` to `?`
                    sqlTemplate = logicalInsert.getSqlTemplate();
                    // fields in sqlTemplate may be reordered
                    paramIndexes.clear();
                    paramIndexes.addAll(PlannerUtils.getDynamicParamIndex(sqlTemplate));
                }
            } else if (autoIncNode instanceof RexDynamicParam) {
                autoIncParamIndex = ((RexDynamicParam) autoIncNode).getIndex();
            }
        }

        autoIncResult[0] = autoIncParamIndex;
        return logicalInsert;
    }
}
