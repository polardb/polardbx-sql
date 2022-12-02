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

package com.alibaba.polardbx.optimizer.sql.sql2rel;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.timezone.TimestampUtils;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.ExecutionStrategy;
import com.alibaba.polardbx.optimizer.exception.SqlValidateException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.DrdsViewExpander;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.ByteString;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.TableInfo;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddFullTextIndex;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddSpatialIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDropIndex;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlDefaultOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_LOCAL_INDEX_PREFIX;
import static com.alibaba.polardbx.common.TddlConstants.INFORMATION_SCHEMA;
import static com.google.common.util.concurrent.Runnables.doNothing;
import static org.apache.calcite.util.Static.RESOURCE;

public class TddlSqlToRelConverter extends SqlToRelConverter {

    protected PlannerContext plannerContext;

    public TddlSqlToRelConverter(DrdsViewExpander viewExpander, SqlValidator validator, CatalogReader catalogReader,
                                 RelOptCluster cluster, SqlRexConvertletTable convertletTable, Config config,
                                 PlannerContext plannerContext) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
        this.plannerContext = plannerContext;
    }

    public TddlSqlToRelConverter(DrdsViewExpander viewExpander, SqlValidator validator, CatalogReader catalogReader,
                                 RelOptCluster cluster, SqlRexConvertletTable convertletTable, Config config,
                                 PlannerContext plannerContext, int inSubQueryThreshold) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config, inSubQueryThreshold);
        this.plannerContext = plannerContext;
    }

    @Override
    protected RelNode convertInsert(SqlInsert call) {
        RelNode relNode = super.convertInsert(call);

        if (relNode instanceof TableModify) {
            TableModify modify = (TableModify) relNode;

            // add keywords to result
            SqlNodeList keywords = (SqlNodeList) call.getOperandList().get(0);
            List<String> keywordNames = SqlDmlKeyword.convertFromSqlNodeToString(keywords);
            modify.setKeywords(keywordNames);

            // on duplicate key update
            SqlNodeList updateList = (SqlNodeList) call.getOperandList().get(4);
            if (updateList.size() > 0) {
                RelOptTable targetTable = modify.getTable();

                ExecutionContext ec = PlannerContext.getPlannerContext(relNode).getExecutionContext();

                final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);
                final String schema = qn.left;
                final String tableName = qn.right;
                final OptimizerContext oc = OptimizerContext.getContext(schema);
                assert oc != null;

                final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(tableName);

                final TableColumnMeta tableColumnMeta = tableMeta.getTableColumnMeta();
                SqlNode sourceNode = null;

                // put all columns to nameToNodeMap
                RelDataType rowType = targetTable.getRowType();

                final RexNode sourceRef = rexBuilder.makeRangeReference(rowType, 0, false);
                final Map<String, RexNode> nameToNodeMap = new HashMap<>();
                final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
                final List<String> targetFields = rowType.getFieldNames();
                for (int i = 0; i < targetFields.size(); i++) {
                    switch (strategies.get(i)) {
                    case STORED:
                    case VIRTUAL:
                        break;
                    default:
                        nameToNodeMap.put(targetFields.get(i), rexBuilder.makeFieldAccess(sourceRef, i));
                    }
                }

                final Set<String> updateColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                Blackboard bb = createBlackboard(null, nameToNodeMap, false);

                //替换默认值需要的变量
                final InitializerExpressionFactory initializerFactory =
                    getInitializerFactory(validator.getNamespace(call).getTable());
                final boolean sqlModeStrict =
                    Optional.ofNullable(oc.getVariableManager()).map(vm -> vm.isSqlModeStrict(ec)).orElse(true);
                final ReplaceDefaultOnDuplicateKeyUpdateList replaceDefaultExpr =
                    new ReplaceDefaultOnDuplicateKeyUpdateList(targetFields,
                        targetTable, bb, initializerFactory, tableMeta, call, sqlModeStrict, ec);

                // convert SqlNode to RexNode
                ImmutableList.Builder<RexNode> rexNodeSourceExpressionListBuilder = ImmutableList.builder();
                for (SqlNode n : updateList) {
                    String updateTargetColumnName = null;
                    if (n instanceof SqlCall) {
                        final SqlNode updateTarget = ((SqlCall) n).getOperandList().get(0);
                        updateTargetColumnName = RelUtils.stringValue(updateTarget);
                        updateColumns.add(updateTargetColumnName);
                    }
                    RexNode rn = bb.convertExpression(n);
                    if (updateTargetColumnName != null) {
                        //替换default值
                        rn = replaceDefaultExpr.replaceDefaultValue(rn, updateTargetColumnName, updateTargetColumnName);
                    }
                    rexNodeSourceExpressionListBuilder.add(rn);
                }

                final boolean isBroadcast = oc.getRuleManager().isBroadCast(tableName);

                final List<TableMeta> gsiMetas = GlobalIndexMeta.getIndex(targetTable, ec);
                final boolean modifyGsi = CheckModifyLimitation.checkModifyGsi(targetTable, updateColumns, ec);
                final boolean allGsiPublished = GlobalIndexMeta.isAllGsiPublished(gsiMetas, this.getPlannerContext());

                final boolean scaleOutCanWrite = ComplexTaskPlanUtils.canWrite(tableMeta);
                final boolean scaleOutReadyToPublish = ComplexTaskPlanUtils.isReadyToPublish(tableMeta);
                final boolean scaleOutConsistentBaseData = !scaleOutCanWrite || scaleOutReadyToPublish;

                final ExecutionStrategy hintEx = getExecutionStrategy();
                final boolean replaceNonDeterministicFunction = hintEx.replaceNonDeterministicFunction();
                final boolean pushdownDuplicateCheck = isPushdownDuplicateCheck();

                final List<List<String>> uniqueKeys =
                    GlobalIndexMeta.getUniqueKeys(tableName, schema, true, tm -> true, ec);
                final boolean withoutPkAndUk = uniqueKeys.isEmpty() || uniqueKeys.get(0).isEmpty();
                final boolean ukContainsPartitionKey =
                    GlobalIndexMeta.isEveryUkContainsAllPartitionKey(tableName, schema, true, ec);

                final boolean canPushDuplicateCheck =
                    withoutPkAndUk || (ukContainsPartitionKey && allGsiPublished && scaleOutConsistentBaseData);

                if (isBroadcast || modifyGsi || scaleOutCanWrite || replaceNonDeterministicFunction
                    || (!pushdownDuplicateCheck && !canPushDuplicateCheck)) {
                    for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                        if (!updateColumns.contains(columnMeta.getName())) {
                            if (TStringUtil.containsIgnoreCase(columnMeta.getField().getExtra(), "on update")) {
                                final Field field = columnMeta.getField();
                                String columnName = columnMeta.getName();

                                // Add SET for column ON UPDATE CURRENT_TIMESTAMP
                                if (DataTypeUtil.anyMatchSemantically(field.getDataType(), DataTypes.TimestampType,
                                    DataTypes.DatetimeType)) {
                                    final SqlBasicCall currentTimestamp =
                                        new SqlBasicCall(SqlStdOperatorTable.CURRENT_TIMESTAMP, SqlNode.EMPTY_ARRAY,
                                            SqlParserPos.ZERO);
                                    final SqlIdentifier targetColumnId =
                                        new SqlIdentifier(columnName, SqlParserPos.ZERO);
                                    final SqlBasicCall onUpdateCurrentTimestamp =
                                        new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                                            ImmutableList.of(targetColumnId, currentTimestamp).toArray(new SqlNode[2]),
                                            SqlParserPos.ZERO);
                                    final RexNode rn = bb.convertExpression(onUpdateCurrentTimestamp);
                                    rexNodeSourceExpressionListBuilder.add(rn);

                                    // Record append column index for generation of correct 'on update current_timestamp' values as MySQL does
                                    final RexCall rexCall = (RexCall) rn;
                                    final RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().get(0);
                                    modify.getAppendedColumnIndex().add(rexInputRef.getIndex());
                                }
                            }
                        }
                    }
                }

                modify.setDuplicateKeyUpdateList(rexNodeSourceExpressionListBuilder.build());
            } else {
                modify.setDuplicateKeyUpdateList(new ArrayList<RexNode>());
            }
        }

        return relNode;
    }

    private class ReplaceDefaultOnDuplicateKeyUpdateList {
        private final List<String> allColumnNames;
        private final RelOptTable targetTable;
        private final Blackboard bb;
        private final InitializerExpressionFactory initializerFactory;
        private final TableMeta tableMeta;
        private final SqlNode sqlNode;
        private final boolean sqlModeStrict;
        private final ExecutionContext ec;

        public ReplaceDefaultOnDuplicateKeyUpdateList(List<String> allColumnNames,
                                                      RelOptTable targetTable, Blackboard bb,
                                                      InitializerExpressionFactory initializerFactory,
                                                      TableMeta tableMeta, SqlNode sqlNode, boolean sqlModeStrict,
                                                      ExecutionContext ec) {
            this.allColumnNames = allColumnNames;
            this.targetTable = targetTable;
            this.bb = bb;
            this.initializerFactory = initializerFactory;
            this.tableMeta = tableMeta;
            this.sqlNode = sqlNode;
            this.sqlModeStrict = sqlModeStrict;
            this.ec = ec;
        }

        /**
         * 替换OnDuplicateKeyUpdateList 中的default表达式
         */
        public RexNode replaceDefaultValue(RexNode relNode, String defaultFieldName, String targetFieldName) {
            //OnDuplicateKey 不是 c1 = RexCall 形式，直接返回
            if (!(relNode instanceof RexCall) || ((RexCall) relNode).getOperator().getKind() != SqlKind.EQUALS ||
                !(((RexCall) relNode).getOperands().get(1) instanceof RexCall)) {
                return relNode;
            }
            RexNode operandOne = ((RexCall) relNode).getOperands().get(0);
            RexCall assignment = (RexCall) ((RexCall) relNode).getOperands().get(1);
            RexNode newAssignment = null;
            boolean modify = false;
            // OnDuplicateKeyUpdateList是c1 = default,
            if (assignment.getOperator().getKind() == SqlKind.DEFAULT && assignment.getOperands().isEmpty()) {
                newAssignment =
                    getFieldDefaultValueOnDuplicateKeyUpdateList(assignment, defaultFieldName, targetFieldName);
                modify = true;
            } else {
                //否则，寻找 c1 = default(c2), c1 = 1 + default(c2) 等表达式替换default值
                Pair<RexNode, Boolean> newOperandPair =
                    findAndReplaceDefaultOnDuplicateKeyUpdateList(assignment, targetFieldName);
                if (newOperandPair.getValue()) {
                    modify = true;
                    newAssignment = newOperandPair.getKey();
                }
            }

            //替换了default值,修改RexNode
            if (modify) {
                relNode = bb.getRexBuilder()
                    .makeCall(((RexCall) relNode).getOperator(), ImmutableList.of(operandOne, newAssignment));
            }

            return relNode;
        }

        /**
         * 获取defaultFieldName列的默认值，赋值的目标列是targetFieldName
         */
        private RexNode getFieldDefaultValueOnDuplicateKeyUpdateList(RexNode node, String defaultFieldName,
                                                                     String targetFieldName) {
            RexNode newNode = null;
            final int index = targetTable.getRowType().getFieldNames().indexOf(defaultFieldName);
            if (index < 0) {
                throw new SqlValidateException(
                    validator.newValidationError(sqlNode, RESOURCE.unknownTargetColumn(defaultFieldName)));
            }
            final Field targetField = tableMeta.getColumn(targetFieldName).getField();
            final String columnDefaultStr = tableMeta.getColumn(defaultFieldName).getField().getDefault();

            if (null == columnDefaultStr && !targetField.isNullable()) {
                //目标列不能为空，获取默认值的列没有默认值，但是需要使用默认值，检查是否用隐式获取默认值
                if (!sqlModeStrict) {
                    newNode = initializerFactory.newImplicitDefaultValue(targetTable, index, bb);
                }
                if (null == newNode) {
                    // Columns not accept NULL but has no default value or AUTO_INCREMENT property
                    throw new SqlValidateException(
                        validator.newValidationError(sqlNode, RESOURCE.columnNotNullable(defaultFieldName)));
                }
            } else {
                newNode = initializerFactory.newColumnDefaultValue(targetTable, index, bb);
                newNode = convertDefaultValue(newNode, targetTable, index, tableMeta, ec);
            }

            if (null == newNode) {
                // Update target data type for DEFAULT call not replaced
                // 考虑下推执行
                newNode = bb.getRexBuilder()
                    .makeCall(new SqlDefaultOperator(node.getType().getSqlTypeName()),
                        ((RexCall) node).getOperands());
            } else {
                newNode = castNullLiteralIfNeeded(newNode, node.getType());
            }
            return newNode;
        }

        private Pair<RexNode, Boolean> findAndReplaceDefaultOnDuplicateKeyUpdateList(RexNode node,
                                                                                     String targetFieldName) {

            if (node instanceof RexCall && node.getKind() == SqlKind.DEFAULT &&
                !((RexCall) node).getOperands().isEmpty()) {
                //default(c2) 表达式
                RexNode field = ((RexCall) node).getOperands().get(0);
                String defaultFieldName = null;
                if (field instanceof RexInputRef) {
                    defaultFieldName = allColumnNames.get(((RexInputRef) field).getIndex());
                }
                if (defaultFieldName == null) {
                    throw new SqlValidateException(
                        validator.newValidationError(sqlNode, RESOURCE.unknownField(node.toString())));
                }
                RexNode newNode = getFieldDefaultValueOnDuplicateKeyUpdateList(node, defaultFieldName, targetFieldName);
                return Pair.of(newNode, true);
            } else if (node instanceof RexCall && node.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC)) {
                //计算式，则遍历所有getOperands()
                List<RexNode> operands = ((RexCall) node).getOperands();
                List<RexNode> newOperands = new ArrayList<>(operands.size());
                boolean modify = false;
                for (RexNode operand : operands) {
                    Pair<RexNode, Boolean> newOperandPair =
                        findAndReplaceDefaultOnDuplicateKeyUpdateList(operand, targetFieldName);
                    if (newOperandPair.getValue()) {
                        modify = true;
                    }
                    newOperands.add(newOperandPair.getKey());
                }
                if (modify) {
                    RexNode newNode = bb.getRexBuilder().makeCall(((RexCall) node).getOperator(), newOperands);
                    return Pair.of(newNode, true);
                }
            }
            return Pair.of(node, false);
        }
    }

    /**
     * <pre>
     * Add columns which are not specified in INSERT/REPLACE statement
     *   1. Add AUTO_INCREMENT column with a value of NULL, For later be replaced with sequence by executor
     *   2. Add partition key of primary and gsi table with the default value from table meta
     *   3. Add columns of which has property DEFAULT CURRENT_TIMESTAMP and included in gsi, with a value of RexCall CURRENT_TIMESTAMP
     *   4. Add columns of which is referenced in ON DUPLICATE KEY UPDATE
     *   5. Add target column in multi-write
     *
     * Replace keyword DEFAULT with a literal of default value (or a RexCall if default value is a function)
     *
     * Example:
     *   create table test1(
     *      id bigint(20) NOT NULL AUTO_INCREMENT,
     *      b int default 1,
     *      c int default 2,
     *      d int default 3,
     *      e int default 4,
     *      f timestamp default current_timestamp,
     *      g int default 5,
     *      h int default 6,
     *      primary key(id),
     *      unique key u_e(d),
     *      unique global index u_g_e(e) covering(f) dbpartition by hash(e)
     *   ) dbpartition by hash(c);
     *
     *   insert into test1(id, b) values(default,default) on duplicate key update g = values(g) + h;
     *
     *   after convert:
     *
     *   insert into test1(id, b, c, d, e, f, g, h) values(null, 1, 2, 3, 4, current_timestamp(), 5, 6)
     *      on duplicate key update g = values(g) + h;
     *
     * </pre>
     */
    @Override
    protected RelNode convertColumnList(final SqlInsert call, RelNode source, Set<Integer> appendedColumnIndex) {
        RelDataType sourceRowType = source.getRowType();
        final RexNode sourceRef = rexBuilder.makeRangeReference(sourceRowType, 0, false);
        final List<String> targetColumnNames = new ArrayList<>();
        final List<RexNode> columnExprs = new ArrayList<>();
        collectInsertTargets(call, sourceRef, targetColumnNames, columnExprs);

        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType targetRowType = RelOptTableImpl.realRowType(targetTable);
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();

        final List<RexNode> sourceExps = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();

        final Supplier<Blackboard> bb = () -> createInsertBlackboard(targetTable, sourceRef, targetColumnNames);

        final InitializerExpressionFactory initializerFactory = getInitializerFactory(validator.getNamespace(call)
            .getTable());

        List<String> qualifiedName = targetTable.getQualifiedName();
        String tableName = Util.last(qualifiedName);
        String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        TableMeta tableMeta = plannerContext.getExecutionContext().getSchemaManager(schemaName).getTable(tableName);
        final boolean isBroadcast = oc.getRuleManager().isBroadCast(tableName);
        final boolean isPartitioned = oc.getRuleManager().isShard(tableName);
        final boolean withGsi = tableMeta.withGsi();
        final boolean withScaleOutMultiWrite = ComplexTaskPlanUtils.canWrite(tableMeta);
        final ExecutionStrategy hintEx = getExecutionStrategy();
        final boolean replaceNonDeterministicFunction = hintEx.replaceNonDeterministicFunction();
        final boolean isReplaceOrInsertIgnore =
            (call instanceof SqlReplace) || call.getModifierNode(SqlDmlKeyword.IGNORE) != null;

        // Add auto_increment column to insert with value of NULL;
        final Set<String> autoIncrementColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        autoIncrementColumns.addAll(tableMeta.getAutoIncrementColumns());

        // Add default value for partition keys of primary and gsi
        final Set<String> partitionKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        partitionKeys.addAll(oc.getRuleManager().getSharedColumns(tableName));
        if (withGsi) {
            tableMeta.getGsiTableMetaBean().indexMap.keySet()
                .forEach(indexTable -> partitionKeys.addAll(oc.getRuleManager().getSharedColumns(indexTable)));
        }

        // Add default value for unique keys of primary and gsi
        final Set<String> uniqueKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (isPartitioned || (withScaleOutMultiWrite && isBroadcast) || (isReplaceOrInsertIgnore && (withGsi
            || hintEx == ExecutionStrategy.LOGICAL))) {
            uniqueKeys.addAll(GlobalIndexMeta
                .getUniqueKeyColumnList(tableName, schemaName, false, plannerContext.getExecutionContext()));
        }

        // Auto fill the default value if the column marked. This is useful when change default in table with GSI.
        final Set<String> autoFillDefaultColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (withGsi) {
            tableMeta.getAllColumns().stream().filter(ColumnMeta::isFillDefault)
                .forEach(col -> autoFillDefaultColumns.add(col.getName()));
        }

        // Add function call for all columns with DEFAULT CURRENT_TIMESTAMP of writable(WRITE_ONLY and PUBLISH) gsi
        // Scenario like "partition key with DEFAULT CURRENT_TIMESTAMP" is included in set partitionKeys above
        final Set<String> defaultCurrentTimestamp = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (withGsi) {
            final List<TableMeta> indexes = GlobalIndexMeta
                .getIndex(tableName, schemaName, IndexStatus.WRITABLE, plannerContext.getExecutionContext());

            indexes.stream().flatMap(index -> index.getAllColumns().stream())
                .filter(cm -> !defaultCurrentTimestamp.contains(cm.getName()) && TStringUtil
                    .containsIgnoreCase(cm.getField().getDefault(), "CURRENT_TIMESTAMP"))
                .forEach(cm -> defaultCurrentTimestamp.add(cm.getName()));
        }

        // Add function call for all columns with DEFAULT CURRENT_TIMESTAMP of broadcast and scaleout writable table
        if (isBroadcast || withScaleOutMultiWrite || replaceNonDeterministicFunction) {
            tableMeta.getAllColumns().stream()
                .filter(cm -> !defaultCurrentTimestamp.contains(cm.getName()) && TStringUtil
                    .containsIgnoreCase(cm.getField().getDefault(), "CURRENT_TIMESTAMP"))
                .forEach(cm -> defaultCurrentTimestamp.add(cm.getName()));
        }

        // Check all primary key has been specified a value
        final Set<String> primaryKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (tableMeta.isHasPrimaryKey()) {
            tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).forEach(primaryKeys::add);
        }

        // Get sql_mode
        final ExecutionContext ec = getPlannerContext().getExecutionContext();
        final boolean sqlModeStrict =
            Optional.ofNullable(oc.getVariableManager()).map(vm -> vm.isSqlModeStrict(ec)).orElse(true);

        // Replace DEFAULT with default value
        final ReplaceDefaultShuttle replaceDefaultShuttle =
            new ReplaceDefaultShuttle(targetFields, targetColumnNames, targetTable, bb, initializerFactory, tableMeta,
                call.getSource(), autoIncrementColumns, sqlModeStrict, ec);
        source = source.accept(replaceDefaultShuttle);
        // Update columnExpr with data type from new source
        final List<RelDataTypeField> sourceFields = source.getRowType().getFieldList();
        IntStream.range(0, columnExprs.size()).forEach(i -> {
            final RexNode columnExpr = columnExprs.get(i);
            if (columnExpr instanceof RexInputRef) {
                final int refIndex = ((RexInputRef) columnExpr).getIndex();
                final RelDataTypeField sourceField = sourceFields.get(refIndex);

                if (sourceField.getType().getSqlTypeName() != columnExpr.getType().getSqlTypeName()) {
                    columnExprs.set(i, RexInputRef.of(refIndex, sourceFields));
                }
            }
        });

        // Aad source column's default value in column multi-write, otherwise the default values of primary and gsi may
        // differ
        final TableColumnMeta tableColumnMeta = tableMeta.getTableColumnMeta();
        Pair<String, String> columnMapping = TableColumnUtils.getColumnMultiWriteMapping(tableColumnMeta, ec);

        // Append all column in target table for upsert with multi write
        final SqlNodeList duplicateKeyUpdateList = (SqlNodeList) call.getOperandList().get(4);
        // Whether UPSERT modify partition key
        final List<String> updateColumns = BuildPlanUtils.buildUpdateColumnList(duplicateKeyUpdateList, (sqlNode) -> {
            throw new SqlValidateException(validator.newValidationError(sqlNode,
                RESOURCE.unsupportedCallInDuplicateKeyUpdate(RelUtils.stringValue(sqlNode))));
        });
        final boolean upsertModifyPartitionKey = updateColumns.stream().anyMatch(partitionKeys::contains);

        // Append all column in target table for upsert with multi write
        final Boolean pushdownDuplicateCheck = isPushdownDuplicateCheck();
        final boolean appendAllColumnsForUpsert =
            duplicateKeyUpdateList.size() > 0 && (isBroadcast || withGsi || withScaleOutMultiWrite
                || !pushdownDuplicateCheck || upsertModifyPartitionKey || hintEx == ExecutionStrategy.LOGICAL
                || columnMapping != null);

        // Walk the expression list and get default values for columns that were wanted and not supplied
        // in the statement. Get field names too.
        for (int i = 0; i < targetFields.size(); i++) {
            final RelDataTypeField targetField = targetFields.get(i);
            final String targetFieldName = targetField.getName();
            int index = targetColumnNames.indexOf(targetFieldName);
            RexNode node = null;
            if (index >= 0) {
                node = columnExprs.get(index);
            } else if (autoIncrementColumns.contains(targetFieldName)) {
                // Add auto_increment column to insert with value of NULL;
                node = bb.get().getRexBuilder().constantNull();
            } else if (columnMapping != null && targetFieldName.equalsIgnoreCase(columnMapping.right)) {
                // Do nothing, we will add it later in WriterFactory
            } else if (partitionKeys.contains(targetFieldName) || uniqueKeys.contains(targetFieldName)
                || defaultCurrentTimestamp.contains(targetFieldName) || primaryKeys.contains(targetFieldName)
                || appendAllColumnsForUpsert || autoFillDefaultColumns.contains(targetFieldName) || (
                columnMapping != null && targetFieldName.equalsIgnoreCase(columnMapping.left))) {
                // Add literal or function call as default value of column;
                node =
                    convertDefaultValue(initializerFactory.newColumnDefaultValue(targetTable, i, bb.get()), targetTable,
                        i, tableMeta, ec);

                if (null == node && (primaryKeys.contains(targetFieldName) || !targetField.getType().isNullable())) {
                    if (!sqlModeStrict) {
                        node = initializerFactory.newImplicitDefaultValue(targetTable, i, bb.get());
                    }

                    if (null == node) {
                        // Invalidated value for primary key or columns with not null property
                        throw new SqlValidateException(
                            validator
                                .newValidationError(call.getSource(), RESOURCE.columnNotNullable(targetFieldName)));
                    }
                }
            }

            if (null == node) {
                continue;
            }

            node = castNullLiteralIfNeeded(node, targetField.getType());
            sourceExps.add(node);
            fieldNames.add(targetFieldName);
        }

        // A project with target rows must be reserved, so optimize=false.
        return RelOptUtil.createProject(source, sourceExps, fieldNames, false);
    }

    private ExecutionStrategy getExecutionStrategy() {
        return Optional.ofNullable(this.getPlannerContext())
            .map(pc -> ExecutionStrategy.fromHint(pc.getExecutionContext())).orElse(ExecutionStrategy.PUSHDOWN);
    }

    private Boolean isPushdownDuplicateCheck() {
        return Optional.ofNullable(this.getPlannerContext())
            .map(pc -> pc.getExecutionContext().getParamManager().getBoolean(ConnectionParams.DML_PUSH_DUPLICATE_CHECK))
            .orElse(Boolean.valueOf(ConnectionParams.DML_PUSH_DUPLICATE_CHECK.getDefault()));
    }

    /**
     * Replace keyword DEFAULT with a literal of default value (or a RexCall if default value is a function)
     * <p>
     * Example:
     * create table test1(id bigint(20) unsigned NOT NULL AUTO_INCREMENT, b int default 1, primary key(id));
     * insert into test1 values(default,default);
     * <p>
     * after convert:
     * <p>
     * insert into test1(id, b) values(null, 1);
     */
    private class ReplaceDefaultShuttle extends RelShuttleImpl {
        private final List<RelDataTypeField> targetFields;
        private final List<String> targetColumnNames;
        private final RelOptTable targetTable;
        private final Supplier<Blackboard> bb;
        private final InitializerExpressionFactory initializerFactory;
        private final TableMeta tableMeta;
        private final SqlNode sqlNode;
        private final Set<String> autoIncrementColumns;
        private final boolean sqlModeStrict;
        private final ExecutionContext ec;

        private final Deque<Boolean> isTop = new ArrayDeque<>();

        public ReplaceDefaultShuttle(List<RelDataTypeField> targetFields,
                                     List<String> targetColumnNames,
                                     RelOptTable targetTable, Supplier<Blackboard> bb,
                                     InitializerExpressionFactory initializerFactory,
                                     TableMeta tableMeta, SqlNode sqlNode,
                                     Set<String> autoIncrementColumns, boolean sqlModeStrict,
                                     ExecutionContext ec) {
            this.targetFields = targetFields;
            this.targetColumnNames = targetColumnNames;
            this.targetTable = targetTable;
            this.bb = bb;
            this.initializerFactory = initializerFactory;
            this.tableMeta = tableMeta;
            this.sqlNode = sqlNode;
            this.autoIncrementColumns = autoIncrementColumns;
            this.isTop.push(true);
            this.sqlModeStrict = sqlModeStrict;
            this.ec = ec;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            LogicalProject visited;

            this.isTop.push(false);
            try {
                visited = (LogicalProject) super.visit(project);
            } finally {
                this.isTop.pop();
            }

            if (Boolean.FALSE.equals(this.isTop.peek())) {
                return visited;
            }

            final List<RexNode> oriExps = visited.getChildExps();

            boolean defaultReplaced = false;
            final RexNode[] sourceExps = new RexNode[oriExps.size()];
            final String[] fieldNames = new String[oriExps.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                final RelDataTypeField targetField = targetFields.get(i);
                final String targetFieldName = targetField.getName();
                final int index = targetColumnNames.indexOf(targetFieldName);
                if (index >= 0) {
                    RexNode node = oriExps.get(index);
                    //关键字default替换值，例如values(default, c1, c1 + 1, default(c2), default(c2) + 1)
                    //mysql：values(default + 1)不支持，所以不支持 default + 1 等计算式，
                    if (node instanceof RexCall && ((RexCall) node).getOperator().getKind() == SqlKind.DEFAULT
                        && ((RexCall) node).getOperands().isEmpty()) {
                        // 简单的default关键字，例如：values(default);
                        node = getFieldDefaultValue(node, targetField, targetFieldName, false);
                        defaultReplaced = true;
                    } else {
                        //递归替换所有values(c1 + 1, default(c2) + 1, c1 + 1 + 1)等形式的RexCall
                        Pair<RexNode, Boolean> newNodePair = replaceDefaultValueSpecial(node, targetFieldName, false);
                        if (newNodePair.getValue()) {
                            node = newNodePair.getKey();
                            defaultReplaced = true;
                        }
                    }

                    sourceExps[index] = node;
                    fieldNames[index] = targetFieldName;
                }
            }

            if (defaultReplaced) {
                return RelOptUtil
                    .createProject(visited.getInput(), ImmutableList.copyOf(sourceExps),
                        ImmutableList.copyOf(fieldNames),
                        false);
            } else {
                return visited;
            }
        }

        /**
         * 获取某个列（Field）的默认值，参数：
         * <li>defaultField代表获取default值的列 </li>
         * <li>targetFieldName要插入的列名</li>
         * <li>isArithmetic父节点是否是计算式。</li>
         * <br>
         * 特殊：当某列 c1 是AUTO_INCREMENT：
         * <li>例1：insert t1(c1) values(default),(c1),(default(c1)); 应该是获取sequence</li>
         * <li>例2：insert t1(c1) values(c1 + 1), c1的default应该隐式转换0，而不是null去获取sequence</li>
         * <li>例3：insert t1(c2) values(c1),(c1 + 1); c1的default应该隐式转换0，而不是null去获取sequence</li>
         */
        private RexNode getFieldDefaultValue(RexNode node, RelDataTypeField defaultField, String targetFieldName,
                                             boolean isArithmetic) {
            RelDataType type = node.getType();
            RexNode newNode = null;
            final String defaultFieldName = defaultField.getName();
            final int index = targetFields.indexOf(defaultField);
            if (index < 0) {
                throw new SqlValidateException(
                    validator.newValidationError(sqlNode, RESOURCE.unknownTargetColumn(defaultField.getName())));
            }
            final Field targetField = tableMeta.getColumn(targetFieldName).getField();
            final String columnDefaultStr = tableMeta.getColumn(defaultFieldName).getField().getDefault();

            if (!isArithmetic && autoIncrementColumns.contains(targetFieldName)
                && autoIncrementColumns.contains(defaultFieldName)) {
                //不是计算式，目标列是 AUTO_INCREMENT，要获取的default列也是AUTO_INCREMENT；用null，后续获取sequence
                newNode = bb.get().getRexBuilder().constantNull();
                type = defaultField.getType();

            } else if (null == columnDefaultStr && !targetField.isNullable()) {
                //目标列不能为空，获取默认值的列没有默认值，但是需要使用默认值，检查是否用隐式获取默认值
                if (!sqlModeStrict) {
                    newNode = initializerFactory.newImplicitDefaultValue(targetTable, index, bb.get());
                }
                if (null == newNode) {
                    // Columns not accept NULL but has no default value or AUTO_INCREMENT property
                    throw new SqlValidateException(
                        validator.newValidationError(sqlNode, RESOURCE.columnNotNullable(defaultFieldName)));
                }
            } else {
                newNode = initializerFactory.newColumnDefaultValue(targetTable, index, bb.get());
                newNode = convertDefaultValue(newNode, targetTable, index, tableMeta, ec);
            }

            if (null == newNode) {
                // Update target data type for DEFAULT call not replaced
                // 考虑下推执行
                if (node instanceof RexCall) {
                    newNode = bb.get().getRexBuilder()
                        .makeCall(new SqlDefaultOperator(defaultField.getType().getSqlTypeName()),
                            ((RexCall) node).getOperands());
                } else if (node instanceof RexFieldAccess) {
                    //将列换成DEFAULT(c1)
                    newNode = bb.get().getRexBuilder()
                        .makeCall(new SqlDefaultOperator(defaultField.getType().getSqlTypeName()), node);
                } else {
                    newNode = bb.get().getRexBuilder()
                        .makeCall(new SqlDefaultOperator(defaultField.getType().getSqlTypeName()));
                }

            } else {
                newNode = castNullLiteralIfNeeded(newNode, type);
            }
            return newNode;
        }

        /**
         * 遍历所有RexCall，替换特殊情况的default值.
         * 例如：values(c1, c1 + 1, default(c2), default(c2) + 1);
         * <br>
         * 返回值 Boolean 表示是否进行了修改.
         */
        private Pair<RexNode, Boolean> replaceDefaultValueSpecial(RexNode node, String targetFieldName,
                                                                  boolean isArithmetic) {
            /*
              三种情况：
              1.values(c1)
              2.values(c1 + 1), c1是别的类型，可能有一层CAST
              3.values(default(c1) + 1);
             */
            if (node instanceof RexFieldAccess ||
                (node instanceof RexCall && node.getKind() == SqlKind.CAST
                    && ((RexCall) node).getOperands().size() == 1
                    && ((RexCall) node).getOperands().get(0) instanceof RexFieldAccess) ||
                (node instanceof RexCall && node.getKind() == SqlKind.DEFAULT &&
                    !((RexCall) node).getOperands().isEmpty())) {
                RelDataTypeField defaultField = null;
                if (node instanceof RexFieldAccess) {
                    //values(c1)情况
                    defaultField = ((RexFieldAccess) node).getField();
                } else {
                    //values(default(c1))情况
                    RexNode field = ((RexCall) node).getOperands().get(0);
                    if (field instanceof RexFieldAccess) {
                        defaultField = ((RexFieldAccess) field).getField();
                    }
                }
                if (defaultField == null || !targetFields.contains(defaultField)) {
                    throw new SqlValidateException(
                        validator.newValidationError(sqlNode, RESOURCE.unknownTargetColumn(node.toString())));
                }
                RexNode newNode = getFieldDefaultValue(node, defaultField, targetFieldName, isArithmetic);
                if (node instanceof RexCall && node.getKind() == SqlKind.CAST) {
                    newNode = bb.get().getRexBuilder()
                        .makeCall(node.getType(), ((RexCall) node).getOperator(), ImmutableList.of(newNode));
                }
                return Pair.of(newNode, true);

            } else if (node instanceof RexCall && node.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC)) {
                //计算式RexCall，遍历每一个operand是否需要替换的default值
                List<RexNode> operands = ((RexCall) node).getOperands();
                List<RexNode> newOperands = new ArrayList<>(operands.size());
                boolean modify = false;
                for (RexNode operand : operands) {
                    Pair<RexNode, Boolean> newOperandPair =
                        replaceDefaultValueSpecial(operand, targetFieldName, true);
                    if (newOperandPair.getValue()) {
                        modify = true;
                    }
                    newOperands.add(newOperandPair.getKey());
                }
                if (modify) {
                    RexNode newNode = bb.get().getRexBuilder().makeCall(((RexCall) node).getOperator(), newOperands);
                    return Pair.of(newNode, true);
                }

            }
            return Pair.of(node, false);
        }

    }

    private class ValuesCallFinder extends SqlShuttle {
        final List<SqlIdentifier> refColumns = new ArrayList<>();
        final boolean includingDirectColumnRef;
        final AtomicBoolean withDirectColumnRef = new AtomicBoolean(false);

        public ValuesCallFinder(boolean includingDirectColumnRef) {
            this.includingDirectColumnRef = includingDirectColumnRef;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            final SqlOperator op = call.getOperator();
            if ("VALUES".equalsIgnoreCase(op.getName())) {
                refColumns.add((SqlIdentifier) call.getOperandList().get(0));
                return call;
            }
            return super.visit(call);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            withDirectColumnRef.set(true);
            if (includingDirectColumnRef) {
                refColumns.add(id);
            }
            return super.visit(id);
        }

        public boolean withDirectColumnRef() {
            return withDirectColumnRef.get();
        }
    }

    public static String unwrapGsiName(String wrappedName) {
        final int len = wrappedName.length();
        if (len > 6 && wrappedName.startsWith("_$", len - 6)) {
            return wrappedName.substring(0, len - 6);
        }
        return wrappedName;
    }

    public static String unwrapPhysicalTableName(String wrappedName) {
        String ret = wrappedName;
        int len = wrappedName.length();
        while (len > 4 && ret.startsWith("_%", len - 4)) {
            ret = ret.substring(0, len - 4);
            len = ret.length();
        }
        return ret;
    }

    public static String unwrapLocalIndexName(String wrappedName) {
        final int len = wrappedName.length();
        if (len > 7 && wrappedName.startsWith(AUTO_LOCAL_INDEX_PREFIX)) {
            return wrappedName.substring(7, len);
        }
        return wrappedName;
    }

    private String assignGsiName(SqlNode queryName, Set<String> existsNames, String preferName) {
        preferName = unwrapGsiName(preferName); // Unwrap and assign again.
        if (existsNames.contains(preferName)) {
            throw validator.newValidationError(queryName, RESOURCE.gsiExists(preferName));
        }
        // Put in the set.
        existsNames.add(preferName);
        // Assign new name with suffix.
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        String fullName;
        do {
            final String suffix = "_$" + formatter.format("%04x", random.nextInt(0x10000));
            fullName = preferName + suffix;
        } while (!plannerContext.getExecutionContext().getSchemaManager(plannerContext.getSchemaName())
            .getGsi(fullName, IndexStatus.ALL).isEmpty());
        return fullName;
    }

    @Override
    protected SqlCreateTable checkAndRewriteGsiName(SqlCreateTable query) {

        if (plannerContext.isExplain()) {
            return query;
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(plannerContext.getSchemaName())) {
            // Collect all local index names.
            final Set<String> existsNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            if (GeneralUtil.isNotEmpty(query.getKeys())) {
                query.getKeys().forEach(pair -> existsNames.add(pair.left.getLastName()));
            }
            if (GeneralUtil.isNotEmpty(query.getUniqueKeys())) {
                query.getUniqueKeys().forEach(pair -> existsNames.add(pair.left.getLastName()));
            }

            // Reassign name.
            if (GeneralUtil.isNotEmpty(query.getGlobalKeys())) {
                final List<Pair<SqlIdentifier, SqlIndexDefinition>> tmp = new ArrayList<>(query.getGlobalKeys().size());
                query.getGlobalKeys().forEach(pair -> {
                    final String newName = assignGsiName(query.getName(), existsNames, pair.getKey().getLastName());
                    final SqlIdentifier newIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);
                    tmp.add(new Pair<>(newIdentifier, pair.right.rebuildToGsiNewPartition(newIdentifier, null, false)));
                });
                query.setGlobalKeys(tmp);
            }
            if (GeneralUtil.isNotEmpty(query.getGlobalUniqueKeys())) {
                final List<Pair<SqlIdentifier, SqlIndexDefinition>> tmp =
                    new ArrayList<>(query.getGlobalUniqueKeys().size());
                query.getGlobalUniqueKeys().forEach(pair -> {
                    final String newName = assignGsiName(query.getName(), existsNames, pair.getKey().getLastName());
                    final SqlIdentifier newIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);
                    tmp.add(new Pair<>(newIdentifier, pair.right.rebuildToGsiNewPartition(newIdentifier, null, false)));
                });
                query.setGlobalUniqueKeys(tmp);
            }
            if (GeneralUtil.isNotEmpty(query.getClusteredKeys())) {
                final List<Pair<SqlIdentifier, SqlIndexDefinition>> tmp =
                    new ArrayList<>(query.getClusteredKeys().size());
                query.getClusteredKeys().forEach(pair -> {
                    final String newName = assignGsiName(query.getName(), existsNames, pair.getKey().getLastName());
                    final SqlIdentifier newIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);
                    tmp.add(new Pair<>(newIdentifier, pair.right.rebuildToGsiNewPartition(newIdentifier, null, true)));
                });
                query.setClusteredKeys(tmp);
            }
            if (GeneralUtil.isNotEmpty(query.getClusteredUniqueKeys())) {
                final List<Pair<SqlIdentifier, SqlIndexDefinition>> tmp =
                    new ArrayList<>(query.getClusteredUniqueKeys().size());
                query.getClusteredUniqueKeys().forEach(pair -> {
                    final String newName = assignGsiName(query.getName(), existsNames, pair.getKey().getLastName());
                    final SqlIdentifier newIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);
                    tmp.add(new Pair<>(newIdentifier, pair.right.rebuildToGsiNewPartition(newIdentifier, null, true)));
                });
                query.setClusteredUniqueKeys(tmp);
            }
        }

        final Set<String> gsiNames = new HashSet<>();
        if (GeneralUtil.isNotEmpty(query.getGlobalKeys())) {
            query.getGlobalKeys().forEach(pair -> {
                validator.validateGsiName(gsiNames, pair.getKey());
                final String indexName = pair.getKey().getLastName();

                if (!plannerContext.getExecutionContext().getSchemaManager(plannerContext.getSchemaName())
                    .getGsi(indexName,
                        IndexStatus.ALL).isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
                }
            });
        }

        if (GeneralUtil.isNotEmpty(query.getGlobalUniqueKeys())) {
            query.getGlobalUniqueKeys().forEach(pair -> {
                validator.validateGsiName(gsiNames, pair.getKey());
                final String indexName = pair.getKey().getLastName();
                if (!OptimizerContext.getContext(plannerContext.getSchemaName()).getLatestSchemaManager()
                    .getGsi(indexName,
                        IndexStatus.ALL).isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
                }
            });
        }

        if (GeneralUtil.isNotEmpty(query.getClusteredKeys())) {
            query.getClusteredKeys().forEach(pair -> {
                validator.validateGsiName(gsiNames, pair.getKey());
                final String indexName = pair.getKey().getLastName();
                if (!OptimizerContext.getContext(plannerContext.getSchemaName()).getLatestSchemaManager()
                    .getGsi(indexName,
                        IndexStatus.ALL).isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
                }
            });
        }

        if (GeneralUtil.isNotEmpty(query.getClusteredUniqueKeys())) {
            query.getClusteredUniqueKeys().forEach(pair -> {
                validator.validateGsiName(gsiNames, pair.getKey());
                final String indexName = pair.getKey().getLastName();
                if (!plannerContext.getExecutionContext().getSchemaManager(plannerContext.getSchemaName())
                    .getGsi(indexName,
                        IndexStatus.ALL).isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
                }
            });
        }

        final String tableName = ((SqlIdentifier) query.getName()).getLastName();
        if (gsiNames.contains(tableName)) {
            throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(tableName));
        }

        return query;
    }

    static private SqlNode generateNewPartition(TableMeta tableMeta, List<String> indexColNames, boolean unique) {
        final List<ColumnMeta> columnMetas = new ArrayList<>(indexColNames.size());
        for (String name : indexColNames) {
            ColumnMeta meta =
                tableMeta.getPhysicalColumns().stream().filter(col -> col.getName().equalsIgnoreCase(name)).findFirst()
                    .get();
            if (meta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Unknown GSI column '" + name + "'");
            }
            columnMetas.add(meta);
        }
        final List<String> typeNames = columnMetas.stream()
            .map(meta -> meta.getField().getDataType().getStringSqlType().toLowerCase()).collect(Collectors.toList());

        if (typeNames.isEmpty() || !SqlValidatorImpl.supportNewPartition(typeNames.get(0))) {
            return null;
        }

        final List<SqlIdentifier> pks = tableMeta.getPrimaryKey().stream()
            .map(col -> new SqlIdentifier(col.getName(), SqlParserPos.ZERO))
            .collect(Collectors.toList());
        final List<String> pkTypeNames = tableMeta.getPrimaryKey().stream()
            .map(meta -> meta.getField().getDataType().getStringSqlType().toLowerCase())
            .collect(Collectors.toList());

        final List<SqlIdentifier> concatKeys = indexColNames.stream()
            .map(name -> new SqlIdentifier(name, SqlParserPos.ZERO))
            .collect(Collectors.toList());
        if (!unique) {
            // Only concat PK for non-unique.
            concatKeys.addAll(pks);
            typeNames.addAll(pkTypeNames);
        }
        assert concatKeys.size() == typeNames.size();

        return SqlValidatorImpl.assignAutoPartitionNewPartition(concatKeys, typeNames);
    }

    @Override
    protected SqlCreateIndex checkAndRewriteGsiName(SqlCreateIndex query) {
        final String schemaName =
            2 == query.getOriginTableName().names.size() ? query.getOriginTableName().names.get(0) :
                plannerContext.getSchemaName();
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (null == oc) {
            throw validator.newValidationError(query.getName(), RESOURCE.schemaNotFound(schemaName));
        }
        final TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
            .getTable(query.getOriginTableName().getLastName());
        final boolean convertToGSI =
            tableMeta.isAutoPartition() && query.getIndexResiding() != SqlIndexDefinition.SqlIndexResiding.LOCAL
                // Ignore special index.
                && (null == query.getConstraintType()
                || SqlCreateIndex.SqlIndexConstraintType.UNIQUE == query.getConstraintType());

        if (plannerContext.isExplain() || (!query.createGsi() && !convertToGSI)) {
            return query;
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            // Collect existing names.
            final Set<String> existsNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            tableMeta.getSecondaryIndexes().forEach(meta -> existsNames.add(meta.getPhysicalIndexName()));
            if (tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().indexMap != null) {
                tableMeta.getGsiTableMetaBean().indexMap.forEach((k, v) -> existsNames.add(unwrapGsiName(k)));
            }

            // Assign name if no name spec.
            final String orgName;
            if (null == query.getIndexName() || null == query.getIndexName().getLastName()) {
                final String baseName = "i_";
                int prob = 0;
                while (existsNames.contains(baseName + prob)) {
                    ++prob;
                }
                orgName = baseName + prob;
            } else {
                orgName = query.getIndexName().getLastName();
            }

            // Now assign gsi name and do auto partition rewrite.
            final String newName = assignGsiName(query.getName(), existsNames, orgName);
            final SqlIdentifier newNameIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);

            if (tableMeta.isAutoPartition()) {
                // Assign name and partition.
                if (query.getDbPartitionBy() != null || query.getDbPartitions() != null ||
                    query.getTbPartitionBy() != null || query.getTbPartitions() != null) {
                    throw new NotSupportException(
                        "Incorrect index definition; New partition table should contain no legacy partition define.");
                }

                if (query.getColumns().isEmpty()) {
                    throw new NotSupportException(
                        "Incorrect index definition; No index column.");
                }

                final List<String> indexColNames = query.getColumns().stream()
                    .map(SqlIndexColumnName::getColumnNameStr).collect(Collectors.toList());
                final boolean unique = query.getConstraintType() != null &&
                    SqlCreateIndex.SqlIndexConstraintType.UNIQUE == query.getConstraintType();
                final SqlNode newPartition =
                    null == query.getPartitioning() ? generateNewPartition(tableMeta, indexColNames, unique) : null;
                if (null == query.getPartitioning() && null == newPartition) {
                    return query; // No extra dealing needed.
                } else {
                    // Convert to GSI only if new partition is generated or it has one(GSI).
                    query = query.rebuildToGsiNewPartition(
                        newNameIdentifier, newPartition, query.createClusteredIndex());
                }
            } else {
                // Assign name only.
                assert query.getIndexResiding()
                    == SqlIndexDefinition.SqlIndexResiding.GLOBAL; // Not convert to GSI, so it is GSI.
                query = query.rebuildToGsiNewPartition(newNameIdentifier, null, query.createClusteredIndex());
            }
        }

        final Set<String> gsiNames = new HashSet<>();
        validator.validateGsiName(gsiNames, query.getIndexName());

        final String indexName = query.getIndexName().getLastName();
        if (!plannerContext.getExecutionContext().getSchemaManager(schemaName)
            .getGsi(indexName, IndexStatus.ALL).isEmpty()) {
            throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
        }

        final String tableName = ((SqlIdentifier) query.getName()).getLastName();
        if (gsiNames.contains(tableName)) {
            throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(tableName));
        }

        return query;
    }

    @Override
    protected SqlDropIndex checkAndRewriteGsiName(SqlDropIndex query) {
        // Do drop GSI name replace.
        final String schemaName =
            2 == query.getOriginTableName().names.size() ? query.getOriginTableName().names.get(0) :
                plannerContext.getSchemaName();
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (null == oc) {
            throw validator.newValidationError(query.getName(), RESOURCE.schemaNotFound(schemaName));
        }
        final TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
            .getTable(query.getOriginTableName().getLastName());

        if (!tableMeta.withGsi()) {
            return query;
        }

        if (null == query.getIndexName() || query.getIndexName().getLastName().isEmpty()) {
            throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(""));
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            // Add GSI suffix if drop GSI.
            final String indexName = query.getIndexName().getLastName();
            final String wrapped = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(indexName))
                .findFirst().orElse(null);
            if (wrapped != null) {
                query = query.replaceIndexName(new SqlIdentifier(wrapped, SqlParserPos.ZERO));
            }
        }

        return query;
    }

    @Override
    protected SqlAlterTable checkAndRewriteGsiName(SqlAlterTable query) {
        final String schemaName =
            2 == query.getOriginTableName().names.size() ? query.getOriginTableName().names.get(0) :
                plannerContext.getSchemaName();
        final TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
            .getTable(query.getOriginTableName().getLastName());

        // Pre check of single GSI related operation.
        boolean check = false;
        for (SqlAlterSpecification alterSpecification : query.getAlters()) {
            if (alterSpecification instanceof SqlAddIndex) {
                final SqlAddIndex addIndex = (SqlAddIndex) alterSpecification;

                final boolean convertToGSI =
                    tableMeta.isAutoPartition()
                        && addIndex.getIndexDef().getIndexResiding() != SqlIndexDefinition.SqlIndexResiding.LOCAL
                        // Ignore special index.
                        && !(addIndex instanceof SqlAddForeignKey)
                        && !(addIndex instanceof SqlAddFullTextIndex)
                        && !(addIndex instanceof SqlAddSpatialIndex)
                        // Unexpected and unrecognized type.
                        && !(addIndex.getIndexDef().getType() != null
                        && addIndex.getIndexDef().getType().equalsIgnoreCase("fulltext"))
                        && !(addIndex.getIndexDef().getType() != null
                        && addIndex.getIndexDef().getType().equalsIgnoreCase("spatial"));

                if (plannerContext.isExplain() || (!query.createGsi() && !convertToGSI)) {
                    continue;
                }

                if (query.getAlters().size() != 1) {
                    throw new NotSupportException("Multi alter specifications when create GSI");
                }
                check = true;
            } else if (alterSpecification instanceof SqlAlterTableDropIndex) {
                final SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterSpecification;

                if (null == dropIndex.getIndexName() || dropIndex.getIndexName().getLastName().isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(""));
                }

                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    // Add GSI suffix if drop GSI.
                    final String indexName = dropIndex.getIndexName().getLastName();
                    final String wrapped =
                        null == tableMeta.getGsiTableMetaBean() || null == tableMeta.getGsiTableMetaBean().indexMap ?
                            null : tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                            .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(indexName))
                            .findFirst().orElse(null);
                    if (wrapped != null) {
                        // Drop GSI.
                        if (query.getAlters().size() != 1) {
                            throw new NotSupportException("Multi alter specifications when drop GSI");
                        }
                        check = true;
                    }
                }
            }
        }

        if (check) {
            if (query.getAlters().get(0) instanceof SqlAddIndex) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    // Collect existing names.
                    final Set<String> existsNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    tableMeta.getSecondaryIndexes().forEach(meta -> existsNames.add(meta.getPhysicalIndexName()));
                    if (tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().indexMap != null) {
                        tableMeta.getGsiTableMetaBean().indexMap.forEach((k, v) -> existsNames.add(unwrapGsiName(k)));
                    }

                    final SqlAddIndex addIndex = (SqlAddIndex) query.getAlters().get(0);

                    // Assign name if no name spec.
                    final String orgName;
                    if (null == addIndex.getIndexName() || null == addIndex.getIndexName().getLastName()) {
                        final String baseName = "i_";
                        int prob = 0;
                        while (existsNames.contains(baseName + prob)) {
                            ++prob;
                        }
                        orgName = baseName + prob;
                    } else {
                        orgName = addIndex.getIndexName().getLastName();
                    }

                    // Now assign gsi name and do auto partition rewrite.
                    final String newName = assignGsiName(query.getName(), existsNames, orgName);
                    final SqlIdentifier newNameIdentifier = new SqlIdentifier(newName, SqlParserPos.ZERO);

                    if (tableMeta.isAutoPartition()) {
                        // Assign name and partition.
                        if (addIndex.getIndexDef().getDbPartitionBy() != null
                            || addIndex.getIndexDef().getDbPartitions() != null ||
                            addIndex.getIndexDef().getTbPartitionBy() != null
                            || addIndex.getIndexDef().getTbPartitions() != null) {
                            throw new NotSupportException(
                                "Incorrect index definition; New partition table should contain no legacy partition define.");
                        }

                        if (addIndex.getIndexDef().getColumns().isEmpty()) {
                            throw new NotSupportException(
                                "Incorrect index definition; No index column.");
                        }

                        final List<String> indexColNames = addIndex.getIndexDef().getColumns().stream()
                            .map(SqlIndexColumnName::getColumnNameStr).collect(Collectors.toList());
                        final SqlNode newPartition =
                            null == addIndex.getIndexDef().getPartitioning() ?
                                generateNewPartition(tableMeta, indexColNames, addIndex instanceof SqlAddUniqueIndex) :
                                null;
                        if (null == newPartition && null == addIndex.getIndexDef().getPartitioning()) {
                            return query; // No extra dealing needed.
                        } else {
                            final SqlIndexDefinition newIndexDefinition = addIndex.getIndexDef()
                                .rebuildToGsiNewPartition(newNameIdentifier, newPartition,
                                    addIndex.getIndexDef().isClustered());
                            final SqlAddIndex newAddIndex;
                            if (addIndex instanceof SqlAddUniqueIndex) {
                                newAddIndex = new SqlAddUniqueIndex(
                                    SqlParserPos.ZERO, newIndexDefinition.getIndexName(), newIndexDefinition);
                            } else {
                                newAddIndex = new SqlAddIndex(
                                    SqlParserPos.ZERO, newIndexDefinition.getIndexName(), newIndexDefinition);
                            }
                            assert 1 == query.getAlters().size();
                            query.getAlters().clear();
                            query.getAlters().add(newAddIndex);
                        }
                    } else {
                        // Assign name only.
                        assert addIndex.getIndexDef().isGlobal() || addIndex.getIndexDef()
                            .isClustered(); // Not convert to GSI, so it is GSI.
                        final SqlIndexDefinition newIndexDefinition = addIndex.getIndexDef()
                            .rebuildToGsiNewPartition(newNameIdentifier, null, addIndex.getIndexDef().isClustered());
                        final SqlAddIndex newAddIndex;
                        if (addIndex instanceof SqlAddUniqueIndex) {
                            newAddIndex =
                                new SqlAddUniqueIndex(SqlParserPos.ZERO, newNameIdentifier, newIndexDefinition);
                        } else {
                            newAddIndex = new SqlAddIndex(SqlParserPos.ZERO, newNameIdentifier, newIndexDefinition);
                        }
                        assert 1 == query.getAlters().size();
                        query.getAlters().clear();
                        query.getAlters().add(newAddIndex);
                    }
                }

                final SqlAddIndex addIndex = (SqlAddIndex) query.getAlters().get(0);

                final Set<String> gsiNames = new HashSet<>();
                validator.validateGsiName(gsiNames, addIndex.getIndexName());

                final String indexName = addIndex.getIndexName().getLastName();
                if (!plannerContext.getExecutionContext().getSchemaManager(schemaName)
                    .getGsi(indexName, IndexStatus.ALL).isEmpty()) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(indexName));
                }

                final String tableName = ((SqlIdentifier) query.getName()).getLastName();
                if (gsiNames.contains(tableName)) {
                    throw validator.newValidationError(query.getName(), RESOURCE.gsiExists(tableName));
                }
            } else if (query.getAlters().get(0) instanceof SqlAlterTableDropIndex) {
                final SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) query.getAlters().get(0);

                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    // Add GSI suffix if drop GSI.
                    final String indexName = dropIndex.getIndexName().getLastName();
                    final String wrapped = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                        .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(indexName))
                        .findFirst().orElse(null);
                    if (wrapped != null) {
                        final SqlAlterTableDropIndex newDropIndex =
                            new SqlAlterTableDropIndex((SqlIdentifier) dropIndex.getTableName(),
                                new SqlIdentifier(wrapped, SqlParserPos.ZERO), dropIndex.getSourceSql(),
                                SqlParserPos.ZERO);
                        assert 1 == query.getAlters().size();
                        query.getAlters().clear();
                        query.getAlters().add(newDropIndex);
                    }
                }
            }
        }

        return query;
    }

    @Override
    protected SqlShow checkAndRewriteShow(SqlShow show) {
        if (show instanceof SqlShowCreateTable) {
            final SqlShowCreateTable createTable = (SqlShowCreateTable) show;

            final String tableName;
            if (createTable.getTableName() instanceof SqlIdentifier) {
                final SqlIdentifier identifier = (SqlIdentifier) createTable.getTableName();
                tableName = identifier.getLastName();

                final String schemaName =
                    2 == identifier.names.size() ? identifier.names.get(0) : plannerContext.getSchemaName();

                final SchemaManager schemaManager = plannerContext.getExecutionContext().getSchemaManager(schemaName);
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName) && !schemaManager
                    .getTddlRuleManager().getPartitionInfoManager().isNewPartDbTable(tableName)) {
                    // Table not found. Try GSI name match.
                    final Set<String> gsi = schemaManager.guessGsi(tableName);
                    if (1 == gsi.size()) {
                        // Do smart replace.
                        show = SqlShowCreateTable
                            .create(SqlParserPos.ZERO, new SqlIdentifier(gsi.iterator().next(), SqlParserPos.ZERO),
                                ((SqlShowCreateTable) show).isFull());
                    } else if (gsi.size() >= 2) {
                        throw new SqlValidateException(
                            "Table '" + tableName + "' not found and multiple GSI table found.");
                    }
                }
            }
        }
        return show;
    }

    /**
     * Rewrite source select, add SET item for column with attribute ON UPDATE
     * CURRENT_TIMESTAMP. This method will update select list of
     * {@link SqlUpdate#sourceSelect}, no matter SET part modified or not. This
     * method will NOT update the {@link SqlUpdate#targetColumnList} and
     * {@link SqlUpdate#sourceExpressionList}, no matter SET part modified or not.
     *
     * @param outExtraTargetTableIndexes target tables for added SET item
     * @param outExtraTargetColumns target columns for added SET item
     * @return new source select
     */
    @Override
    protected SqlSelect rewriteUpdateSourceSelect(SqlUpdate update, List<Integer> targetTableIndexes,
                                                  List<String> targetColumns, List<TableModify.TableInfoNode> srcTables,
                                                  List<Integer> outExtraTargetTableIndexes,
                                                  List<String> outExtraTargetColumns) {
        final List<RelOptTable> targetTables =
            targetTableIndexes.stream().map(i -> srcTables.get(i).getRefTable()).collect(Collectors.toList());
        final boolean modifyBroadcast = CheckModifyLimitation.checkModifyBroadcast(targetTables, doNothing());
        final boolean modifyGsi = CheckModifyLimitation
            .checkModifyGsi(targetTables, targetColumns, false, this.plannerContext.getExecutionContext());
        final boolean scaleOutIsRunning =
            ComplexTaskPlanUtils.isScaleOutRunningOnTables(targetTables, this.plannerContext.getExecutionContext());
        final boolean modifyPartitionKey =
            CheckModifyLimitation.checkModifyShardingColumnWithGsi(targetTables, targetColumns,
                this.plannerContext.getExecutionContext());
        final boolean gsiHasAutoUpdateColumns =
            CheckModifyLimitation.checkGsiHasAutoUpdateColumns(srcTables, this.plannerContext.getExecutionContext());

        final Set<Integer> targetTableIndexSet = new TreeSet<>(targetTableIndexes);

        final SqlSelect sourceSelect = update.getSourceSelect();
        final List<SqlNode> selectList = sourceSelect.getSelectList().getList();
        final AtomicInteger ordinal = new AtomicInteger(selectList.size() - 1);



        if (!modifyPartitionKey && !modifyBroadcast && !modifyGsi && !scaleOutIsRunning && !gsiHasAutoUpdateColumns
            ) {
            sourceSelect.setSelectList(new SqlNodeList(selectList, SqlParserPos.ZERO));
            return update.getSourceSelect();
        }

        for (Integer tableIndex : targetTableIndexSet) {
            final RelOptTable table = srcTables.get(tableIndex).getRefTable();
            final Pair<String, String> qn = RelUtils.getQualifiedTableName(table);
            final TableMeta tableMeta =
                plannerContext.getExecutionContext().getSchemaManager(qn.left).getTable(qn.right);
            final TableColumnMeta tableColumnMeta = tableMeta.getTableColumnMeta();
            final Pair<String, String> columnMapping =
                TableColumnUtils.getColumnMultiWriteMapping(tableColumnMeta, plannerContext.getExecutionContext());

            final TreeSet<String> targetColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < targetColumns.size(); i++) {
                if (targetTableIndexes.get(i).equals(tableIndex)) {
                    targetColumnSet.add(targetColumns.get(i));
                }
            }

            tableMeta.getAllColumns()
                .stream()
                .filter(c -> !targetColumnSet.contains(c.getName()))
                .forEach(columnMeta -> {
                    final Field field = columnMeta.getField();

                    // Add SET for column ON UPDATE CURRENT_TIMESTAMP
                    if (TStringUtil.containsIgnoreCase(field.getExtra(), "on update") &&
                        !(columnMapping != null && columnMeta.getName().equalsIgnoreCase(columnMapping.right))) {
                        if (DataTypeUtil
                            .anyMatchSemantically(field.getDataType(), DataTypes.TimestampType,
                                DataTypes.DatetimeType)) {
                            outExtraTargetTableIndexes.add(tableIndex);
                            outExtraTargetColumns.add(columnMeta.getName());
                            selectList
                                .add(SqlValidatorUtil.addAlias(new SqlBasicCall(SqlStdOperatorTable.CURRENT_TIMESTAMP,
                                    SqlNode.EMPTY_ARRAY,
                                    SqlParserPos.ZERO), SqlUtil.deriveAliasFromOrdinal(ordinal.getAndIncrement())));
                        }
                    }
                });
        }

        // Update select list
        sourceSelect.setSelectList(new SqlNodeList(selectList, SqlParserPos.ZERO));

        return sourceSelect;
    }

    @Override
    protected RelNode transformUpdateSourceRel(RelNode old, TableInfo tableInfo, List<String> targetColumns,
                                               List<Map<String, Integer>> sourceColumnIndexMap,
                                               List<String> outTargetColumns, List<Integer> outTargetTables) {
        final List<RelOptTable> targetTables = tableInfo.getTargetTables();
        final List<Integer> targetTableIndexes = tableInfo.getTargetTableIndexes();
        final boolean modifyBroadcast = CheckModifyLimitation.checkModifyBroadcast(targetTables, doNothing());
        final boolean modifyGsi = CheckModifyLimitation
            .checkModifyGsi(targetTables, targetColumns, false, this.plannerContext.getExecutionContext());
        final boolean scaleOutIsRunning =
            ComplexTaskPlanUtils.isScaleOutRunningOnTables(targetTables, this.plannerContext.getExecutionContext());
        final boolean modifyPartitionKey =
            CheckModifyLimitation.checkModifyShardingColumnWithGsi(targetTables, targetColumns,
                this.plannerContext.getExecutionContext());
        final boolean modifyColumn = CheckModifyLimitation.checkOnlineModifyColumnDdl(targetTables,
            this.plannerContext.getExecutionContext());

        final ExecutionStrategy hintEx = getExecutionStrategy();
        if (!modifyPartitionKey && !modifyBroadcast && !modifyGsi && !scaleOutIsRunning && !modifyColumn
            && hintEx != ExecutionStrategy.LOGICAL) {
            outTargetColumns.addAll(targetColumns);
            outTargetTables.addAll(targetTableIndexes);
            return old;
        }

        final Project oldProject = getProject(old);

        if (null == oldProject) {
            outTargetColumns.addAll(targetColumns);
            outTargetTables.addAll(targetTableIndexes);
            return old;
        }
        final RelNode base = oldProject.getInput();

        final List<String> oldFieldNames = oldProject.getRowType().getFieldNames();
        final List<RexNode> oldProjects = oldProject.getProjects();
        final int oldFieldCount = oldFieldNames.size();
        final int setSrcOffset = oldFieldCount - targetColumns.size();

        final List<RexNode> baseProjects = oldProjects.subList(0, setSrcOffset);

        final Map<Integer, Map<String, Integer>> tableColumnIndexMap = new HashMap<>();

        final List<Integer> modifiedColumns = new ArrayList<>();
        final TargetMapping modifiedColumnMapping = Mappings.target(
            IntStream.range(0, oldFieldNames.size()).boxed().collect(Collectors.toMap(i -> i, i -> i, (o, n) -> o)),
            oldFieldCount,
            oldFieldCount);
        final Deque<RelNode> bases = new ArrayDeque<>(ImmutableList.of(base));
        final List<RexNode> currentProjectItems = new ArrayList<>(baseProjects);
        final List<String> currentFieldNames = new ArrayList<>(oldFieldNames.subList(0, setSrcOffset));
        Ord.zip(targetColumns).forEach(o -> {
            final Integer targetTableIndex = targetTableIndexes.get(o.i);
            final Map<String, Integer> columnIndexMap =
                tableColumnIndexMap.computeIfAbsent(targetTableIndex, sourceColumnIndexMap::get);

            final int currentSrcIndex = setSrcOffset + o.i;
            RexNode projectItem = oldProjects.get(currentSrcIndex);

            final ImmutableBitSet inputBitSet = InputFinder.bits(projectItem);
            if (inputBitSet.intersects(ImmutableBitSet.of(modifiedColumns))) {
                final RelNode bottom = bases.peek();

                final Project topProject = buildProject(currentProjectItems, currentFieldNames, bottom);

                bases.push(topProject);

                currentProjectItems.clear();
                IntStream.range(0, currentFieldNames.size())
                    .mapToObj(j -> rexBuilder.makeInputRef(topProject, j))
                    .forEach(currentProjectItems::add);

                projectItem = projectItem.accept(RexPermuteInputsShuttle.of(modifiedColumnMapping));
            }

            currentFieldNames.add(oldFieldNames.get(currentSrcIndex));
            currentProjectItems.add(projectItem);

            final Integer modifiedColumnIndex = columnIndexMap.get(o.e);
            modifiedColumns.add(modifiedColumnIndex);
            modifiedColumnMapping.set(modifiedColumnIndex, currentSrcIndex);
        });

        if (bases.size() == 1) {
            outTargetColumns.addAll(targetColumns);
            outTargetTables.addAll(targetTableIndexes);
            return old;
        }

        RelNode result = buildProject(currentProjectItems, currentFieldNames, bases.peek());

        // Remove redundant SET item
        final Map<Pair<Integer, String>, Integer> distinctTargetColumnIndexMap = new HashMap<>();
        final List<RexNode> targetProjects = ((Project) result).getProjects();
        final List<String> targetFieldNames = result.getRowType().getFieldNames();
        final List<RexNode> resultProjects = new ArrayList<>(targetProjects.subList(0, setSrcOffset));
        final List<String> resultFieldNames = new ArrayList<>(targetFieldNames.subList(0, setSrcOffset));
        Ord.zip(targetColumns).forEach(o -> {
            final Integer targetTableIndex = targetTableIndexes.get(o.i);
            final Pair<Integer, String> key = Pair.of(targetTableIndex, o.e);

            if (distinctTargetColumnIndexMap.containsKey(key)) {
                final Integer index = distinctTargetColumnIndexMap.get(key);
                resultFieldNames.set(index + setSrcOffset, targetFieldNames.get(o.i + setSrcOffset));
                resultProjects.set(index + setSrcOffset, targetProjects.get(o.i + setSrcOffset));
                outTargetTables.set(index, key.left);
                outTargetColumns.set(index, key.right);
            } else {
                resultFieldNames.add(targetFieldNames.get(o.i + setSrcOffset));
                resultProjects.add(targetProjects.get(o.i + setSrcOffset));
                outTargetTables.add(key.left);
                outTargetColumns.add(key.right);

                distinctTargetColumnIndexMap.put(key, outTargetTables.size() - 1);
            }
        });

        if (resultProjects.size() < targetProjects.size()) {
            result = LogicalProject.create(result.getInput(0), resultProjects, resultFieldNames);
        }

        if (old instanceof Sort) {
            result = ((Sort) old).copy(old.getTraitSet(), ImmutableList.of(result));
        }

        return result;
    }

    private Project buildProject(List<RexNode> currentProjectItems, List<String> currentFieldNames, RelNode bottom) {
        Project topProject;
        if (bottom instanceof Project) {
            topProject = mergeProject(currentProjectItems, currentFieldNames, (Project) bottom);
        } else {
            assert bottom != null;
            topProject = LogicalProject.create(bottom, currentProjectItems, currentFieldNames);
        }
        return topProject;
    }

    private Project mergeProject(List<RexNode> currentProjectItems, List<String> currentFieldNames, Project bottom) {
        Project topProject = LogicalProject.create(bottom, currentProjectItems, currentFieldNames);

        final List<RexNode> topProjects = PlannerUtils.mergeProject(topProject, bottom, relBuilder);
        topProject = LogicalProject.create(bottom, topProjects, currentFieldNames);
        return topProject;
    }

    private static Project getProject(RelNode rel) {
        Project oldProject = null;
        if (rel instanceof Project) {
            oldProject = (Project) rel;
        } else if (rel instanceof Sort) {
            oldProject = (Project) ((Sort) rel).getInput();
        }
        return oldProject;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public SqlToRelConverter setPlannerContext(PlannerContext plannerContext) {
        this.plannerContext = plannerContext;
        return this;
    }

    private String getAliasName(TableMeta tableMeta, SqlNodeList aliases) {
        if (aliases == null) {
            return null;
        }
        String tableName = tableMeta.getTableName();
        String fullTableName = tableMeta.getSchemaName() + "." + tableName;
        for (int i = 0; i < aliases.size(); i++) {
            final SqlNode aliasNode = aliases.get(i);
            switch (aliasNode.getKind()) {
            case AS:
                if (((SqlCall) aliasNode).operand(0).toString().equalsIgnoreCase(tableName) || ((SqlCall) aliasNode)
                    .operand(0).toString().equalsIgnoreCase(fullTableName)) {
                    return ((SqlCall) aliasNode).operand(1).toString();
                }
            }
        }
        return null;
    }

    private RexNode convertDefaultValue(RexNode node, RelOptTable targetTable, int i, TableMeta tableMeta,
                                        ExecutionContext ec) {
        final RelDataTypeField relDataTypeField = targetTable.getRowType().getFieldList().get(i);
        final String columnName = relDataTypeField.getName();
        final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);

        final Field field = tableMeta.getColumn(columnName).getField();
        final DataType columnDataType = field.getDataType();
        final String columnDefaultStr = field.getDefault();

        if (DataTypeUtil.isTimezoneDependentType(columnDataType)) {
            // If it's not current_timestamp(RexCall), convert default value from timezone in metadb(+08:00) to current
            // timezone
            if (node instanceof RexLiteral && TStringUtil.isNotBlank(columnDefaultStr)) {
                final String timeInCurrentTimezone =
                    TimestampUtils.convertFromGMT8(columnDefaultStr, ec.getTimeZone().getTimeZone(), field.getScale());
                final NlsString valueStr = new NlsString(timeInCurrentTimezone, null, null);
                node = rexBuilder.makeCharLiteral(valueStr);
            }
        } else if (columnMeta.isBinaryDefault()) {
            // Convert from hex string for binary default value
            if (TStringUtil.isNotBlank(columnDefaultStr)) {
                node = rexBuilder.makeBinaryLiteral(ByteString.of(columnDefaultStr, 16));
            }
        }

        return node;
    }

    @Override
    public RelOptTable.ToRelContext createToRelContext() {
        return (DrdsViewExpander) viewExpander;
    }

    //columnNames 必须是分区键，且表必须是分表
    @Override
    protected boolean supportInValuesConvertJoin(
        List<String> columnNames, SqlNodeList valueList, Blackboard bb) {
        boolean bMatchAllDynamic = valueList.getList().stream().allMatch(t -> t instanceof SqlDynamicParam);
        boolean bMatchAllLiteral = valueList.getList().stream().allMatch(node ->
            (node instanceof SqlLiteral) && ((SqlLiteral) node).getValue() != null
        );
        if ((bMatchAllLiteral || bMatchAllDynamic) &&
            valueList.size() >= config.getInSubQueryThreshold() && !columnNames.isEmpty()) {

            if (bb.root instanceof LogicalTableScan) {
                LogicalTableScan targetTable = (LogicalTableScan) bb.root;
                final List<String> qualifiedName = targetTable.getTable().getQualifiedName();
                final String tableName = Util.last(qualifiedName);
                final String schema = qualifiedName.get(qualifiedName.size() - 2);
                TableMeta tableMeta =
                    OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(tableName);
                if (tableMeta != null && tableMeta.getEngine() == Engine.OSS) {
                    return false;
                }
                final TddlRuleManager rule = OptimizerContext.getContext(schema).getRuleManager();
                if (rule != null) {
                    List<String> shardColumns = rule.getSharedColumns(tableName);
                    if (rule != null && !shardColumns.isEmpty() && columnNames.size() == shardColumns.size()) {
                        boolean ret = true;
                        for (int index = 0; index < columnNames.size(); index++) {
                            if (!shardColumns.get(index).equalsIgnoreCase(columnNames.get(index))) {
                                ret = false;
                                break;
                            }
                        }
                        final boolean shard = rule.isShard(tableName);
                        return ret && shard;
                    }
                }
            }
        }
        return false;
    }

    @Override
    protected List<Map<String, Integer>> getColumnIndexMap(Blackboard bb, RelNode sourceRel,
                                                           List<Pair<SqlNode, List<SqlNode>>> tableColumns) {
        final SqlNameMatcher sqlNameMatcher = this.catalogReader.nameMatcher();

        final List<Set<RelColumnOrigin>> columnOriginNames =
            sourceRel.getCluster().getMetadataQuery().getDmlColumnNames(sourceRel);

        final List<Map<String, Integer>> targetColumnIndexMap = new ArrayList<>();
        for (Pair<SqlNode, List<SqlNode>> pair : tableColumns) {
            final List<SqlNode> targetColumns = pair.getValue();

            final Map<String, Integer> columnIndexMap =
                sqlNameMatcher.isCaseSensitive() ? new HashMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (SqlNode targetColumn : targetColumns) {
                final RexInputRef ref = (RexInputRef) bb.convertExpression(targetColumn);
                final String columnName = columnOriginNames.get(ref.getIndex()).iterator().next().getColumnName();
                columnIndexMap.put(columnName, ref.getIndex());
            }
            targetColumnIndexMap.add(columnIndexMap);
        }
        return targetColumnIndexMap;
    }

    @Override
    protected void interceptDMLAllTableSql(SqlNode sqlNode) {
        final PlannerContext plannerContext = PlannerContext.getPlannerContext(this.cluster);
        final ExecutionContext executionContext = plannerContext.getExecutionContext();
        boolean forbidDmlAll = executionContext.getParamManager().getBoolean(ConnectionParams.FORBID_EXECUTE_DML_ALL);
        if (forbidDmlAll) {
            if (sqlNode.getKind() == SqlKind.DELETE || sqlNode.getKind() == SqlKind.UPDATE) {
                RelUtils.forbidDMLAllTableSql(sqlNode);
            }
        }
    }
}

