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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder.PhyTableShardResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.rel.rel2sql.TddlRelToSqlConverter;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.changeParameterContextIndex;

/**
 * @author minggong.zm 2018-01-17
 */
public class LogicalInsert extends TableModify {

    private final DbType dbType;
    // designated insert rows
    private RelDataType insertRowType = null;
    private SqlNode sqlTemplate;
    @Getter
    private Map<String, List<List<String>>> targetTablesHintCache;
    private String schemaName = null;
    @Getter
    @Setter
    private LogicalDynamicValues unOptimizedLogicalDynamicValues = null;
    @Setter
    @Getter
    private List<RexNode> unOptimizedDuplicateKeyUpdateList;

    /**
     * Which columns should be replaced with literals
     */
    private List<Integer> literalColumnIndex = null;
    /**
     * Of which columns insert value should be deterministic (eg. columns in gsi)
     */
    private List<Integer> deterministicColumnIndex = null;
    /**
     * Index of sequence column in field list. null means not initialized, -1 means no sequence column.
     */
    private Integer seqColumnIndex = null;
    /**
     * Index of columns with property AUTO_INCREMENT
     */
    @Setter
    @Getter
    private List<Integer> autoIncParamIndex = new ArrayList<>();
    /**
     * Index of columns with NOT NULL and dynamic implicit default (.e.g CURRENT_TIMESTAMP for timestamp type)
     */
    private volatile Map<Integer, RexNode> dynamicImplicitDefaultColumnIndexMap = null;
    /**
     * Index of columns with NOT NULL and static or dynamic implicit default (.e.g 0 for number type, '' for string type)
     */
    private volatile Map<Integer, RexNode> implicitDefaultColumnIndexMap = null;

    /**
     * RexCallParam for dynamic implicit default column
     */
    @Getter
    private final List<RexCallParam> dynamicImplicitDefaultParams;

    /**
     * RexCallParam for dynamic implicit default column, before rebuild LogicalInsert for multi values
     */
    @Getter
    @Setter
    private List<RexCallParam> unoptimizedDynamicImplicitDefaultParams = null;

    @Setter
    @Getter
    protected InsertWriter primaryInsertWriter;
    protected DistinctWriter primaryDeleteWriter;
    @Setter
    @Getter
    protected List<InsertWriter> gsiInsertWriters = new ArrayList<>();
    protected List<InsertWriter> gsiInsertIgnoreWriters = new ArrayList<>();
    protected List<DistinctWriter> gsiDeleteWriters = new ArrayList<>();

    /**
     * Special Writer for executing with PUSHDOWN/DETERMINISTIC_PUSHDOWN policy
     * when the target Groups of current LogicalInsert has NOT any ScaleOut Group
     * and target table has NOT any GSI/GUK
     */
    @Setter
    @Getter
    protected InsertWriter pushDownInsertWriter;

    /**
     * The part pruning info for logical insert
     */
    protected volatile boolean initTupleRoutingInfo = false;
    protected volatile boolean initReplicationTupleRoutingInfo = false;
    protected PartitionTupleRouteInfo tupleRoutingInfo;
    protected PartitionTupleRouteInfo replicationTupleRoutingInfo;

    /**
     * 当Insert Select语句时，选择执行模式：
     */
    public enum InsertSelectMode {
        // 单线程执行Insert
        SINGLE,
        // 多线程执行Insert
        MULTI,
        // 使用MPP调度别的节点执行Insert
        MPP,
    }

    //默认单线程执行
    protected InsertSelectMode insertSelectMode = InsertSelectMode.SINGLE;

    protected boolean pushablePrimaryKeyCheck = false;
    protected boolean pushableForeignConstraintCheck = true;

    // Following variables used by generated columns
    // evalRow is built to eval generated columns
    protected List<ColumnMeta> evalRowColMetas = null;
    protected List<RexNode> genColRexNodes = null;
    protected List<Integer> inputToEvalFieldsMapping = null;

    // modify foreign key in replace or upsert
    protected boolean modifyForeignKey = false;

    protected List<ColumnMeta> defaultExprColMetas = null;
    protected List<RexNode> defaultExprColRexNodes = null;
    protected List<Integer> defaultExprEvalFieldsMapping = null;

    protected boolean ukContainsAllSkAndGsiContainsAllUk = false;

    public LogicalInsert(TableModify modify) {
        this(modify.getCluster(),
            modify.getTraitSet(),
            modify.getTable(),
            modify.getCatalogReader(),
            modify.getInput(),
            modify.getOperation(),
            modify.isFlattened(),
            null,
            modify.getKeywords(),
            modify.getDuplicateKeyUpdateList(),
            modify.getBatchSize(),
            modify.getAppendedColumnIndex(),
            modify.getHints(),
            modify.getTableInfo(),
            ImmutableList.of());
    }

    public LogicalInsert(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation, boolean flattened,
                         RelDataType insertRowType, List<String> keywords, List<RexNode> duplicateKeyUpdateList,
                         int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
                         List<RexCallParam> dynamicImplicitDefaultParams) {
        super(cluster,
            traitSet,
            table,
            catalogReader,
            input,
            operation,
            null,
            null,
            flattened,
            keywords,
            batchSize,
            appendedColumnIndex,
            hints,
            tableInfo);
        this.duplicateKeyUpdateList = duplicateKeyUpdateList;
        this.dynamicImplicitDefaultParams = dynamicImplicitDefaultParams;
        this.dbType = DbType.MYSQL;
        this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) :
            PlannerContext.getPlannerContext(cluster).getSchemaName();
        this.insertRowType = insertRowType;
    }

    public LogicalInsert(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                         Prepare.CatalogReader catalogReader, RelNode input, Operation operation, boolean flattened,
                         RelDataType insertRowType, List<String> keywords, List<RexNode> duplicateKeyUpdateList,
                         int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
                         InsertWriter primaryInsertWriter, List<InsertWriter> gsiInsertWriters,
                         List<Integer> autoIncParamIndex, LogicalDynamicValues unOptimizedLogicalDynamicValues,
                         List<RexNode> unOptimizedDuplicateKeyUpdateList, List<ColumnMeta> evalRowColMetas,
                         List<RexNode> genColRexNodes, List<Integer> inputToEvalFieldsMapping,
                         List<ColumnMeta> defaultExprColMetas, List<RexNode> defaultExprColRexNodes,
                         List<Integer> defaultExprEvalFieldsMapping, boolean pushablePrimaryKeyCheck,
                         boolean pushableForeignConstraintCheck, boolean modifyForeignKey,
                         boolean ukContainsAllSkAndGsiContainsAllUk, List<RexCallParam> dynamicImplicitDefaultParams,
                         List<RexCallParam> unoptimizedDynamicImplicitDefaultParams) {
        super(cluster, traitSet, table, catalogReader, input, operation, null, null, flattened, keywords, batchSize,
            appendedColumnIndex, hints, tableInfo);
        this.duplicateKeyUpdateList = duplicateKeyUpdateList;
        this.dynamicImplicitDefaultParams = dynamicImplicitDefaultParams;
        this.unoptimizedDynamicImplicitDefaultParams = unoptimizedDynamicImplicitDefaultParams;
        this.dbType = DbType.MYSQL;
        this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) : null;
        this.insertRowType = insertRowType;
        this.autoIncParamIndex = autoIncParamIndex;
        this.primaryInsertWriter = primaryInsertWriter;
        this.gsiInsertWriters = gsiInsertWriters;
        this.unOptimizedLogicalDynamicValues = unOptimizedLogicalDynamicValues;
        this.unOptimizedDuplicateKeyUpdateList = unOptimizedDuplicateKeyUpdateList;
        this.evalRowColMetas = evalRowColMetas;
        this.genColRexNodes = genColRexNodes;
        this.inputToEvalFieldsMapping = inputToEvalFieldsMapping;
        this.defaultExprColMetas = defaultExprColMetas;
        this.defaultExprEvalFieldsMapping = defaultExprEvalFieldsMapping;
        this.defaultExprColRexNodes = defaultExprColRexNodes;
        this.pushablePrimaryKeyCheck = pushablePrimaryKeyCheck;
        this.pushableForeignConstraintCheck = pushableForeignConstraintCheck;
        this.modifyForeignKey = modifyForeignKey;
        this.ukContainsAllSkAndGsiContainsAllUk = ukContainsAllSkAndGsiContainsAllUk;
    }

    /**
     * for json Deserialization
     */
    public LogicalInsert(RelInput relInput) {
        super(relInput);
        this.dbType = DbType.MYSQL;
        schemaName = relInput.getString("schemaName");
        insertRowType = relInput.getRowType("insertRowType");
        duplicateKeyUpdateList = relInput.getExpressionList("duplicateKeyUpdateList");
        dynamicImplicitDefaultParams = Optional.ofNullable(relInput.getExpressionList("dynamicImplicitDefaultParams"))
            .map(list -> list.stream()
                .map(item -> (RexCallParam) item)
                .collect(Collectors.toList()))
            .orElse(new ArrayList<>());
//        dynamicImplicitDefaultParams = new ArrayList<>();
    }

    public RelDataType getInsertRowType() {
        return insertRowType;
    }

    public void setInsertRowType(RelDataType insertRowType) {
        this.insertRowType = insertRowType;
    }

    /**
     * update TargetTables cache, for HINT ONLY!
     */
    public void setTargetTables(Map<String, List<List<String>>> targetTables) {
        this.targetTablesHintCache = targetTables;
    }

    public boolean isSourceSelect() {
        RelNode input = this.input;
        if (input == null) {
            return false;
        }

        if (input instanceof HepRelVertex) {
            input = ((HepRelVertex) input).getCurrentRel();
        }

        if (input instanceof LogicalValues || input instanceof LogicalDynamicValues) {
            return false;
        }

        return true;
    }

    public boolean hasHint() {
        return targetTablesHintCache != null && !targetTablesHintCache.isEmpty();
    }

    /**
     * 构建并获取其下层的 PhyTableModify 节点
     * <p>
     * <pre>
     *     计算分片
     *     构建对应的 PhyTableModify
     * </pre>
     */
    public List<RelNode> getInput(PhyTableInsertSharder insertSharder, List<PhyTableShardResult> shardResult,
                                  ExecutionContext executionContext) {
        String schema = schemaName == null ? executionContext.getSchemaName() : schemaName;
        if (targetTablesHintCache != null) {

            // Change data structure. Fill valueIndices with null.
            for (Map.Entry<String, List<List<String>>> entry : targetTablesHintCache.entrySet()) {
                String groupIndex = entry.getKey();
                for (List<String> tableNames : entry.getValue()) {
                    PhyTableShardResult result = new PhyTableShardResult(groupIndex, tableNames.get(0), null);
                    shardResult.add(result);
                }
            }

            PhyTableInsertBuilder phyTableInsertbuilder = new PhyTableInsertBuilder(getSqlTemplate(),
                executionContext,
                this,
                dbType,
                schema);
            return phyTableInsertbuilder.build(shardResult);

        } else {
            shardResult.addAll(insertSharder.shard(executionContext));

            PhyTableInsertBuilder phyTableInsertbuilder = new PhyTableInsertBuilder(insertSharder.getSqlTemplate(),
                executionContext,
                this,
                dbType,
                schema);
            return phyTableInsertbuilder.build(shardResult);
        }
    }

    public LogicalInsert buildInsertWithValues() {
        final LogicalDynamicValues dynamicValues = buildValues();

        return (LogicalInsert) copy(getTraitSet(), ImmutableList.of(dynamicValues));
    }

    protected LogicalDynamicValues buildValues() {
        final List<RelDataTypeField> fields = getInsertRowType().getFieldList();

        // Build LogicalDynamicValues
        final List<RexNode> tuple =
            IntStream.range(0, fields.size()).mapToObj(i -> new RexDynamicParam(fields.get(i).getType(), i))
                .collect(Collectors.toList());

        final ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.copyOf(tuple));

        return LogicalDynamicValues.createDrdsValues(getCluster(), getTraitSet(), getInsertRowType(), tuples);
    }

    public LogicalInsert updateDuplicateKeyUpdateList(AtomicInteger nextParamIndex,
                                                      Map<Integer, Integer> outParamMapping) {
        if (!withDuplicateKeyUpdate()) {
            return this;
        }

        final List<RexCallParam> newDynamicImplicitDefaultParams = new ArrayList<>();
        final List<RexNode> newDuplicatedUpdateList =
            buildDuplicateKeyUpdateList(this, nextParamIndex, newDynamicImplicitDefaultParams, outParamMapping);

        if (GeneralUtil.isNotEmpty(newDuplicatedUpdateList)) {
            final LogicalInsert copied = newDynamicImplicitDefaultParams.isEmpty() ?
                (LogicalInsert) copy(getTraitSet(), getInputs()) :
                (LogicalInsert) copy(getTraitSet(), getInputs(), newDynamicImplicitDefaultParams);
            copied.setDuplicateKeyUpdateList(newDuplicatedUpdateList);
            return copied;
        }

        return this;
    }

    public static List<RexNode> buildDuplicateKeyUpdateList(LogicalInsert insert, AtomicInteger nextParamIndex,
                                                            List<RexCallParam> newDynamicImplicitDefaultParams,
                                                            Map<Integer, Integer> outParamMapping) {
        if (!insert.withDuplicateKeyUpdate()) {
            return null;
        }

        final List<RexNode> onDuplicatedUpdate = insert.getDuplicateKeyUpdateList();
        final List<RexCallParam> oldDynamicImplicitDefaultParams = insert.getDynamicImplicitDefaultParams();

        final RexUtils.FindMinDynamicParam findMinDynamicParam = new RexUtils.FindMinDynamicParam();
        onDuplicatedUpdate.forEach(o -> ((RexCall) o).getOperands().get(1).accept(findMinDynamicParam));

        int minPara = findMinDynamicParam.getMinDynamicParam();
        if (minPara != Integer.MAX_VALUE) {
            final List<RexNode> newOnDuplicatedUpdate = new ArrayList<>(onDuplicatedUpdate.size());

            int offset = nextParamIndex.get() - minPara;

            final RexBuilder rexBuilder = insert.getCluster().getRexBuilder();
            final RexUtils.ReplaceDynamicParam replaceDynamicParam = new RexUtils.ReplaceDynamicParam(offset);
            //no action need for offset = 0
            if (offset != 0) {
                onDuplicatedUpdate
                    .forEach(o -> {
                        RexNode rexNode = ((RexCall) o).getOperands().get(1).accept(replaceDynamicParam);
                        List<RexNode> operands = new ArrayList<>(2);
                        operands.add(((RexCall) o).getOperands().get(0));
                        operands.add(rexNode);
                        RexNode rexCall = rexBuilder.makeCall(((RexCall) o).getOperator(), operands);
                        newOnDuplicatedUpdate.add(rexCall);
                    });

                if (null != newDynamicImplicitDefaultParams && null != oldDynamicImplicitDefaultParams) {
                    oldDynamicImplicitDefaultParams.stream()
                        .map(o -> (RexCallParam) o.accept(replaceDynamicParam))
                        .forEach(newDynamicImplicitDefaultParams::add);
                }
            } else {
                newOnDuplicatedUpdate.addAll(onDuplicatedUpdate);

                if (null != newDynamicImplicitDefaultParams && null != oldDynamicImplicitDefaultParams) {
                    newDynamicImplicitDefaultParams.addAll(oldDynamicImplicitDefaultParams);
                }

                if (null != outParamMapping) {
                    onDuplicatedUpdate.forEach(o -> ((RexCall) o).getOperands().get(1).accept(replaceDynamicParam));
                }
            }

            // Update dynamic implicit default param mapping
            if (null != outParamMapping) {
                outParamMapping.putAll(replaceDynamicParam.getParamMapping());
            }

            // Update max parameter index
            nextParamIndex.set(replaceDynamicParam.getMaxParamIndex() + 1);

            return newOnDuplicatedUpdate;
        } else {
            return onDuplicatedUpdate;
        }
    }

    public LogicalInsert buildDynamicValues(ExecutionContext ec) {
        TableMeta tableMeta = ec.getSchemaManager(schemaName)
            .getTable(getLogicalTableName());
        List<String> autoIncrementColumns = tableMeta.getAutoIncrementColumns();
        RelDataType fullRowType = getTable().getRowType();
        // all rows of the target table
        List<RelDataTypeField> fullFieldList = fullRowType.getFieldList();
        // designated insert rows of the target table
        List<RelDataTypeField> insertFieldList = new ArrayList<>(insertRowType.getFieldList());
        // If sequence column is not inserted, add it.
        for (String columnName : autoIncrementColumns) {
            boolean autoIncDefined = false;
            for (RelDataTypeField insertField : insertFieldList) {
                if (insertField.getName().equals(columnName)) {
                    autoIncDefined = true;
                    break;
                }
            }
            if (!autoIncDefined) {
                for (RelDataTypeField field : fullFieldList) {
                    if (field.getName().equals(columnName)) {
                        insertFieldList.add(field);
                        break;
                    }
                }
            }
        }
        RelDataType insertRowType = new RelRecordType(fullRowType.getStructKind(), insertFieldList);
        List<RexNode> tuple = new ArrayList<>(insertFieldList.size());
        for (int i = 0; i < insertFieldList.size(); i++) {
            RexDynamicParam dynamicParam = new RexDynamicParam(fullRowType, i);
            tuple.add(dynamicParam);
        }
        ImmutableList<ImmutableList<RexNode>> tuples = ImmutableList.of(ImmutableList.copyOf(tuple));

        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(getCluster(),
            getTraitSet(),
            insertRowType,
            tuples);
        LogicalInsert logicalInsert = new LogicalInsert(getCluster(),
            getTraitSet(),
            getTable(),
            getCatalogReader(),
            dynamicValues,
            getOperation(),
            isFlattened(),
            getInsertRowType(),
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            ImmutableList.of());
        return logicalInsert;
    }

    public SqlNode getSqlTemplate() {
        if (sqlTemplate == null) {
            sqlTemplate = buildSqlTemplate();
        }
        return sqlTemplate;
    }

    private SqlNode buildSqlTemplate() {
        SqlNode sqlTemplate = getNativeSqlNode();
        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName,
            PlannerContext.getPlannerContext(this).getExecutionContext());
        return sqlTemplate.accept(visitor);
    }

    public SqlNode getNativeSqlNode() {
        return TddlRelToSqlConverter.relNodeToSqlNode(this, dbType);
    }

    /**
     * LogcialModify will be copied several times across the optimizer, so lazy
     * init this field.
     */
    public List<Integer> getLiteralColumnIndex() {
        if (literalColumnIndex == null) {
            initLiteralColumnIndex(false);
        }
        return literalColumnIndex;
    }

    /**
     * LogcialModify will be copied several times across the optimizer, so lazy
     * init this field.
     */
    public List<Integer> getDeterministicColumnIndex(ExecutionContext ec) {
        if (deterministicColumnIndex == null) {
            initDeterministicColumnIndex(ec);
        }
        return deterministicColumnIndex;
    }

    public int getSeqColumnIndex() {
        if (seqColumnIndex == null) {
            initAutoIncrementColumn();
        }
        return seqColumnIndex;
    }

    public Map<Integer, RexNode> getDynamicImplicitDefaultColumnIndexMap() {
        if (dynamicImplicitDefaultColumnIndexMap == null) {
            initImplicitDefaultColumnIndexMap();
        }

        return dynamicImplicitDefaultColumnIndexMap;
    }

    public Map<Integer, RexNode> getImplicitDefaultColumnIndexMap() {
        if (implicitDefaultColumnIndexMap == null) {
            initImplicitDefaultColumnIndexMap();
        }

        return implicitDefaultColumnIndexMap;
    }

    public @NotNull Map<String, RexCallParam> loadDynamicImplicitDefaultParamMap(AtomicInteger maxParamIndex,
                                                                                 boolean doUpdate) {
        final TreeMap<String, RexCallParam> result = getCurrentDynamicImplicitDefaultParamMap();

        if (!doUpdate) {
            return result;
        }

        // Update with current DynamicImplicitDefaultValues
        for (RexNode rexNode : getDynamicImplicitDefaultColumnIndexMap().values()) {
            if (rexNode instanceof RexCall) {
                RexUtils.replaceParamWithHigherScale((RexCall) rexNode, maxParamIndex, result);
            }
        }
        return result;
    }

    private @NotNull TreeMap<String, RexCallParam> getCurrentDynamicImplicitDefaultParamMap() {
        final TreeMap<String, RexCallParam> result = new TreeMap<>(String::compareToIgnoreCase);

        // Convert list to map
        for (RexCallParam o : getDynamicImplicitDefaultParams()) {
            final RexCall rexCall = (RexCall) o.getRexCall();
            result.put(rexCall.getOperator().getName(), o);
        }
        return result;
    }

    /**
     * for json serialization
     */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("insertRowType", insertRowType)
            .item("schemaName", schemaName)
            .itemIf("duplicateKeyUpdateList", duplicateKeyUpdateList, duplicateKeyUpdateList != null)
            .itemIf("dynamicImplicitDefaultParams", dynamicImplicitDefaultParams,
                dynamicImplicitDefaultParams != null);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        // We need Parameters to get routing result.
        Parameters parameterSettings = null;
        Function<RexNode, Object> funcEvaluator = null;
        ExecutionContext executionContext = null;

        if (pw instanceof RelDrdsWriter) {
            Map<Integer, ParameterContext> params = ((RelDrdsWriter) pw).getParams();
            if (params != null) {
                // Copy param
                parameterSettings = new Parameters(new HashMap<>(params), false);
            }
            funcEvaluator = ((RelDrdsWriter) pw).getFuncEvaluator();
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        }

        // Init param
        if (parameterSettings == null) {
            parameterSettings = new Parameters(new HashMap<>(), false);
        }

        // Copy execution context
        if (executionContext == null) {
            executionContext = new ExecutionContext();
            // Use copied param
            executionContext.setParams(parameterSettings);
        } else {
            // Use copied param
            executionContext = executionContext.copy(parameterSettings);
        }

        if (schemaName != null) {
            executionContext.setSchemaName(schemaName);
        }

        if (input instanceof LogicalValues || input instanceof LogicalDynamicValues) {
            LogicalInsert logicalInsert = this;

            if (!logicalInsert.isSourceSelect()) {
                RexUtils.calculateAndUpdateAllRexCallParams(logicalInsert, executionContext);
            }

            // For batch insert, change params index.
            if (logicalInsert.getBatchSize() > 0) {
                buildParamsForBatch(executionContext);
            }

            if (funcEvaluator != null && null == targetTablesHintCache) {
                RexUtils.updateParam(logicalInsert, executionContext, true, null);

                ((RelDrdsWriter) pw).getParams().putAll(executionContext.getParams().getCurrentParameter());
            }

            final List<RelNode> inputs = getPhyPlanForDisplay(executionContext, logicalInsert);
            for (RelNode input : inputs) {
                input.explainForDisplay(pw);
            }
        } else {
            // insert select
            pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
            pw.item("table", getLogicalTableName());
            pw.item("columns", insertRowType);
            pw.item("mode", insertSelectMode);
        }

        return pw;
    }

    public <R extends LogicalInsert> List<RelNode> getPhyPlanForDisplay(ExecutionContext executionContext,
                                                                        R logicalInsert) {
        List<RelNode> inputs;
        if (!hasHint()) {
            // Get plan for primary
            final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
            inputs = new ArrayList<>(primaryWriter.getInput(executionContext));
        } else {
            PhyTableInsertSharder partitioner =
                new PhyTableInsertSharder(logicalInsert, executionContext.getParams(), false);
            inputs = logicalInsert.getInput(partitioner, new ArrayList<>(), executionContext);
        }
        return inputs;
    }

    public boolean withWriter() {
        return null != getPrimaryInsertWriter();
    }

    /**
     * Find those columns which must be literal. If it's a call, we need to
     * convert it to literal. Stored column indexes are corresponding to the
     * full row type, not insertRowType.
     * <pre>
     * Including:
     * 1. Partition columns of primary and index table
     * 2. Local unique keys of primary table, if gsi exists or INSERT IGNORE/REPLACE/INSERT ON DUPLICATE KEY UPDATE on partitioned table
     * 3. For INSERT ON DUPLICATED KEY UPDATE, if VALUES(col) appears in the update list, col must be literal
     * 4. All columns with auto increment property must be literal
     * </pre>
     */
    public void initLiteralColumnIndex(boolean withScaleOut) {
        final String tableName = targetTableNames.get(0);
        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert null != oc;
        final List<String> columnNames = getInsertRowType().getFieldNames();
        final ExecutionContext ec = PlannerContext.getPlannerContext(this.getCluster()).getExecutionContext();

        // Get name of columns must be literal
        final Set<String> literalColumnNames =
            LogicalWriteUtil.literalColumnNames(schemaName, tableName, ec);

        if (withScaleOut) {
            // For scale out, all values that can not be pushdown should
            // replace with literal
            LogicalDynamicValues values = (LogicalDynamicValues) this.getInput();
            List<RexNode> row = values.getTuples().get(0);
            for (int i = 0; i < columnNames.size(); i++) {
                RexNode node = row.get(i);
                String fieldName = columnNames.get(i);
                if (!BuildFinalPlanVisitor.canBePushDown(node, true)) {
                    literalColumnNames.add(fieldName);
                }
            }
        }

        // Convert column names to column indexes
        final Set<Integer> literalColumnIndexes = new HashSet<>(literalColumnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            if (literalColumnNames.contains(columnNames.get(i))) {
                literalColumnIndexes.add(i);
            }
        }

        if (!GlobalIndexMeta.hasGsi(tableName, schemaName, ec)) {
            // If VALUES(col) appears in the update list, col must be literal
            List<RexNode> updateList = getDuplicateKeyUpdateList();
            if (updateList != null && !updateList.isEmpty()) {
                for (RexNode rexNode : updateList) {
                    getUpdateValuesColumns(rexNode, literalColumnIndexes);
                }
            }
        }

        literalColumnIndex = Lists.newArrayList(literalColumnIndexes);
    }

    /**
     * Columns has to be replaced with literal if multi write happens
     * <p>
     * Including:
     * 1. All columns of broadcast table
     * 2. All columns of gsi table
     * 3. All column of ScaleOut writable table
     */
    private void initDeterministicColumnIndex(ExecutionContext ec) {
        final Set<String> resultColumnNames =
            LogicalWriteUtil.deterministicColumnNames(schemaName, targetTableNames.get(0), ec);

        // Convert column names to column indexes
        final List<String> fieldNames = getInsertRowType().getFieldNames();
        final Set<Integer> calcColumnIndexes = new HashSet<>(resultColumnNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            if (resultColumnNames.contains(fieldNames.get(i))) {
                calcColumnIndexes.add(i);
            }
        }

        this.deterministicColumnIndex = Lists.newArrayList(calcColumnIndexes);
    }

    /**
     * Columns has to be replaced with literal if multi write happens and statement explicitly set column to null
     * <p>
     * Including:
     * 1. All columns with type of timestamp not null
     * 2. All columns with type of not null and the statement is insert ignore
     */
    private void initImplicitDefaultColumnIndexMap() {
        final Map<String, RexNode> allImplicitDefaultColumnNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, RexNode> dynamicImplicitDefaultColumnNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        // DN will use implicit default for non-null column in multi-value-insert and insert select statement,
        // no matter if it is insert ignore or not.
        // see https://dev.mysql.com/doc/refman/5.7/en/constraint-invalid-data.html
        //
        // Try on dn with example below:
        //
        // CREATE TABLE `t` (`a` bigint(20) NOT NULL);
        // SET sql_mode = "";
        //
        // -- Throw exception: ERROR 1048 (23000): Column 'a' cannot be null
        // INSERT INTO t (`a`) VALUES(null);
        //
        // -- No exception, only warning: Warning | 1048 | Column 'a' cannot be null
        // INSERT INTO t (`a`) VALUES(null),(null);
        //
        // -- No exception, only warning: Warning | 1048 | Column 'a' cannot be null
        // INSERT INTO t (`a`) SELECT NULL FROM t;
        //
        // So that we have to replace implicit default value for "must-be-literal" column no meter if it is insert ignore or not.
        final boolean isInsertIgnore = true;
        //final boolean isInsertIgnore = withIgnore();

        LogicalWriteUtil.implicitDefaultColumnNames(getTargetTables().get(0),
            isInsertIgnore,
            rexBuilder,
            allImplicitDefaultColumnNames,
            dynamicImplicitDefaultColumnNames);

        // Convert column names to column indexes
        final List<String> fieldNames = getInsertRowType().getFieldNames();
        final Map<Integer, RexNode> allColumnIndexes = new HashMap<>(allImplicitDefaultColumnNames.size());
        final Map<Integer, RexNode> dynamicImplicitColumnIndexes =
            new HashMap<>(dynamicImplicitDefaultColumnNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            final String columnName = fieldNames.get(i);
            final RexNode defaultValueRex = allImplicitDefaultColumnNames.get(columnName);
            if (defaultValueRex != null) {
                allColumnIndexes.put(i, defaultValueRex);
                if (dynamicImplicitDefaultColumnNames.containsKey(columnName)) {
                    dynamicImplicitColumnIndexes.put(i, defaultValueRex);
                }
            }
        }

        synchronized (this) {
            this.implicitDefaultColumnIndexMap = allColumnIndexes;
            this.dynamicImplicitDefaultColumnIndexMap = dynamicImplicitColumnIndexes;
        }
    }

    private static void getUpdateValuesColumns(RexNode rexNode, Set<Integer> columnIndexes) {
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperator() == TddlOperatorTable.VALUES) {
                int columnIndex = ((RexInputRef) call.getOperands().get(0)).getIndex();
                columnIndexes.add(columnIndex);
                return;
            }

            List<RexNode> subNodes = call.getOperands();
            for (RexNode subNode : subNodes) {
                getUpdateValuesColumns(subNode, columnIndexes);
            }
        }
    }

    /**
     * Find auto increment column and its corresponding index in full row type.
     */
    public void initAutoIncrementColumn() {
        seqColumnIndex = -1;

        String tableName = getLogicalTableName();
        TableMeta tableMeta = PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
            .getTable(tableName);
        List<String> autoIncrementColumns = tableMeta.getAutoIncrementColumns();
        if (autoIncrementColumns.isEmpty()) {
            return;
        }

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();

        if (or.isTableInSingleDb(tableName) || or.isBroadCast(tableName)) {
            if (!SequenceManagerProxy.getInstance().isUsingSequence(schemaName, tableName)) {
                // It's using MySQL auto increment rule
                return;
            }
        }

        final RelDataType rowType = isSourceSelect() ? insertRowType : input.getRowType();
        if (rowType == null) {
            return;
        }

        List<RelDataTypeField> fields = rowType.getFieldList();
        for (int i = 0; i < fields.size(); i++) {
            if (autoIncrementColumns.contains(fields.get(i).getName())) {
                seqColumnIndex = i;
                break;
            }
        }
    }

    /**
     * In broadcast tables and gsi tables, data must be consistent. That is,
     * some functions should be calculated in advance.
     */
    public boolean needConsistency() {
        String tableName = getLogicalTableName();
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        return or.isBroadCast(tableName) || GlobalIndexMeta
            .hasGsi(tableName, schemaName, PlannerContext.getPlannerContext(this).getExecutionContext());
    }

    public String getLogicalTableName() {
        return getTargetTableNames().get(0);
    }

    public DbType getDbType() {
        return dbType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        LogicalInsert newLogicalInsert = new LogicalInsert(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            isFlattened(),
            insertRowType,
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getEvalRowColMetas(),
            getGenColRexNodes(),
            getInputToEvalFieldsMapping(),
            getDefaultExprColMetas(),
            getDefaultExprColRexNodes(),
            getDefaultExprEvalFieldsMapping(),
            isPushablePrimaryKeyCheck(),
            isPushableForeignConstraintCheck(),
            isModifyForeignKey(),
            isUkContainsAllSkAndGsiContainsAllUk(),
            getDynamicImplicitDefaultParams(),
            getUnoptimizedDynamicImplicitDefaultParams());
        newLogicalInsert.sqlTemplate = sqlTemplate;
        newLogicalInsert.literalColumnIndex = literalColumnIndex;
        newLogicalInsert.seqColumnIndex = seqColumnIndex;
        newLogicalInsert.tupleRoutingInfo = tupleRoutingInfo;
        newLogicalInsert.insertSelectMode = insertSelectMode;
        return newLogicalInsert;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, List<RexCallParam> dynamicImplicitDefaultParams) {
        LogicalInsert newLogicalInsert = new LogicalInsert(getCluster(),
            traitSet,
            table,
            catalogReader,
            sole(inputs),
            getOperation(),
            isFlattened(),
            insertRowType,
            getKeywords(),
            getDuplicateKeyUpdateList(),
            getBatchSize(),
            getAppendedColumnIndex(),
            getHints(),
            getTableInfo(),
            getPrimaryInsertWriter(),
            getGsiInsertWriters(),
            getAutoIncParamIndex(),
            getUnOptimizedLogicalDynamicValues(),
            getUnOptimizedDuplicateKeyUpdateList(),
            getEvalRowColMetas(),
            getGenColRexNodes(),
            getInputToEvalFieldsMapping(),
            getDefaultExprColMetas(),
            getDefaultExprColRexNodes(),
            getDefaultExprEvalFieldsMapping(),
            isPushablePrimaryKeyCheck(),
            isPushableForeignConstraintCheck(),
            isModifyForeignKey(),
            isUkContainsAllSkAndGsiContainsAllUk(),
            dynamicImplicitDefaultParams,
            getUnoptimizedDynamicImplicitDefaultParams());
        newLogicalInsert.sqlTemplate = sqlTemplate;
        newLogicalInsert.literalColumnIndex = literalColumnIndex;
        newLogicalInsert.seqColumnIndex = seqColumnIndex;
        newLogicalInsert.tupleRoutingInfo = tupleRoutingInfo;
        newLogicalInsert.insertSelectMode = insertSelectMode;
        return newLogicalInsert;
    }

    public String explainNodeName() {
        if (isReplace()) {
            return "LogicalReplace";
        }
        return "LogicalInsert";
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public boolean isSimpleInsert(boolean ignoreIsSimple) {
        if (ignoreIsSimple) {
            return isInsert() && !withDuplicateKeyUpdate() && !isSourceSelect();
        } else {
            return isInsert() && !withDuplicateKeyUpdate() && !withIgnore() && !isSourceSelect();
        }
    }

    public boolean isSimpleInsert() {
        // INSERT or INSERT SELECT
        return isInsert() && !withIgnore() && !withDuplicateKeyUpdate();
    }

    public boolean isInsertIgnore() {
        return isInsert() && withIgnore() && !withDuplicateKeyUpdate();
    }

    public boolean isUpsert() {
        return isInsert() && withDuplicateKeyUpdate();
    }

    public boolean withDuplicateKeyUpdate() {
        return GeneralUtil.isNotEmpty(getDuplicateKeyUpdateList());
    }

    public boolean withIgnore() {
        return Optional.ofNullable(getKeywords())
            .filter(l -> l.stream().anyMatch("ignore"::equalsIgnoreCase)).isPresent();
    }

    public boolean withDynamicImplicitDefaultParams() {
        return GeneralUtil.isNotEmpty(getDynamicImplicitDefaultParams());
    }

    public PartitionTupleRouteInfo getTupleRoutingInfo() {
        if (!initTupleRoutingInfo) {
            initTupleRouteInfo();
        }
        return tupleRoutingInfo;
    }

    public PartitionTupleRouteInfo getReplicationTupleRoutingInfo(PartitionInfo replicationPartitionInfo) {
        if (!initReplicationTupleRoutingInfo) {
            initReplicationTupleRouteInfo(replicationPartitionInfo);
        }
        return replicationTupleRoutingInfo;
    }

    public static class HandlerParams {
        public boolean usingSequence = true;
        public long lastInsertId = 0;
        public long returnedLastInsertId = 0;
        public long expectAffectedRows = 0;
        public boolean autoIncrementUsingSeq = false;
        public boolean optimizedWithReturning = false;
    }

    /**
     * Convert params from Map<> to List<Map<>>
     */
    public void buildParamsForBatch(ExecutionContext executionContext) {
        Parameters parameterSettings = executionContext.getParams();
        if (parameterSettings == null) {
            return;
        }

        final Map<Integer, ParameterContext> oldParams = parameterSettings.getCurrentParameter();
        final int fieldNum = countParamNumInEachBatch();
        final List<Integer> duplicateKeyUpdateParamIndexes = getParamInDuplicateKeyUpdateList();

        final int duplicateKeyUpdateParamNum = duplicateKeyUpdateParamIndexes.size();
        final int batchSize = getBatchSize();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(batchSize);

        for (int batchIndex = 0; batchIndex < batchSize; batchIndex++) {
            final Map<Integer, ParameterContext> rowValues = new HashMap<>(fieldNum);

            final int rowOffset = batchIndex * fieldNum;
            for (int fieldIndex = 0; fieldIndex < fieldNum; fieldIndex++) {
                final int oldIndex = rowOffset + fieldIndex + 1;
                final ParameterContext oldPc = oldParams.get(oldIndex);

                final int newIndex = fieldIndex + 1;
                final ParameterContext newPc = changeParameterContextIndex(oldPc, newIndex);

                rowValues.put(newIndex, newPc);
            }

            // ON DUPLICATE KEY UPDATE param
            for (int updateIndex = 0; updateIndex < duplicateKeyUpdateParamNum; updateIndex++) {
                int oldIndex = duplicateKeyUpdateParamIndexes.get(updateIndex) + 1;
                final ParameterContext oldPc = oldParams.get(oldIndex);

                // keep old param index
                final ParameterContext newPc = changeParameterContextIndex(oldPc, oldIndex);

                rowValues.put(oldIndex, newPc);
            }

            batchParams.add(rowValues);
        }

        parameterSettings.setBatchParams(batchParams);
    }

    /**
     * Get index of parameters referenced in ON DUPLICATE KEY UPDATE list.
     * Get real parameter index for RexCallParam
     *
     * @return Parameter indexes in ON DUPLICATE KEY UPDATE list
     */
    public List<Integer> getParamInDuplicateKeyUpdateList() {
        final List<RexNode> duplicateKeyUpdateList = getDuplicateKeyUpdateList();
        final List<Integer> duplicateKeyUpdateParamIndexes = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(duplicateKeyUpdateList)) {
            for (RexNode rex : duplicateKeyUpdateList) {
                final RexNode val = ((RexCall) rex).getOperands().get(1);
                if (val instanceof RexCallParam) {
                    RexUtils.ParamFinder.getParams(((RexCallParam) val).getRexCall())
                        .forEach(p -> duplicateKeyUpdateParamIndexes.add(p.getIndex()));
                } else if (val instanceof RexDynamicParam) {
                    duplicateKeyUpdateParamIndexes.add(((RexDynamicParam) val).getIndex());
                } else if (val instanceof RexCall) {
                    RexUtils.ParamFinder.getParams(val)
                        .forEach(p -> duplicateKeyUpdateParamIndexes.add(p.getIndex()));
                }
            }
        }
        return duplicateKeyUpdateParamIndexes;
    }

    /**
     * Iterate over one row, counting number of DynamicParam. If there's an
     * auto_increment column, it could be Literal.
     *
     * @return How many DynamicParam in each row
     */
    private int countParamNumInEachBatch() {
        // It could only be LogicalDynamicValues in batch mode.
        LogicalDynamicValues input = (LogicalDynamicValues) getInput();
        // Every row must be the same.
        List<RexNode> rowNodes = input.getTuples().get(0);

        final Set<Integer> autoIncParamIndex = new HashSet<>(getAutoIncParamIndex());
        final long amendColumnCount = rowNodes.stream()
            .filter(
                rex ->
                    (rex instanceof RexDynamicParam && autoIncParamIndex.contains(((RexDynamicParam) rex).getIndex()))
                        || (rex instanceof RexCallParam && !((RexCallParam) rex).isWrapRexDynamicParam())
            ).count();

        final AtomicInteger num = new AtomicInteger();
        for (RexNode node : rowNodes) {
            if (node instanceof RexDynamicParam) {
                num.getAndIncrement();
            } else if (node instanceof RexCall) {
                RexUtils.RexDynamicParamVisitor.analyze(node, (r) -> {
                    if (!(r instanceof RexCallParam) || ((RexCallParam) r).isWrapRexDynamicParam()) {
                        num.getAndIncrement();
                    }
                    return false;
                });
            }
        }

        return (int) (num.get() - amendColumnCount);
    }

    protected void initTupleRouteInfo() {
        if (initTupleRoutingInfo) {
            return;
        }
        synchronized (this) {
            if (!initTupleRoutingInfo) {
                if (!isSourceSelect() && input != null) {
                    String logTbName = getLogicalTableName();
                    PartitionInfo partInfo =
                        PlannerContext.getPlannerContext(this).getExecutionContext()
                            .getSchemaManager(this.getSchemaName())
                            .getTable(logTbName)
                            .getPartitionInfo();
                    this.tupleRoutingInfo =
                        PartitionPruner.generatePartitionTupleRoutingInfo(this, partInfo);
                } else {
                    this.tupleRoutingInfo = null;
                }
                this.initTupleRoutingInfo = true;
            }

        }
    }

    protected void initReplicationTupleRouteInfo(PartitionInfo partInfo) {
        if (initReplicationTupleRoutingInfo) {
            return;
        }
        synchronized (this) {
            if (!initReplicationTupleRoutingInfo && partInfo != null) {
                if (!isSourceSelect() && input != null) {
                    this.replicationTupleRoutingInfo =
                        PartitionPruner.generatePartitionTupleRoutingInfo(this, partInfo);
                } else {
                    this.replicationTupleRoutingInfo = null;
                }
                this.initReplicationTupleRoutingInfo = true;
            }

        }
    }

    public List<InsertWriter> getGsiInsertIgnoreWriters() {
        return gsiInsertIgnoreWriters;
    }

    public void setGsiInsertIgnoreWriters(List<InsertWriter> gsiInsertIgnoreWriters) {
        this.gsiInsertIgnoreWriters = gsiInsertIgnoreWriters;
    }

    public DistinctWriter getPrimaryDeleteWriter() {
        return primaryDeleteWriter;
    }

    public void setPrimaryDeleteWriter(DistinctWriter primaryDeleteWriter) {
        this.primaryDeleteWriter = primaryDeleteWriter;
    }

    public List<DistinctWriter> getGsiDeleteWriters() {
        return gsiDeleteWriters;
    }

    public void setGsiDeleteWriters(List<DistinctWriter> gsiDeleteWriters) {
        this.gsiDeleteWriters = gsiDeleteWriters;
    }

    public void setInsertSelectMode(InsertSelectMode mode) {
        this.insertSelectMode = mode;
    }

    public InsertSelectMode getInsertSelectMode() {
        return insertSelectMode;
    }

    public List<RexNode> getGenColRexNodes() {
        return genColRexNodes;
    }

    public void setGenColRexNodes(List<RexNode> genColRexNodes) {
        this.genColRexNodes = genColRexNodes;
    }

    public List<ColumnMeta> getEvalRowColMetas() {
        return evalRowColMetas;
    }

    public void setEvalRowColMetas(List<ColumnMeta> evalRowColMetas) {
        this.evalRowColMetas = evalRowColMetas;
    }

    public List<Integer> getInputToEvalFieldsMapping() {
        return inputToEvalFieldsMapping;
    }

    public void setInputToEvalFieldsMapping(List<Integer> inputToEvalFieldsMapping) {
        this.inputToEvalFieldsMapping = inputToEvalFieldsMapping;
    }

    public List<ColumnMeta> getDefaultExprColMetas() {
        return defaultExprColMetas;
    }

    public void setDefaultExprColMetas(List<ColumnMeta> defaultExprColMetas) {
        this.defaultExprColMetas = defaultExprColMetas;
    }

    public List<Integer> getDefaultExprEvalFieldsMapping() {
        return defaultExprEvalFieldsMapping;
    }

    public void setDefaultExprEvalFieldsMapping(List<Integer> defaultExprEvalFieldsMapping) {
        this.defaultExprEvalFieldsMapping = defaultExprEvalFieldsMapping;
    }

    public List<RexNode> getDefaultExprColRexNodes() {
        return defaultExprColRexNodes;
    }

    public void setDefaultExprColRexNodes(List<RexNode> defaultExprColRexNodes) {
        this.defaultExprColRexNodes = defaultExprColRexNodes;
    }

    public boolean isPushablePrimaryKeyCheck() {
        return pushablePrimaryKeyCheck;
    }

    public void setPushablePrimaryKeyCheck(boolean pushablePrimaryKeyCheck) {
        this.pushablePrimaryKeyCheck = pushablePrimaryKeyCheck;
    }

    public boolean isPushableForeignConstraintCheck() {
        return pushableForeignConstraintCheck;
    }

    public void setPushableForeignConstraintCheck(boolean pushableForeignConstraintCheck) {
        this.pushableForeignConstraintCheck = pushableForeignConstraintCheck;
    }

    public boolean isModifyForeignKey() {
        return this.modifyForeignKey;
    }

    public void setModifyForeignKey(boolean modifyForeignKeys) {
        this.modifyForeignKey = modifyForeignKeys;
    }

    public boolean isUkContainsAllSkAndGsiContainsAllUk() {
        return ukContainsAllSkAndGsiContainsAllUk;
    }

    public void setUkContainsAllSkAndGsiContainsAllUk(boolean ukContainsAllSkAndGsiContainsAllUk) {
        this.ukContainsAllSkAndGsiContainsAllUk = ukContainsAllSkAndGsiContainsAllUk;
    }
}
