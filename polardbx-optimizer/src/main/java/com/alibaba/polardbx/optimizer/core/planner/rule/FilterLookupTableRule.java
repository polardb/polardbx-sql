package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.core.TddlOperatorTable.CARTESIAN;
import static com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil.hasIndexHint;

public class FilterLookupTableRule extends RelOptRule {

    public static final FilterLookupTableRule INSTANCE = new FilterLookupTableRule();

    private List<SqlKind> supportedNotEqKinds = ImmutableList.of(SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
        SqlKind.LESS_THAN_OR_EQUAL, SqlKind.LESS_THAN);

    protected FilterLookupTableRule() {
        super(operand(Sort.class, operand(Project.class, operand(Filter.class, operand(LogicalView.class, none())))),
            "FilterLookupTableRule");
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_IN_TO_UNION_ALL);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!enable(PlannerContext.getPlannerContext(call))) {
            return false;
        }
        Project project = call.rel(1);
        RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
        //Don't support the sub-query!
        if (e != null) {
            return false;
        }
        Filter filter = call.rel(2);
        //Don't support the  sub-query!
        e = RexUtil.SubQueryFinder.find(project.getProjects());
        if (e != null) {
            return false;
        }
        final List<RexNode> predicates = RelOptUtil.conjunctions(filter.getCondition());
        if (predicates.isEmpty()) {
            return false;
        }
        boolean ret = PlannerContext.getPlannerContext(call).getParamManager().getBoolean(
            ConnectionParams.ENABLE_IN_TO_EXPAND_IN);
        boolean containExpandIn = predicates.stream().anyMatch(t -> t.getKind() == SqlKind.IN &&
            (((SqlInOperator) ((RexCall) t).getOperator()).isExpand() || ret));

        if (!containExpandIn) {
            return false;
        }
        LogicalView logicalView = call.rel(3);

        //Only support a table
        if (logicalView.getTableNames().size() != 1) {
            return false;
        }
        TableMeta tableMeta = CBOUtil.getTableMeta(logicalView.getTable());
        //Only support the sharding table
        boolean isSharding = TableTopologyUtil.isShard(tableMeta);
        return isSharding;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        int strictMode = PlannerContext.getPlannerContext(call).getParamManager().getInt(
            ConnectionParams.IN_TO_UNION_STRICT_MODE);
        Sort sort = call.rel(0);
        Project project = call.rel(1);
        Filter filter = call.rel(2);
        LogicalView logicalView = call.rel(3);
        TableMeta tableMeta = CBOUtil.getTableMeta(logicalView.getTable());

        final List<RexNode> predicates = RelOptUtil.conjunctions(filter.getCondition());
        final List<RexNode> newPredicates = new ArrayList<>();
        RexBuilder rb = call.builder().getRexBuilder();
        List<String> inValuesColumns = new ArrayList<>();
        List<Integer> inValuesKeys = new ArrayList<>();
        List<RelDataType> inValuesTypes = new ArrayList<>();
        List<RexNode> inValuesRexParam = new ArrayList<>();
        List<RexNode> inValuesRexNodes = new ArrayList<>();
        Pair<IndexMeta, Set<Integer>> pair = findTargetIndexAndInPredicate(
            call, predicates, newPredicates, strictMode, tableMeta, inValuesColumns, inValuesKeys, inValuesTypes,
            inValuesRexParam, inValuesRexNodes, sort, filter);

        IndexMeta targetIndex = null;
        Set<Integer> targetInKeySet = null;

        if (pair == null) {
            if (strictMode == 2 && !inValuesKeys.isEmpty()) {
                RelNode rel = logicalView.getPushedRelNode();
                TableScanFinder tableScanFinder = new TableScanFinder();
                rel.accept(tableScanFinder);
                List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
                TableScan tableScan = tableScans.get(0).getValue();
                if (!hasIndexHint(tableScan)) {
                    return;
                }
                targetInKeySet = ImmutableBitSet.range(0, inValuesKeys.size()).asSet();
            } else {
                return;
            }
        } else {
            targetIndex = pair.getKey();
            targetInKeySet = pair.getValue();
        }

        List<String> targetInColumns = new ArrayList<>();
        List<Integer> targetInKeys = new ArrayList<>();
        List<RelDataType> targetInTypes = new ArrayList<>();
        List<RexNode> targetInRexParam = new ArrayList<>();
        for (Integer index : targetInKeySet) {
            targetInColumns.add(inValuesColumns.get(index));
            targetInKeys.add(inValuesKeys.get(index));
            targetInTypes.add(inValuesTypes.get(index));
            targetInRexParam.add(inValuesRexParam.get(index));
        }

        TreeSet<String> ignoreShardingColumns = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        ignoreShardingColumns.addAll(OptimizerContext.getContext(tableMeta.getSchemaName())
            .getRuleManager()
            .getSharedColumns(tableMeta.getTableName()));

        for (String column : targetInColumns) {
            if (ignoreShardingColumns.contains(column)) {
                //Don't support convert the in to union all when the in values column contains the shared column
                //because the DelegateInValue don't support partition pruning.
                return;
            }
        }

        //remain the in values which are not in the target index
        for (int i = 0; i < inValuesRexNodes.size(); i++) {
            if (!targetInKeySet.contains(i)) {
                newPredicates.add(inValuesRexNodes.get(i));
            }
        }

        RelDataTypeFactory typeFactory = filter.getCluster().getTypeFactory();
        RelDataType dynamicRowType =
            filter.getCluster().getTypeFactory().createStructType(targetInTypes, targetInColumns);

        RexNode cartesianNode = rb.makeCall(CARTESIAN, targetInRexParam);
        DynamicValues newValues = DynamicValues.create(filter.getCluster(),
            filter.getTraitSet(),
            dynamicRowType,
            ImmutableList.of(ImmutableList.of(cartesianNode)));

        final RelBuilder relBuilder = call.builder();

        //generate the in filter
        RexNode inRowNode = null;
        if (targetInKeys.size() == 1) {
            inRowNode = rb.makeInputRef(logicalView, targetInKeys.get(0));
        } else {
            List<RexNode> rexNodeList = new ArrayList<>();
            for (int i = 0; i < targetInKeys.size(); i++) {
                rexNodeList.add(rb.makeInputRef(logicalView, targetInKeys.get(i)));
            }
            inRowNode = rb.makeCall(SqlStdOperatorTable.ROW, rexNodeList);
        }

        RexNode row =
            rb.makeCall(SqlStdOperatorTable.ROW, rb.makeDynamicParam(typeFactory.createSqlType(
                SqlTypeName.CHAR), PlannerUtils.APPLY_IN_VALUES_PARAM_INDEX));
        RexNode newInFilterCondition = rb.makeCall(SqlStdOperatorTable.IN, inRowNode, row);
        newPredicates.add(newInFilterCondition);

        RexNode deDuplicateCondition =
            RexUtil.composeConjunction(rb, newPredicates, false);

        Filter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), deDuplicateCondition);
        final RexNode joinCond = RelOptUtil.createEquiJoinCondition(
            newFilter, targetInKeys, newValues, ImmutableIntList.range(
                0, dynamicRowType.getFieldCount()), rb);

        RelCollation relCollation = sortProjectTranspose(sort, project);
        if (relCollation == null || relCollation.getKeys().size() != sort.getCollation().getKeys().size()) {
            return;
        } else {
            //force index
            forceIndex(logicalView, targetIndex);
        }
        RelNode newSort = sort.copy(sort.getTraitSet(), newFilter, relCollation);

        relBuilder.push(newSort);
        relBuilder.push(newValues);
        relBuilder.logicalSemiJoin(ImmutableList.of(joinCond), new SqlNodeList(SqlParserPos.ZERO));
        relBuilder.project(project.getProjects());
        RelNode newProject = relBuilder.build();

        // disable mpp for current MaterializedSemiJoin.
        PlannerContext.getPlannerContext(call).getExtraCmds().put(ConnectionProperties.ENABLE_MPP, false);
        // disable the other join.
        PlannerContext.getPlannerContext(call).getExtraCmds().put(ConnectionProperties.ENABLE_SEMI_HASH_JOIN, false);
        PlannerContext.getPlannerContext(call).getExtraCmds().put(ConnectionProperties.ENABLE_SEMI_NL_JOIN, false);
        PlannerContext.getPlannerContext(call).getExtraCmds()
            .put(ConnectionProperties.ENABLE_SEMI_SORT_MERGE_JOIN, false);
        PlannerContext.getPlannerContext(call).getExtraCmds().put(ConnectionProperties.ENABLE_SEMI_BKA_JOIN, false);

        // disable post planner
        PlannerContext.getPlannerContext(call).getExtraCmds().put(ConnectionProperties.ENABLE_POST_PLANNER, false);

        call.transformTo(newProject);
    }

    private boolean isLiteral(RexNode rexNode) {
        if (rexNode instanceof RexDynamicParam || rexNode instanceof RexLiteral) {
            return true;
        }
        return false;
    }

    private boolean isInRexDynamicParam(RexNode rexNode) {
        if (isLiteral(rexNode)) {
            return true;
        }

        if (rexNode instanceof RexNode && rexNode.getKind() == SqlKind.ROW) {
            RexNode ret = ((RexCall) rexNode).getOperands().get(0);
            return isLiteral(ret);
        }
        return false;
    }

    private RexNode newRexDynamicList(RexBuilder rb, RexNode rexNode, RelDataType relDataType) {
        if (isLiteral(rexNode)) {
            return rb.makeCall(TddlOperatorTable.LIST, Lists.newArrayList(rexNode));
        }
        return rb.makeCall(TddlOperatorTable.LIST, ((RexCall) rexNode).getOperands());
    }

    private Pair<IndexMeta, Set<Integer>> findTargetIndexAndInPredicate(
        RelOptRuleCall call,
        List<RexNode> predicates,
        List<RexNode> newPredicates,
        int strictMode,
        TableMeta tableMeta,
        List<String> inValuesColumns,
        List<Integer> inValuesKeys,
        List<RelDataType> inValuesTypes,
        List<RexNode> inValuesRexParam,
        List<RexNode> inValuesRexNodes,
        Sort sort,
        Filter filter) {
        final RexBuilder rb = call.builder().getRexBuilder();
        final RelMetadataQuery mq = call.getMetadataQuery();
        Set<String> equalSetColumns = new HashSet<>();
        List<String> orderByColumns = new ArrayList<>();
        List<String> notEqualColumns = new ArrayList<>();

        boolean ret = PlannerContext.getPlannerContext(call).getParamManager().getBoolean(
            ConnectionParams.ENABLE_IN_TO_EXPAND_IN);

        for (RexNode rexNode : predicates) {
            if (rexNode.isAlwaysTrue()) {
                continue;
            }
            int index = -1;
            int kindType = -1; // 0 mean EQUALS, 1 mean <=>, 2 mean in
            RexNode dynamicListParam = null;
            RelDataType inValuesType = null;
            if (SqlKind.EQUALS.equals(rexNode.getKind()) && rexNode instanceof RexCall) {
                RexNode leftRexNode = ((RexCall) rexNode).getOperands().get(0);
                RexNode rightRexNode = ((RexCall) rexNode).getOperands().get(1);
                if (leftRexNode instanceof RexInputRef && isLiteral(rightRexNode)) {
                    index = ((RexInputRef) leftRexNode).getIndex();
                } else if (isLiteral(leftRexNode) && rightRexNode instanceof RexInputRef) {
                    index = ((RexInputRef) rightRexNode).getIndex();
                }
                kindType = 0;
                newPredicates.add(rexNode);
            } else if (supportedNotEqKinds.contains(rexNode.getKind()) && rexNode instanceof RexCall) {
                RexNode leftRexNode = ((RexCall) rexNode).getOperands().get(0);
                RexNode rightRexNode = ((RexCall) rexNode).getOperands().get(1);
                if (leftRexNode instanceof RexInputRef && isLiteral(rightRexNode)) {
                    index = ((RexInputRef) leftRexNode).getIndex();
                } else if (isLiteral(leftRexNode) && rightRexNode instanceof RexInputRef) {
                    index = ((RexInputRef) rightRexNode).getIndex();
                }
                kindType = 1;
                newPredicates.add(rexNode);
            } else if (SqlKind.IN.equals(rexNode.getKind()) && rexNode instanceof RexCall &&
                (((SqlInOperator) ((RexCall) rexNode).getOperator()).isExpand() || ret)) {
                RexNode leftRexNode = ((RexCall) rexNode).getOperands().get(0);
                RexNode rightRexNode = ((RexCall) rexNode).getOperands().get(1);
                if (leftRexNode instanceof RexInputRef && isInRexDynamicParam(rightRexNode)) {
                    index = ((RexInputRef) leftRexNode).getIndex();
                    inValuesType = leftRexNode.getType();
                    dynamicListParam = newRexDynamicList(rb, rightRexNode, leftRexNode.getType());
                } else if (isInRexDynamicParam(leftRexNode) && rightRexNode instanceof RexInputRef) {
                    index = ((RexInputRef) rightRexNode).getIndex();
                    inValuesType = rightRexNode.getType();
                    dynamicListParam = newRexDynamicList(rb, leftRexNode, rightRexNode.getType());
                }
                kindType = 2;
            } else {
                newPredicates.add(rexNode);
            }
            if (index == -1) {
                continue;
            }
            RelColumnOrigin relColumnOrigin =
                mq.getColumnOrigin(filter.getInput(), index);
            if (relColumnOrigin == null) {
                return null;
            }
            if (kindType == 0) {
                equalSetColumns.add(relColumnOrigin.getColumnName().toLowerCase(Locale.ROOT));
            } else if (kindType == 1) {
                notEqualColumns.add(relColumnOrigin.getColumnName().toLowerCase(Locale.ROOT));
            } else {
                inValuesKeys.add(index);
                inValuesColumns.add(relColumnOrigin.getColumnName().toLowerCase(Locale.ROOT));
                inValuesRexParam.add(dynamicListParam);
                inValuesTypes.add(inValuesType);
                inValuesRexNodes.add(rexNode);
            }
        }

        ImmutableIntList immutableIntList = sort.collation.getKeys();
        for (int index : immutableIntList) {
            RelColumnOrigin relColumnOrigin =
                mq.getColumnOrigin(sort, index);
            if (relColumnOrigin == null) {
                return null;
            }
            orderByColumns.add(relColumnOrigin.getColumnName().toLowerCase(Locale.ROOT));
        }

        if (inValuesColumns.isEmpty()) {
            return null;
        }

        List<List<String>> referenceColumns = new ArrayList<>();
        //select the proper index, firstly from order by columns, then from not equal columns
        referenceColumns.add(orderByColumns);

        if (strictMode == 1) {
            for (String notEqualColumn : notEqualColumns) {
                referenceColumns.add(ImmutableList.of(notEqualColumn));
            }
        }

        Pair<IndexMeta, Set<Integer>> pair = null;
        for (List<String> reference : referenceColumns) {
            //If the reference columns are all in the equal set, no need to do anything
            if (equalSetColumns.containsAll(reference)) {
                return pair;
            }

            //If the in values columns are all in the reference columns, no need to do anything
            if (reference.containsAll(inValuesColumns)) {
                return pair;
            }
            List<IndexMeta> indexMetas = tableMeta.getIndexes();
            pair =
                getForceIndexMeta(indexMetas, equalSetColumns, reference, inValuesColumns);
            if (pair != null) {
                break;
            }
        }
        return pair;
    }

    private Pair<IndexMeta, Set<Integer>> getForceIndexMeta(
        List<IndexMeta> indexMetas, Set<String> equalSetColumns, List<String> referenceByColumns,
        List<String> inValuesColumns) {
        List<Pair<IndexMeta, Set<Integer>>> rets = new ArrayList<>();
        int maxMatchInSize = -1;
        for (IndexMeta indexMeta : indexMetas) {
            Set<Integer> matchInIndexs = new HashSet<>();
            List<String> localIndex =
                indexMeta.getKeyColumns().stream().map(
                    t -> t.getOriginColumnName().toLowerCase(Locale.ROOT)).collect(Collectors.toList());
            //make sure the prefix columns of the order by columns are in the index
            int matchOrderBySize = 0;
            int startOffsetForOrderBy = -1;
            for (int i = 0; i < indexMeta.getKeyColumns().size() && matchOrderBySize < referenceByColumns.size(); i++) {
                String localIndexName = localIndex.get(i);
                String localOrderByName = referenceByColumns.get(matchOrderBySize);
                //TODO 没有匹配排序
                if (localOrderByName.equalsIgnoreCase(localIndexName)) {
                    if (startOffsetForOrderBy == -1) {
                        startOffsetForOrderBy = i;
                    }
                    matchOrderBySize++;
                } else if (startOffsetForOrderBy != -1) {
                    break;
                }
            }
            if (startOffsetForOrderBy == -1 || matchOrderBySize != referenceByColumns.size()) {
                continue;
            }

            //make sure the prefix columns of the equal set are in the index
            boolean returnFlag = true;
            for (int i = 0; i < startOffsetForOrderBy; i++) {
                if (equalSetColumns.contains(localIndex.get(i))) {
                    continue;
                } else {
                    if (inValuesColumns.contains(localIndex.get(i))) {
                        matchInIndexs.add(inValuesColumns.indexOf(localIndex.get(i)));
                    } else {
                        returnFlag = false;
                        break;
                    }
                }
            }
            if (!returnFlag) {
                continue;
            }

            if (!matchInIndexs.isEmpty()) {
                maxMatchInSize = matchInIndexs.size() > maxMatchInSize ? matchInIndexs.size() : maxMatchInSize;
                rets.add(new Pair<>(indexMeta, matchInIndexs));
            }
        }

        if (!rets.isEmpty()) {
            for (Pair<IndexMeta, Set<Integer>> pair : rets) {
                if (pair.getValue().size() == maxMatchInSize) {
                    return pair;
                }
            }
        }
        return null;
    }

    private void forceIndex(LogicalView logicalView, IndexMeta targetIndexMeta) {
        // logicalView should have no index hint
        RelNode rel = logicalView.getPushedRelNode();
        TableScanFinder tableScanFinder = new TableScanFinder();
        rel.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
        TableScan tableScan = tableScans.get(0).getValue();
        if (hasIndexHint(tableScan)) {
            //logicalView has force index
        } else {
            String indexName = targetIndexMeta.getPhysicalIndexName();
            logicalView.getPushDownOpt().autoForceIndex(indexName);
        }
    }

    public RelCollation sortProjectTranspose(Sort sort, Project project) {
        RelCollation newCollation = null;
        final RelOptCluster cluster = project.getCluster();
        // Determine mapping between project input and output fields. If sort
        // relies on non-trivial expressions, we can't push.
        final Mappings.TargetMapping map = RelOptUtil.permutationIgnoreCast(project.getProjects(),
            project.getInput().getRowType());
        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return newCollation;
            }
        }
        // Build
        newCollation = cluster.traitSet().canonize(RexUtil.apply(map, sort.getCollation()));
        return newCollation;
    }

}
