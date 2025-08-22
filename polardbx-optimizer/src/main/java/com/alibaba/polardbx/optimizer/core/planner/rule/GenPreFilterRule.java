package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

/**
 * A rule to generate pre-filter conditions for a query.
 * Pre-filter conditions are used to optimize query performance by filtering data at an early stage.
 */
public class GenPreFilterRule extends RelOptRule {

    public GenPreFilterRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final GenPreFilterRule INSTANCE = new GenPreFilterRule(
        operand(LogicalFilter.class, RelOptRule.any()), "gen_prefilter");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return !StringUtils.isEmpty(paramManager.getString(ConnectionParams.PREFILTER_COLUMNS));
    }

    public void onMatch(RelOptRuleCall call) {
        // Retrieve the filter relational expression matched by the rule
        LogicalFilter filter = (LogicalFilter) call.rels[0];
        // Decompose the filter condition into a list of conjunctions
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
        // Initialize lists for pre-filter conditions, sargable filters, and new conditions
        List<RexCall> preFilters = Lists.newArrayList();
        List<RexCall> sargableFilters = Lists.newArrayList();
        List<RexNode> newConditions = Lists.newArrayList();

        // Iterate through each condition to classify them
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall) {
                RexCall rexCall = (RexCall) condition;
                // Check if the condition is a pre-filter
                if (rexCall.getOperator() == TddlOperatorTable.PRE_FILTER) {
                    preFilters.add(rexCall);
                    continue;
                }
                newConditions.add(rexCall);
                // Check if the condition is sargable
                if (rexCall.getOperator().getKind().belongsTo(SqlKind.SARGABLE)) {
                    sargableFilters.add(rexCall);
                }
            }
        }

        // don't generate prefilter when there is prefilter already
        if (!preFilters.isEmpty()) {
            return;
        }

        // Retrieve the metadata query interface for further information retrieval
        RelMetadataQuery mq = call.getMetadataQuery();
        // Get the table references in the filter, only proceed if there is exactly one table reference
        Set<RexTableInputRef.RelTableRef> tableRefs = mq.getTableReferences(filter);
        if (tableRefs == null || tableRefs.size() != 1) {
            return;
        }

        // Retrieve the list of columns eligible for pre-filtering from the planner context
        String preFilterColumns = PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getString(ConnectionParams.PREFILTER_COLUMNS);
        Set<String> preFilterColumnList = Arrays.stream(preFilterColumns.split(",")).map(SQLUtils::normalizeNoTrim)
            .filter(x -> !StringUtils.isEmpty(x)).map(String::toLowerCase).collect(Collectors.toSet());

        // Initialize a set to store the candidate columns for pre-filtering
        Set<Integer> resultPrefilter = Sets.newHashSet();
        PlannerContext pc = PlannerContext.getPlannerContext(filter);
        // Evaluate each sargable filter to determine if it can be used as a pre-filter
        for (RexCall sargableFilter : sargableFilters) {
            resultPrefilter.addAll(
                findCandidatePrefilter(sargableFilter,
                    index -> columnShouldPrefilter(filter, index, mq, preFilterColumnList),
                    pc));
        }

        // If no pre-filter columns are found, exit the method
        if (resultPrefilter.isEmpty()) {
            return;
        }

        // Retrieve the RexBuilder for constructing new relational expressions
        RexBuilder rb = call.builder().getRexBuilder();
        // Add the identified pre-filter conditions to the list of new conditions
        for (Integer ord : resultPrefilter) {
            newConditions.add(rb.makeCall(TddlOperatorTable.PRE_FILTER, RexInputRef.of(ord, filter.getRowType())));
        }
        // Transform the original filter relational expression, integrating the new conditions
        call.transformTo(
            filter.copy(filter.getTraitSet(), filter.getInput(), RexUtil.flatten(rb, rb.makeCall(AND, newConditions))));
    }

    /**
     * Finds candidate columns for pre-filtering.
     *
     * @param rexCall the RexCall to examine
     * @param function a function to determine if a column should be pre-filtered
     * @param pc the PlannerContext
     * @return a set of candidate columns for pre-filtering
     */
    private Set<Integer> findCandidatePrefilter(RexCall rexCall,
                                                Function<Integer, Boolean> function,
                                                PlannerContext pc) {
        Set<Integer> candidateColumns = Sets.newHashSet();
        RexNode leftRexNode;
        RexNode rightRexNode;
        Boolean originInfo;
        int inputRef;
        // Determine if the operator of rexCall is a comparison operator, and handle it accordingly
        switch (rexCall.getOperator().getKind()) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                // Comparison operations require two operands
                if (rexCall.getOperands().size() != 2) {
                    break;
                }
                leftRexNode = rexCall.getOperands().get(0);
                rightRexNode = rexCall.getOperands().get(1);
                // If the left operand is a column reference and the right operand is not, check if the column should be pre-filtered
                if (leftRexNode instanceof RexInputRef && !(rightRexNode instanceof RexInputRef)) {
                    inputRef = ((RexInputRef) leftRexNode).getIndex();
                    // If the right operand can be folded and the column should be pre-filtered, add it to the candidate set
                    if (DrdsRexFolder.fold(rightRexNode, pc) != null &&
                        (originInfo = function.apply(inputRef)) != null) {
                        if (originInfo) {
                            candidateColumns.add(((RexInputRef) leftRexNode).getIndex());
                        }
                    }
                } else if (rightRexNode instanceof RexInputRef && !(leftRexNode instanceof RexInputRef)) {
                    inputRef = ((RexInputRef) rightRexNode).getIndex();
                    // If the left operand can be folded and the column should be pre-filtered, add it to the candidate set
                    if (DrdsRexFolder.fold(leftRexNode, pc) != null &&
                        (originInfo = function.apply(inputRef)) != null) {
                        if (originInfo) {
                            candidateColumns.add(((RexInputRef) rightRexNode).getIndex());
                        }
                    }
                }
                break;
            case BETWEEN:
                // The BETWEEN operation requires three operands
                if (rexCall.getOperands().size() != 3) {
                    break;
                }
                leftRexNode = rexCall.getOperands().get(0);
                rightRexNode = rexCall.getOperands().get(1);
                RexNode thirdRexNode = rexCall.getOperands().get(2);
                // If the left operand is a column reference and the other two are not, check if the column should be pre-filtered
                if (leftRexNode instanceof RexInputRef &&
                    !(rightRexNode instanceof RexInputRef) &&
                    !(thirdRexNode instanceof RexInputRef)) {
                    inputRef = ((RexInputRef) leftRexNode).getIndex();
                    // If the other two operands can be folded and the column should be pre-filtered, add it to the candidate set
                    if (DrdsRexFolder.fold(rightRexNode, pc) != null &&
                        DrdsRexFolder.fold(thirdRexNode, pc) != null &&
                        (originInfo = function.apply(inputRef)) != null) {
                        if (originInfo) {
                            candidateColumns.add(((RexInputRef) leftRexNode).getIndex());
                        }
                    }
                }
                break;
            default:
                // do nothing
        }
        return candidateColumns;
    }

    /**
     * Determines if a column should be pre-filtered.
     *
     * @param filter the LogicalFilter
     * @param ref the column reference
     * @param mq the RelMetadataQuery
     * @param preFilterColumnList the list of columns to be pre-filtered
     * @return true if the column should be pre-filtered, false otherwise
     */
    private static boolean columnShouldPrefilter(LogicalFilter filter, Integer ref, RelMetadataQuery mq,
                                                 Set<String> preFilterColumnList) {
        String columnName = filter.getRowType().getFieldList().get(ref).getName();
        if (StringUtils.isEmpty(columnName)) {
            return false;
        }
        RelColumnOrigin origin = mq.getColumnOrigin(filter, ref);
        if (origin == null) {
            return false;
        }
        TableMeta tm = CBOUtil.getTableMeta(origin.getOriginTable());
        if (tm == null) {
            return false;
        }
        String schemaName = tm.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            return false;
        }
        String tableName = StatisticManager.getSourceTableName(schemaName, tm.getTableName());
        if (StringUtils.isEmpty(tableName)) {
            return false;
        }
        String preFilter = tm.getSchemaName() + "." + tableName + "." + columnName;
        return preFilterColumnList.contains(preFilter.toLowerCase());
    }
}
