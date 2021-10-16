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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.mergeProject;
import static org.apache.calcite.rel.rules.JoinProjectTransposeRule.createProjectExprs;
import static org.apache.calcite.rel.rules.JoinPushThroughJoinRule.split;

/**
 * push join through table lookup
 *
 * @author chenmo.cm
 */
public class JoinTableLookupTransposeRule extends PushJoinRule {

    public static final Predicate<LogicalTableLookup> INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW =
        new PredicateImpl<LogicalTableLookup>() {
            @Override
            public boolean test(LogicalTableLookup tableLookup) {
                return tableLookup.getJoin().getJoinType() == JoinRelType.INNER &&
                    tableLookup.getJoin().getRight() instanceof LogicalView &&
                    tableLookup.getTraitSet().simplify().getTrait(RelCollationTraitDef.INSTANCE).isTop();
            }
        };

    public static final JoinTableLookupTransposeRule INSTANCE_LEFT = new JoinTableLookupTransposeRule(
        "JoinTableLookupTransposeRule:left",
        true,
        false,
        LogicalJoin.class);

    public static final JoinTableLookupTransposeRule INSTANCE_RIGHT = new JoinTableLookupTransposeRule(
        "JoinTableLookupTransposeRule:right",
        false,
        true,
        LogicalJoin.class);

    public static final JoinTableLookupTransposeRule INSTANCE_BOTH = new JoinTableLookupTransposeRule(
        "JoinTableLookupTransposeRule:both",
        true,
        true,
        LogicalJoin.class);

    /**
     * <pre>
     *
     * left: false
     * right: true
     *
     *           Join
     *           /  \
     *      RelNode  TableLookup
     *                /  \
     *        IndexScan  LogicalView
     *
     * -----------------------------
     * left: true
     * right: false
     *
     *              Join
     *              /  \
     *    TableLookup  RelNode
     *         /  \
     * IndexScan  LogicalView
     *
     * -----------------------------
     * left: true
     * right true
     *
     *           Join
     *           /  \
     * TableLookup  TableLookup
     *   /  \           /  \
     * IS   LV        IS   LV
     *
     * </pre>
     */
    protected boolean right;
    protected boolean left;

    public JoinTableLookupTransposeRule(String description, boolean left, boolean right, Class<? extends Join> clazz) {
        super(
            operand(clazz,
                left ? operand(LogicalTableLookup.class, null, INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                    operand(LogicalIndexScan.class, none())) : operand(RelNode.class, none()),
                right ? operand(LogicalTableLookup.class, null, INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                    operand(LogicalIndexScan.class, none())) : operand(RelNode.class, none())),
            description);
        this.left = left;
        this.right = right;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);

        RexNode joinCondition = join.getCondition();
        if (joinCondition.isAlwaysTrue() || joinCondition.isAlwaysFalse()) {
            return;
        }

        if (this.left && this.right) {
            onMatchBoth(call);
        } else if (this.right) {
            onMatchRight(call);
        } else {
            onMatchLeft(call);
        }
    }

    /**
     * <pre>
     *
     *               join
     *               /  \
     *    tableLookup1  tableLookup2
     *      /    \         /    \
     *    is1    lv1     is2    lv2
     *
     *
     * -----------------------------
     * becomes
     *
     *           tableLookup2
     *              /  \
     *   tableLookup1  lv2
     *       /  \
     *    join  lv1
     *    /  \
     *  is1  is2
     *
     * </pre>
     *
     * @param call rule call
     */
    private void onMatchBoth(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        final LogicalTableLookup tableLookup1 = call.rel(1);
        final LogicalIndexScan is1 = call.rel(2);
        final LogicalView lv1 = (LogicalView) tableLookup1.getJoin().getRight();
        final LogicalTableLookup tableLookup2 = call.rel(3);
        final LogicalIndexScan is2 = call.rel(4);
        final LogicalView lv2 = (LogicalView) tableLookup2.getJoin().getRight();

        // join project transpose
        final Project leftProj = tableLookup1.getProject();
        final LogicalJoin leftJoinChild = tableLookup1.getJoin();
        final Project rightProj = tableLookup2.getProject();
        final LogicalJoin rightJoinChild = tableLookup2.getJoin();

        /**
         * <pre>
         *
         *               join
         *               /  \
         *        leftProj  rightProj
         *           |          |
         *  leftJoinChild  rightJoinChild
         *
         * </pre>
         */
        final JoinProjectTranspose joinProjectTranspose = new JoinProjectTranspose(call,
            join,
            leftProj,
            rightProj,
            leftJoinChild,
            rightJoinChild).invoke();

        if (joinProjectTranspose.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *            topProject
         *                |
         *            newJoinRel
         *              /     \
         *   leftJoinChild   rightJoinChild
         *
         * </pre>
         */
        final LogicalJoin newJoinRel = joinProjectTranspose.getNewJoinRel();
        final Project topProject = joinProjectTranspose.getNewProjectRel();

        final JoinExchange joinExchange = new JoinExchange(call,
            newJoinRel,
            leftJoinChild,
            rightJoinChild,
            is1,
            lv1,
            is2,
            lv2).exchange();

        if (joinExchange.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *                 topProject
         *                     |
         *                bottomProject
         *                     |
         *                 newTopJoin2
         *                  /     \
         *           newTopJoin1  lv2
         *            /     \
         *   newBottomJoin  lv1
         *      /    \
         *    is1   is2
         *
         * </pre>
         */
        final LogicalJoin newBottomJoin = joinExchange.getNewBottomJoin();
        final RexNode newTopCondition1 = joinExchange.getNewTopCondition1();
        final Project bottomProject = joinExchange.getNewProject();
        LogicalJoin newTopJoin2 = joinExchange.getNewTopJoin2();

        // merge topProject with project generate by join transpose
        final List<RexNode> newProjects = PlannerUtils.mergeProject(topProject, bottomProject, call);
        Project newTopProject = topProject
            .copy(topProject.getTraitSet(), bottomProject.getInput(), newProjects, topProject.getRowType());

        /**
         * <pre>
         *
         * becomes
         *
         *                newTopProject
         *                     |
         *                 newTopJoin2
         *                  /     \
         *           newTopJoin1  lv2
         *            /     \
         *   newBottomJoin  lv1
         *      /    \
         *    is1   is2
         *
         * </pre>
         */
        ProjectJoinTranspose projectJoinTranspose = new ProjectJoinTranspose(call, newTopProject, newTopJoin2).invoke();
        if (projectJoinTranspose.isSkipped()) {
            return;
        }
        /**
         * <pre>
         *
         * becomes
         *
         *                 newTopProject *
         *                       |
         *                  newTopJoin2 *
         *                    /     \
         *           leftProjRel   rightProjRel
         *                |             |
         *           newTopJoin1       lv2
         *            /     \
         *   newBottomJoin  lv1
         *      /    \
         *    is1   is2
         *
         * </pre>
         */
        newTopProject = projectJoinTranspose.getNewTopProject();
        newTopJoin2 = projectJoinTranspose.getNewJoin();
        final Project leftProjRel = projectJoinTranspose.getLeftProjRel();
        final Project rightProjRel = projectJoinTranspose.getRightProjRel();

        // build tableLookup
        boolean isRelPushedToPrimary1 = isRelPushedToPrimary(tableLookup1, newBottomJoin, lv1, newTopCondition1);

        newBottomJoin.getJoinReorderContext().avoidParticipateInJoinReorder();

        final LogicalTableLookup newTableLookup1 = tableLookup1.copy(
            JoinRelType.INNER,
            newTopCondition1,
            leftProjRel.getProjects(),
            leftProjRel.getRowType(),
            newBottomJoin,
            lv1,
            isRelPushedToPrimary1);

        final RelNode primary2 = Optional.ofNullable(rightProjRel)
            .map(proj -> proj.copy(proj.getTraitSet(), ImmutableList.of(lv2)))
            .orElse(lv2);
        final RexNode newTopCondition2 = newTopJoin2.getCondition();

        boolean isRelPushedToPrimary2 = isRelPushedToPrimary(tableLookup2, newTableLookup1, primary2, newTopCondition2);

        final LogicalTableLookup newTableLookup2 = tableLookup2.copy(
            JoinRelType.INNER,
            newTopCondition2,
            newTopProject.getProjects(),
            newTopProject.getRowType(),
            newTableLookup1,
            primary2,
            isRelPushedToPrimary2);
        call.transformTo(newTableLookup2);
    }

    /**
     * <pre>
     *
     *              join
     *              /  \
     *    tableLookup  RelNode
     *      /    \
     *  index    primary
     *
     *
     * -----------------------------
     * becomes
     *
     *      tableLookup
     *         /  \
     *      join  primary
     *      /  \
     *  index  RelNode
     *
     * </pre>
     *
     * @param call rule call
     */
    private void onMatchLeft(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        final LogicalTableLookup tableLookup = call.rel(1);
        final LogicalIndexScan index = call.rel(2);
        final LogicalView primary = (LogicalView) tableLookup.getJoin().getRight();

        // join project transpose, project left
        final Project leftProj = tableLookup.getProject();
        final RelNode leftJoinChild = tableLookup.getJoin();
        final RelNode rightJoinChild = join.getRight();

        /**
         * <pre>
         *
         *            join
         *            /  \
         *     leftProj  rightJoinChild
         *        |
         *  leftJoinChild
         *
         * </pre>
         */
        final JoinProjectTranspose joinProjectTranspose = new JoinProjectTranspose(call,
            join,
            leftProj,
            null,
            leftJoinChild,
            rightJoinChild).invoke();

        if (joinProjectTranspose.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *            topProject
         *                |
         *            newJoinRel
         *              /     \
         *   leftJoinChild   rightJoinChild
         *
         * </pre>
         */
        final LogicalJoin newJoinRel = joinProjectTranspose.getNewJoinRel();
        final Project topProject = joinProjectTranspose.getNewProjectRel();

        // join join transpose
        final LogicalJoin bottomJoin = tableLookup.getJoin();

        /**
         * <pre>
         *
         *           topProject
         *               |
         *           newJoinRel
         *            /     \
         *    bottomJoin  rightJoinChild
         *      /    \
         *  index   primary
         *
         * </pre>
         */
        JoinPushLeftThroughJoin joinPushLeftThroughJoin = new JoinPushLeftThroughJoin(call,
            newJoinRel,
            bottomJoin,
            index,
            primary,
            rightJoinChild).left();

        if (joinPushLeftThroughJoin.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *           topProject
         *               |
         *          bottomProject
         *               |
         *           newTopJoin
         *            /     \
         *   newBottomJoin  primary
         *      /    \
         *  index   rightJoinChild
         *
         * </pre>
         */
        LogicalJoin newBottomJoin = joinPushLeftThroughJoin.getNewBottomJoin();
        RexNode newTopCondition = joinPushLeftThroughJoin.getNewTopCondition();
        Project bottomProject = joinPushLeftThroughJoin.getNewProject();

        newBottomJoin.getJoinReorderContext().avoidParticipateInJoinReorder();

        // merge topProject with project generate by join transpose
        final List<RexNode> newProjects = PlannerUtils.mergeProject(topProject, bottomProject, call);

        perform(call, tableLookup, JoinRelType.INNER, newTopCondition, newProjects, topProject.getRowType(),
            newBottomJoin, primary);
    }

    /**
     * <pre>
     *
     *            join
     *            /  \
     *      RelNode  tableLookup
     *                 /    \
     *             index    primary
     *
     *
     * -----------------------------
     * becomes
     *
     *            tableLookup
     *               /  \
     *            join  primary
     *            /  \
     *      RelNode  index
     *
     * </pre>
     *
     * @param call rule call
     */
    private void onMatchRight(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        final RelNode relnode = call.rel(1);
        final LogicalTableLookup tableLookup = call.rel(2);
        final LogicalIndexScan index = call.rel(3);
        final LogicalView primary = (LogicalView) tableLookup.getJoin().getRight();

        // join project transpose, project right
        final Project rightProj = tableLookup.getProject();
        final RelNode rightJoinChild = tableLookup.getJoin();
        final RelNode leftJoinChild = join.getLeft();

        /**
         * <pre>
         *
         *              join
         *              /  \
         *  leftJoinChild  rightProj
         *                     |
         *                rightJoinChild
         *
         * </pre>
         */
        final JoinProjectTranspose joinProjectTranspose = new JoinProjectTranspose(call,
            join,
            null,
            rightProj,
            leftJoinChild,
            rightJoinChild).invoke();

        if (joinProjectTranspose.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *            topProject
         *                |
         *            newJoinRel
         *              /     \
         *   leftJoinChild   rightJoinChild
         *
         * </pre>
         */
        final LogicalJoin newJoinRel = joinProjectTranspose.getNewJoinRel();
        final Project topProject = joinProjectTranspose.getNewProjectRel();

        // join join transpose
        final LogicalJoin bottomJoin = tableLookup.getJoin();

        /**
         * <pre>
         *
         *           topProject
         *               |
         *           newJoinRel
         *            /     \
         *  leftJoinChild   bottomJoin
         *                   /    \
         *               index   primary
         *
         * </pre>
         */
        JoinPushRightThroughJoin joinPushRightThroughJoin = new JoinPushRightThroughJoin(newJoinRel,
            bottomJoin,
            leftJoinChild,
            index,
            primary, tableLookup.isRelPushedToPrimary()).left();

        if (joinPushRightThroughJoin.isSkipped()) {
            return;
        }

        /**
         * <pre>
         *
         * becomes
         *
         *                topProject
         *                    |
         *                newTopJoin
         *                 /     \
         *        newBottomJoin  primary
         *            /    \
         *  leftJoinChild  index
         *
         * </pre>
         */
        final LogicalJoin newBottomJoin = joinPushRightThroughJoin.getNewBottomJoin();
        final RexNode newTopCondition = joinPushRightThroughJoin.getNewTopCondition();
        final JoinRelType newTopJoinType = joinPushRightThroughJoin.getNewTopJoinType();
        // Avoid the join had transposed tablelookup continuing participating join reorder
        // JoinTableLookupTransposeRule is supposed to support gsi as join index which means join index table and
        // then lookup primary table is the plan we want.
        // Further join reorder might explore a better plan, but the search space would become unbearable large.
        newBottomJoin.getJoinReorderContext().avoidParticipateInJoinReorder();

        perform(call,
            tableLookup,
            newTopJoinType,
            newTopCondition,
            topProject.getProjects(),
            topProject.getRowType(),
            newBottomJoin,
            primary);
    }

    @Override
    protected void getShardColumnRef(RelOptRuleCall call, LogicalView leftView,
                                     String leftTable, TableRule ltr, LogicalView rightView,
                                     String rightTable, TableRule rtr, List<Integer> lShardColumnRef,
                                     List<Integer> rShardColumnRef) {
        final LogicalTableLookup tlLeft = this.left ? call.rel(1) : null;
        final LogicalTableLookup tlRight = this.right ? call.rel(this.left ? 3 : 2) : null;
        final LogicalProject pLeft = this.left ? tlLeft.getProject() : null;
        final LogicalProject pRight = this.right ? tlRight.getProject() : null;

        if (null != lShardColumnRef) {
            final List<Integer> shardColumnRef = getRefByColumnName(leftView, leftTable, ltr.getShardColumns(), true);
            if (this.left) {
                /**
                 * <pre>
                 *                Join
                 *                /  \
                 *          project  rightView
                 *            |
                 *           join
                 *           /  \
                 *  (leftView)  LogicalView
                 * </pre>
                 */
                final List<Integer> inverseMap = RelUtils.inverseMap(pLeft.getInput().getRowType().getFieldCount(),
                    pLeft.getChildExps());

                shardColumnRef.forEach(ref -> {
                    if (inverseMap.get(ref) >= 0) {
                        lShardColumnRef.add(inverseMap.get(ref));
                    }
                });
            } else {
                /**
                 * <pre>
                 *           Join
                 *           /  \
                 *  (leftView)  project
                 *                |
                 *               join
                 *               /  \
                 *       rightView  LogicalView
                 * </pre>
                 */
                lShardColumnRef.addAll(shardColumnRef);
            }
        }

        if (null != rShardColumnRef) {
            final List<Integer> shardColumnRef =
                getRefByColumnName(rightView, rightTable, rtr.getShardColumns(), false);
            final int leftCount = this.left ? pLeft.getRowType().getFieldCount() : leftView.getRowType()
                .getFieldCount();

            if (this.right) {
                /**
                 * <pre>
                 *         Join
                 *         /  \
                 *  leftView  project
                 *              |
                 *             join
                 *             /  \
                 *   (rightView)  LogicalView
                 * </pre>
                 */
                final List<Integer> inverseMap = RelUtils.inverseMap(pRight.getInput().getRowType().getFieldCount(),
                    pRight.getChildExps());

                shardColumnRef.forEach(ref -> {
                    if (inverseMap.get(ref) >= 0) {
                        rShardColumnRef.add(leftCount + inverseMap.get(ref));
                    }
                });
            } else {
                /**
                 * <pre>
                 *              Join
                 *              /  \
                 *        project  (rightView)
                 *          |
                 *         join
                 *         /  \
                 *  leftView  LogicalView
                 * </pre>
                 */
                for (int rColRef : shardColumnRef) {
                    rShardColumnRef.add(leftCount + rColRef);
                }
            }
        }
    }

    private void perform(RelOptRuleCall call, LogicalTableLookup oldTableLookup, JoinRelType newTopJoinType,
                         RexNode newTopCondition, List<RexNode> newProjects, RelDataType newProjectRowType,
                         Join newBottomJoin, LogicalView primary) {
        boolean isRelPushedToPrimary = isRelPushedToPrimary(oldTableLookup, newBottomJoin, primary, newTopCondition);

        final LogicalTableLookup newTableLookup = oldTableLookup.copy(newTopJoinType, newTopCondition,
            newProjects,
            newProjectRowType,
            newBottomJoin,
            primary,
            isRelPushedToPrimary);
        call.transformTo(newTableLookup);
    }

    /**
     * check whether join condition contains reference of columns in primary table
     *
     * @param originTableLookup original originTableLookup
     * @param leftChild left child of tableLookup
     * @param primary primary table
     * @param tableLookupCondition new condition
     */
    public static boolean isRelPushedToPrimary(LogicalTableLookup originTableLookup, RelNode leftChild, RelNode primary,
                                               RexNode tableLookupCondition) {
        return originTableLookup.isRelPushedToPrimary()
            || withNontrivialPrimaryRef(originTableLookup, leftChild, primary, tableLookupCondition);
    }

    private static boolean withNontrivialPrimaryRef(LogicalTableLookup originTableLookup, RelNode leftChild,
                                                    RelNode primary,
                                                    RexNode tableLookupCondition) {
        final int lFieldCount = leftChild.getRowType().getFieldCount();
        final int rFieldCount = primary.getRowType().getFieldCount();
        final RelMetadataQuery mq = originTableLookup.getCluster().getMetadataQuery();
        final List<Set<RelColumnOrigin>> lFieldNames = mq.getColumnOriginNames(leftChild);
        final List<Set<RelColumnOrigin>> rFieldNames = mq.getColumnOriginNames(primary);

        final ImmutableBitSet bitSetPrimary = ImmutableBitSet.range(lFieldCount,
            lFieldCount + rFieldCount);
        for (RexNode condition : RelOptUtil.conjunctions(tableLookupCondition)) {
            final ImmutableBitSet conditionBitSet = InputFinder.bits(condition);
            if (conditionBitSet.intersects(bitSetPrimary)) {
                if (condition instanceof RexCall &&
                    (condition.getKind() == SqlKind.EQUALS || condition.getKind() == SqlKind.IS_NOT_DISTINCT_FROM)) {
                    final RexCall call = (RexCall) condition;
                    int l = pos(call.operands.get(0));
                    int r = pos(call.operands.get(1));

                    if (l > r) {
                        int tmp = l;
                        l = r;
                        r = tmp;
                    }

                    r = r - lFieldCount;

                    if (l > -1 && r > -1 && lFieldCount > l && rFieldCount > r
                        && lFieldNames.get(l).size() == 1 && rFieldNames.get(r).size() == 1) {
                        final RelOptTable indexTable = originTableLookup.getIndexTable();
                        final RelOptTable primaryTable = originTableLookup.getPrimaryTable();
                        final RelColumnOrigin lColumnOrigin = lFieldNames.get(l).iterator().next();
                        final RelColumnOrigin rColumnOrigin = rFieldNames.get(r).iterator().next();

                        if (indexTable.equals(lColumnOrigin.getOriginTable())
                            && primaryTable.equals(rColumnOrigin.getOriginTable())
                            && TStringUtil.equals(lColumnOrigin.getColumnName(), rColumnOrigin.getColumnName())) {
                            // skip condition for identical column
                            continue;
                        }
                    }
                }

                return true;
            }
        }
        return false;
    }

    private static int pos(RexNode expr) {
        if (expr instanceof RexInputRef) {
            return ((RexInputRef) expr).getIndex();
        }
        return -1;
    }

    /**
     * <pre>
     *
     *              join
     *              /  \
     *      leftProj   rightProj
     *         |           |
     *  leftJoinChild  rightJoinChild
     *
     * -----------------------------
     * becomes
     *
     *          newProjectRel
     *               |
     *           newJoinRel
     *             /    \
     *  leftJoinChild  rightJoinChild
     *
     * </pre>
     */
    private static class JoinProjectTranspose {
        private RelOptRuleCall call;
        private LogicalJoin join;
        private Project leftProj;
        private Project rightProj;
        private RelNode leftJoinChild;
        private RelNode rightJoinChild;
        private JoinRelType joinType;
        private LogicalJoin newJoinRel;
        private Project newProjectRel;
        private boolean skipped;

        public JoinProjectTranspose(RelOptRuleCall call, LogicalJoin join, Project leftProj, Project rightProj,
                                    RelNode leftJoinChild,
                                    RelNode rightJoinChild) {
            this.call = call;
            this.join = join;
            this.leftProj = leftProj;
            this.rightProj = rightProj;
            this.leftJoinChild = leftJoinChild;
            this.rightJoinChild = rightJoinChild;
            this.joinType = join.getJoinType();
        }

        public LogicalJoin getNewJoinRel() {
            return newJoinRel;
        }

        public Project getNewProjectRel() {
            return newProjectRel;
        }

        public boolean isSkipped() {
            return skipped;
        }

        public JoinProjectTranspose invoke() {
            if (leftProj != null && joinType.generatesNullsOnLeft() && !Strong.allStrong(leftProj.getProjects())) {
                skipped = true;
                return this;
            }

            if (rightProj != null && joinType.generatesNullsOnRight() && !Strong.allStrong(rightProj.getProjects())) {
                skipped = true;
                return this;
            }

            // Construct two RexPrograms and combine them.  The bottom program
            // is a join of the projection expressions from the left and/or
            // right projects that feed into the join.  The top program contains
            // the join condition.

            // Create a row type representing a concatenation of the inputs
            // underneath the projects that feed into the join.  This is the input
            // into the bottom RexProgram.  Note that the join type is an inner
            // join because the inputs haven't actually been joined yet.
            RelDataType joinChildrenRowType =
                SqlValidatorUtil.deriveJoinRowType(
                    leftJoinChild.getRowType(),
                    rightJoinChild.getRowType(),
                    JoinRelType.INNER,
                    join.getCluster().getTypeFactory(),
                    null,
                    Collections.emptyList());

            // Create projection expressions, combining the projection expressions
            // from the projects that feed into the join.  For the RHS projection
            // expressions, shift them to the right by the number of fields on
            // the LHS.  If the join input was not a projection, simply create
            // references to the inputs.
            int nProjExprs = join.getRowType().getFieldCount();
            final List<Pair<RexNode, String>> projects = new ArrayList<>();
            final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

            createProjectExprs(leftProj, leftJoinChild, 0, rexBuilder, joinChildrenRowType.getFieldList(), projects);

            final int nFieldsLeft = leftJoinChild.getRowType().getFieldCount();
            createProjectExprs(rightProj,
                rightJoinChild,
                nFieldsLeft,
                rexBuilder,
                joinChildrenRowType.getFieldList(),
                projects);

            final List<RelDataType> projTypes = new ArrayList<>();
            for (int i = 0; i < nProjExprs; i++) {
                projTypes.add(projects.get(i).left.getType());
            }
            RelDataType projRowType = rexBuilder.getTypeFactory().createStructType(projTypes, Pair.right(projects));

            // create the RexPrograms and merge them
            final RexProgram bottomProgram = RexProgram
                .create(joinChildrenRowType, Pair.left(projects), null, projRowType, rexBuilder);
            final RexProgramBuilder topProgramBuilder = new RexProgramBuilder(projRowType, rexBuilder);

            topProgramBuilder.addIdentity();
            topProgramBuilder.addCondition(join.getCondition());
            RexProgram topProgram = topProgramBuilder.getProgram();
            RexProgram mergedProgram = RexProgramBuilder.mergePrograms(topProgram, bottomProgram, rexBuilder);

            // expand out the join condition and construct a new LogicalJoin that
            // directly references the join children without the intervening
            // ProjectRels
            RexNode newCondition = mergedProgram.expandLocalRef(mergedProgram.getCondition());
            newJoinRel = join.copy(join
                .getTraitSet(), newCondition, leftJoinChild, rightJoinChild, join.getJoinType(), join.isSemiJoinDone());

            // expand out the new projection expressions; if the join is an
            // outer join, modify the expressions to reference the join output
            final List<RexNode> newProjExprs = new ArrayList<>();
            List<RexLocalRef> projList = mergedProgram.getProjectList();
            List<RelDataTypeField> newJoinFields = newJoinRel.getRowType().getFieldList();
            int nJoinFields = newJoinFields.size();
            int[] adjustments = new int[nJoinFields];
            for (int i = 0; i < nProjExprs; i++) {
                RexNode newExpr = mergedProgram.expandLocalRef(projList.get(i));
                if (joinType.isOuterJoin()) {
                    newExpr = newExpr.accept(new RelOptUtil.RexInputConverter(rexBuilder,
                        joinChildrenRowType.getFieldList(),
                        newJoinFields,
                        adjustments));
                }
                newProjExprs.add(newExpr);
            }

            // finally, create the projection on top of the join
            RelBuilder relBuilder = call.builder();
            relBuilder.push(newJoinRel);
            relBuilder.project(newProjExprs, join.getRowType().getFieldNames());
            // if the join was outer, we might need a cast after the
            // projection to fix differences wrt nullability of fields
            if (joinType.isOuterJoin()) {
                relBuilder.convert(join.getRowType(), false);
            }

            this.newProjectRel = (Project) relBuilder.build();
            skipped = false;
            return this;
        }
    }

    /**
     * <pre>
     *        topJoin
     *        /     \
     *  bottomJoin   C
     *    /    \
     *   A      B
     *
     * -----------------------------
     * becomes
     *
     *         newProject
     *              |
     *         newTopJoin
     *           /     \
     *  newBottomJoin   B
     *     /    \
     *    A      C
     * </pre>
     */
    private static class JoinPushLeftThroughJoin {
        private boolean skipped;
        private RelOptRuleCall call;
        private LogicalJoin topJoin;
        private LogicalJoin bottomJoin;
        private RelNode relC;
        private RelNode relA;
        private RelNode relB;
        private LogicalJoin newBottomJoin;
        private RexNode newTopCondition;
        private Project newProject;

        public JoinPushLeftThroughJoin(RelOptRuleCall call, LogicalJoin topJoin, LogicalJoin bottomJoin, RelNode relA,
                                       RelNode relB, RelNode relC) {
            this.call = call;
            this.topJoin = topJoin;
            this.bottomJoin = bottomJoin;
            this.relA = relA;
            this.relB = relB;
            this.relC = relC;
        }

        boolean isSkipped() {
            return skipped;
        }

        public LogicalJoin getNewBottomJoin() {
            return newBottomJoin;
        }

        public RexNode getNewTopCondition() {
            return newTopCondition;
        }

        public Project getNewProject() {
            return newProject;
        }

        /**
         * push to left side of bottom join
         */
        public JoinPushLeftThroughJoin left() {
            final RelOptCluster cluster = topJoin.getCluster();

            //        topJoin
            //        /     \
            //   bottomJoin  C
            //    /    \
            //   A      B

            final int aCount = relA.getRowType().getFieldCount();
            final int bCount = relB.getRowType().getFieldCount();
            final int cCount = relC.getRowType().getFieldCount();
            final ImmutableBitSet bBitSet =
                ImmutableBitSet.range(aCount, aCount + bCount);

            // becomes
            //
            //        newTopJoin
            //        /        \
            //   newBottomJoin  B
            //    /    \
            //   A      C

            // If top join is not inner or left outer, we cannot proceed.
            final EnumSet<JoinRelType> supportedTopJoinType = EnumSet.of(JoinRelType.INNER, JoinRelType.LEFT);
            if (!supportedTopJoinType.contains(topJoin.getJoinType())
                || bottomJoin.getJoinType() != JoinRelType.INNER) {
                skipped = true;
                return this;
            }

            // Split the condition of topJoin into a conjunction. Each of the
            // parts that does not use columns from B can be pushed down.
            final List<RexNode> intersecting = new ArrayList<>();
            final List<RexNode> nonIntersecting = new ArrayList<>();
            split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);

            // top.condition intersecting with B, abort
            if (topJoin.getJoinType() != JoinRelType.INNER && !intersecting.isEmpty()) {
                skipped = true;
                return this;
            }

            // If there's nothing to push down, it's not worth proceeding.
            if (nonIntersecting.isEmpty()) {
                skipped = true;
                return this;
            }

            // Split the condition of bottomJoin into a conjunction. Each of the
            // parts that use columns from B will need to be pulled up.
            final List<RexNode> bottomIntersecting = new ArrayList<>();
            final List<RexNode> bottomNonIntersecting = new ArrayList<>();
            split(bottomJoin.getCondition(), bBitSet, bottomIntersecting, bottomNonIntersecting);

            // target: | A       | C      |
            // source: | A       | B | C      |
            final Mappings.TargetMapping bottomMapping = Mappings
                .createShiftMapping(aCount + bCount + cCount,
                    0, 0, aCount,
                    aCount, aCount + bCount, cCount);
            final List<RexNode> newBottomList = new ArrayList<>();
            new RexPermuteInputsShuttle(bottomMapping, relA, relC).visitList(nonIntersecting, newBottomList);
            new RexPermuteInputsShuttle(bottomMapping, relA, relC).visitList(bottomNonIntersecting, newBottomList);
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode newBottomCondition = RexUtil.composeConjunction(rexBuilder, newBottomList, false);
            newBottomJoin = topJoin.copy(topJoin
                .getTraitSet(), newBottomCondition, relA, relC, topJoin.getJoinType(), topJoin.isSemiJoinDone());

            // target: | A       | C      | B |
            // source: | A       | B | C      |
            final Mappings.TargetMapping topMapping =
                Mappings.createShiftMapping(
                    aCount + bCount + cCount,
                    0, 0, aCount,
                    aCount + cCount, aCount, bCount,
                    aCount, aCount + bCount, cCount);
            final List<RexNode> newTopList = new ArrayList<>();
            new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB).visitList(intersecting, newTopList);
            new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB).visitList(bottomIntersecting, newTopList);
            newTopCondition = RexUtil.composeConjunction(rexBuilder, newTopList, false);
            final Join newTopJoin = bottomJoin.copy(bottomJoin.getTraitSet(),
                newTopCondition,
                newBottomJoin,
                relB,
                bottomJoin.getJoinType(),
                bottomJoin.isSemiJoinDone());

            assert !Mappings.isIdentity(topMapping);
            final RelBuilder relBuilder = call.builder();
            relBuilder.push(newTopJoin);
            relBuilder.project(relBuilder.fields(topMapping));
            newProject = (Project) relBuilder.build();
            skipped = false;
            return this;
        }
    }

    /**
     * <pre>
     *   topJoin
     *   /     \
     *  A   bottomJoin
     *       /    \
     *      B      C
     *
     * -----------------------------
     * becomes
     *
     *          newTopJoin
     *            /     \
     *  newBottomJoin    C
     *     /    \
     *    A      B
     * </pre>
     */
    private static class JoinPushRightThroughJoin {
        private boolean skipped;
        private LogicalJoin topJoin;
        private LogicalJoin bottomJoin;
        private RelNode relC;
        private RelNode relA;
        private RelNode relB;
        private LogicalJoin newBottomJoin;
        private RexNode newTopCondition;
        private JoinRelType newTopJoinType;
        private boolean isRelPushedToPrimary;

        public JoinPushRightThroughJoin(LogicalJoin topJoin, LogicalJoin bottomJoin, RelNode relA, RelNode relB,
                                        RelNode relC, boolean isRelPushedToPrimary) {
            this.topJoin = topJoin;
            this.bottomJoin = bottomJoin;
            this.relA = relA;
            this.relB = relB;
            this.relC = relC;
            this.isRelPushedToPrimary = isRelPushedToPrimary;
        }

        boolean isSkipped() {
            return skipped;
        }

        public LogicalJoin getNewBottomJoin() {
            return newBottomJoin;
        }

        public RexNode getNewTopCondition() {
            return newTopCondition;
        }

        public JoinRelType getNewTopJoinType() {
            return newTopJoinType;
        }

        /**
         * push to left side of bottom join
         */
        public JoinPushRightThroughJoin left() {
            final RelOptCluster cluster = topJoin.getCluster();

            //   topJoin
            //   /     \
            //  A    bottomJoin
            //        /    \
            //       B      C

            final int aCount = relA.getRowType().getFieldCount();
            final int bCount = relB.getRowType().getFieldCount();
            final int cCount = relC.getRowType().getFieldCount();

            // becomes
            //
            //        newTopJoin
            //        /        \
            //   newBottomJoin  C
            //    /    \
            //   A      B

            // If either join is not inner or right outer, we cannot proceed.

            Map<Pair<JoinRelType, JoinRelType>, Pair<JoinRelType, JoinRelType>> assocTable = new HashMap<>();
            assocTable.put(
                Pair.of(JoinRelType.INNER, JoinRelType.INNER), Pair.of(JoinRelType.INNER, JoinRelType.INNER));
            assocTable.put(
                Pair.of(JoinRelType.INNER, JoinRelType.RIGHT), Pair.of(JoinRelType.RIGHT, JoinRelType.INNER));
            if (!isRelPushedToPrimary) {
                assocTable.put(
                    Pair.of(JoinRelType.INNER, JoinRelType.LEFT), Pair.of(JoinRelType.LEFT, JoinRelType.LEFT));
            }
            Pair<JoinRelType, JoinRelType> newJoinTypePair =
                assocTable.get(Pair.of(bottomJoin.getJoinType(), topJoin.getJoinType()));
            if (newJoinTypePair == null) {
                skipped = true;
                return this;
            }

            final ImmutableBitSet cBitSetTop = ImmutableBitSet.range(aCount + bCount, aCount + bCount + cCount);
            // Split the condition of topJoin into a conjunction. Each of the
            // parts that does not use columns from C can be pushed down.
            final List<RexNode> intersecting = new ArrayList<>();
            final List<RexNode> nonIntersecting = new ArrayList<>();
            split(topJoin.getCondition(), cBitSetTop, intersecting, nonIntersecting);

            // top.condition intersecting with C, abort
            if (topJoin.getJoinType() != JoinRelType.INNER && !intersecting.isEmpty()) {
                skipped = true;
                return this;
            }

            // If there's nothing to push down, it's not worth proceeding.
            if (nonIntersecting.isEmpty()) {
                skipped = true;
                return this;
            }

            final ImmutableBitSet cBitSetBottom = ImmutableBitSet.range(bCount, bCount + cCount);
            // Split the condition of bottomJoin into a conjunction. Each of the
            // parts that use columns from C will need to be pulled up.
            final List<RexNode> bottomIntersecting = new ArrayList<>();
            final List<RexNode> bottomNonIntersecting = new ArrayList<>();
            split(bottomJoin.getCondition(), cBitSetBottom, bottomIntersecting, bottomNonIntersecting);

            // target: | A      | B | C      |
            // source: | B | C      |
            final Mappings.TargetMapping bottomMapping =
                Mappings.createShiftMapping(bCount + cCount, aCount, 0, bCount);
            final List<RexNode> newBottomList = new ArrayList<>(nonIntersecting);
            new RexPermuteInputsShuttle(bottomMapping, relA, relB).visitList(bottomNonIntersecting, newBottomList);
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode newBottomCondition = RexUtil.composeConjunction(rexBuilder, newBottomList, false);
            newBottomJoin = topJoin.copy(topJoin.getTraitSet(), newBottomCondition, relA,
                relB, newJoinTypePair.getKey(), topJoin.isSemiJoinDone());

            // target: | A       | B      | C |
            // source: | A       | B | C      |
            final Mappings.TargetMapping topMapping =
                Mappings.createShiftMapping(
                    aCount + bCount + cCount,
                    aCount, 0, bCount,
                    aCount + bCount, bCount, cCount);
            final List<RexNode> newTopList = new ArrayList<>(intersecting);
            new RexPermuteInputsShuttle(topMapping, newBottomJoin, relC).visitList(bottomIntersecting, newTopList);
            newTopCondition = RexUtil.composeConjunction(rexBuilder, newTopList, false);
            newTopJoinType = newJoinTypePair.getValue();
            skipped = false;
            return this;
        }
    }

    /**
     * <pre>
     *            topJoin
     *            /     \
     *  bottomJoin1   bottomJoin2
     *     /    \       /    \
     *    A      B     C      D
     *
     * -----------------------------
     * becomes
     *
     *                 newProject
     *                     |
     *                newTopJoin2
     *                  /     \
     *          newTopJoin1    D
     *            /     \
     *  newBottomJoin    B
     *     /    \
     *    A      C
     * </pre>
     */
    private static class JoinExchange {
        private boolean skipped;
        private LogicalJoin topJoin;
        private LogicalJoin bottomJoin1;
        private LogicalJoin bottomJoin2;
        private RelNode relA;
        private RelNode relB;
        private RelNode relC;
        private RelNode relD;
        private LogicalJoin newBottomJoin;
        private RexNode newTopCondition1;
        private LogicalJoin newTopJoin1;
        private LogicalJoin newTopJoin2;
        private Project newProject;
        private RelOptRuleCall call;

        public JoinExchange(RelOptRuleCall call, LogicalJoin topJoin, LogicalJoin bottomJoin1, LogicalJoin bottomJoin2,
                            RelNode relA, RelNode relB, RelNode relC, RelNode relD) {
            this.call = call;
            this.topJoin = topJoin;
            this.bottomJoin1 = bottomJoin1;
            this.bottomJoin2 = bottomJoin2;
            this.relA = relA;
            this.relB = relB;
            this.relC = relC;
            this.relD = relD;
        }

        boolean isSkipped() {
            return skipped;
        }

        public LogicalJoin getNewBottomJoin() {
            return newBottomJoin;
        }

        public RexNode getNewTopCondition1() {
            return newTopCondition1;
        }

        public RexNode getNewTopCondition2() {
            return getNewTopJoin2().getCondition();
        }

        public LogicalJoin getNewTopJoin1() {
            return newTopJoin1;
        }

        public LogicalJoin getNewTopJoin2() {
            return newTopJoin2;
        }

        public Project getNewProject() {
            return newProject;
        }

        /**
         * exchange relB and relC
         */
        public JoinExchange exchange() {
            final RelOptCluster cluster = topJoin.getCluster();

            //           topJoin
            //           /     \
            //  bottomJoin1    bottomJoin2
            //    /    \          /    \
            //   A      B        C      D

            final int aCount = relA.getRowType().getFieldCount();
            final int bCount = relB.getRowType().getFieldCount();
            final int cCount = relC.getRowType().getFieldCount();
            final int dCount = relD.getRowType().getFieldCount();

            // becomes
            //
            //              newTopJoin2
            //              /        \
            //          newTopJoin1   D
            //          /        \
            //   newBottomJoin    B
            //    /    \
            //   A      C

            // If either join is not inner or right outer, we cannot proceed.
            if (topJoin.getJoinType() != JoinRelType.INNER
                || bottomJoin1.getJoinType() != JoinRelType.INNER
                || bottomJoin2.getJoinType() != JoinRelType.INNER
            ) {
                skipped = true;
                return this;
            }

            // split top join and bottom join1 conditions
            final ImmutableBitSet bBitSetTop = ImmutableBitSet.range(aCount, aCount + bCount);
            final ImmutableBitSet dBitSetTop =
                ImmutableBitSet.range(aCount + bCount + cCount, aCount + bCount + cCount + dCount);
            final List<RexNode> newBottomConditions = new ArrayList<>();
            final List<RexNode> newTopJoin1Conditions = new ArrayList<>();
            final List<RexNode> newTopJoin2Conditions = new ArrayList<>();
            final List<RexNode> originConditions = new ArrayList<>(RelOptUtil.conjunctions(topJoin.getCondition()));
            originConditions.addAll(RelOptUtil.conjunctions(bottomJoin1.getCondition()));
            for (RexNode node : originConditions) {
                ImmutableBitSet inputBitSet = InputFinder.bits(node);
                if (dBitSetTop.intersects(inputBitSet)) {
                    newTopJoin2Conditions.add(node);
                } else if (bBitSetTop.intersects(inputBitSet)) {
                    newTopJoin1Conditions.add(node);
                } else {
                    newBottomConditions.add(node);
                }
            }

            // If there's nothing to push down, it's not worth proceeding.
            if (newBottomConditions.isEmpty()) {
                skipped = true;
                return this;
            }

            // target: | A       | C      | B | D     |
            // source: | A       | B | C      | D     |
            final TargetMapping topMapping =
                Mappings.createShiftMapping(
                    aCount + bCount + cCount + dCount,
                    0, 0, aCount,
                    aCount + cCount, aCount, bCount,
                    aCount, aCount + bCount, cCount,
                    aCount + bCount + cCount, aCount + bCount + cCount, dCount);
            final List<RexNode> newBottomList = new ArrayList<>();
            new RexPermuteInputsShuttle(topMapping, relA, relC).visitList(newBottomConditions, newBottomList);

            // split bottom join2 conditions
            final ImmutableBitSet dBitSetBottom = ImmutableBitSet.range(cCount, cCount + dCount);
            // Split the condition of bottomJoin into a conjunction. Each of the
            // parts that use columns from D will need to be pulled up.
            final List<RexNode> bottom2Intersecting = new ArrayList<>();
            final List<RexNode> bottom2NonIntersecting = new ArrayList<>();
            split(bottomJoin2.getCondition(), dBitSetBottom, bottom2Intersecting, bottom2NonIntersecting);

            // target: | A      | C      | B     | D     |
            // source: | C      | D      |
            final TargetMapping bottom2Mapping = Mappings.createShiftMapping(cCount + dCount + aCount + bCount,
                aCount, 0, cCount,
                aCount + cCount + bCount, cCount, dCount);
            new RexPermuteInputsShuttle(bottom2Mapping, relA, relC).visitList(bottom2NonIntersecting, newBottomList);

            // build bottom join
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode newBottomCondition = RexUtil.composeConjunction(rexBuilder, newBottomList, false);
            newBottomJoin = topJoin.copy(topJoin.getTraitSet(), newBottomCondition, relA,
                relC, topJoin.getJoinType(), topJoin.isSemiJoinDone());

            // build top join 1
            final List<RexNode> newTop1List = new ArrayList<>();
            new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB).visitList(newTopJoin1Conditions, newTop1List);
            this.newTopCondition1 = RexUtil.composeConjunction(rexBuilder, newTop1List, false);
            this.newTopJoin1 = bottomJoin1.copy(bottomJoin1.getTraitSet(),
                newTopCondition1,
                newBottomJoin,
                relB,
                bottomJoin1.getJoinType(),
                bottomJoin1.isSemiJoinDone());

            // build top join 2
            final List<RexNode> newTop2List = new ArrayList<>();
            new RexPermuteInputsShuttle(bottom2Mapping, newTopJoin1, relD).visitList(bottom2Intersecting, newTop2List);
            new RexPermuteInputsShuttle(topMapping, newTopJoin1, relD).visitList(newTopJoin2Conditions, newTop2List);
            RexNode newTopCondition2 = RexUtil.composeConjunction(rexBuilder, newTop2List, false);
            this.newTopJoin2 = bottomJoin2.copy(bottomJoin2.getTraitSet(),
                newTopCondition2,
                newTopJoin1,
                relD,
                bottomJoin2.getJoinType(),
                bottomJoin2.isSemiJoinDone());

            assert !Mappings.isIdentity(topMapping);
            final RelBuilder relBuilder = call.builder();
            relBuilder.push(newTopJoin2);
            relBuilder.project(relBuilder.fields(topMapping));
            newProject = (Project) relBuilder.build();

            skipped = false;
            return this;
        }
    }

}
