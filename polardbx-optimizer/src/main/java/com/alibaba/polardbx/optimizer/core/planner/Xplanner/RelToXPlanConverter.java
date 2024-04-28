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

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class RelToXPlanConverter implements ReflectiveVisitor {

    private final static Logger logger = LoggerFactory.getLogger(RelToXPlanConverter.class);

    private static final ReflectUtil.MethodDispatcher<Result> dispatcher = ReflectUtil.createMethodDispatcher(
        Result.class, RelToXPlanConverter.class, "visit", RelNode.class);

    private final Deque<Frame> stack = new ArrayDeque<>();

    private final XPlanUtil util = new XPlanUtil();

    // Set true to remove the TableProject before TableScan.
    private boolean isTableIdentical = !XConnectionManager.getInstance().isEnableXplanExpendStar();
    // Counter of generated get plan. This is used to evaluate the optimization of XPlan.
    private int getPlanCount = 0;

    public boolean isTableIdentical() {
        return isTableIdentical;
    }

    public void setTableIdentical(boolean tableIdentical) {
        isTableIdentical = tableIdentical;
    }

    public int getGetPlanCount() {
        return getPlanCount;
    }

    public void setGetPlanCount(int getPlanCount) {
        this.getPlanCount = getPlanCount;
    }

    protected Result dispatch(RelNode e) {
        return dispatcher.invoke(this, e);
    }

    public XPlanTemplate convert(RelNode e) {
        Result result = visitChild(0, e);
        if (null == result) {
            return null;
        }

        RelXPlanOptimizer.IndexFinder indexFinder = new RelXPlanOptimizer.IndexFinder();
        indexFinder.go(e);
        if (!indexFinder.found()) {
            return null;
        }
        return new XPlanTemplate(result.getAnyPlan(),
            ImmutableList.copyOf(util.getTableNames()),
            ImmutableList.copyOf(util.getParamInfos()),
            indexFinder.getIndex());
    }

    public Result visitChild(int i, RelNode e) {
        try {
            stack.push(new Frame(i, e));
            return dispatch(e).setStack(stack);
        } finally {
            stack.pop();
        }
    }

    public Result visit(RelNode e) {
        throw new AssertionError("Need to implement " + e.getClass().getName());
    }

    public Result visit(XPlanTableScan e) {
        return new Result(util.convert(e, isTableIdentical));
    }

    public Result visit(TableScan e) {
        throw GeneralUtil.nestedException("Never get here cause we use XPlan optimizer.");
    }

    public Result visit(Filter e) {
        Result sub = visitChild(0, e.getInput());
        if (null == sub) {
            return null;
        }
        return new Result(XPlanUtil.genFilter(sub.getAnyPlan(), util.rexToExpr(e.getCondition())));
    }

    public Result visit(Project e) {
        Result sub = visitChild(0, e.getInput());
        if (null == sub) {
            return null;
        }
        return new Result(XPlanUtil.genProject(sub.getAnyPlan(),
            e.getRowType().getFieldNames(),
            e.getChildExps().stream()
                .map(util::rexToExpr)
                .collect(Collectors.toList())));
    }

    public class Result {
        private final PolarxExecPlan.AnyPlan anyPlan;
        private Deque<Frame> stack = new ArrayDeque<>();

        public Result(PolarxExecPlan.AnyPlan anyPlan) {
            this.anyPlan = anyPlan;
        }

        public PolarxExecPlan.AnyPlan getAnyPlan() {
            return anyPlan;
        }

        public Deque<Frame> getStack() {
            return stack;
        }

        public Result setStack(Deque<Frame> stack) {
            this.stack = stack;
            return this;
        }
    }

    /**
     * Stack frame.
     */
    public static class Frame {
        public final int ordinalInParent;
        public final RelNode r;

        Frame(int ordinalInParent, RelNode r) {
            this.ordinalInParent = ordinalInParent;
            this.r = r;
        }
    }
}
