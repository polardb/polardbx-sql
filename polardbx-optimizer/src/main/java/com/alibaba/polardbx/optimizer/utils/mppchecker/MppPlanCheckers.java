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

package com.alibaba.polardbx.optimizer.utils.mppchecker;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.Optional;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.EXPLICIT_TRANSACTION;

public class MppPlanCheckers {
    public static final MppPlanChecker MPP_ENABLED_CHECKER = input -> input.getPlannerContext()
        .getParamManager()
        .getBoolean(ConnectionParams.ENABLE_MPP);

    public static final MppPlanChecker TRANSACTION_CHECKER =
        input -> Optional.ofNullable(input.getPlannerContext().getExecutionContext())
            .map(c -> mppSupportTransaction(c))
            .orElse(true);

    public static final MppPlanChecker UPDATE_CHECKER =
        input -> Optional.of(input.getPlannerContext().getExecutionContext())
            .map(c -> c.getSqlType() != SqlType.SELECT_FOR_UPDATE)
            .orElse(true);

    public static final MppPlanChecker INTERNAL_SYSTEM_SQL_CHECKER =
        input -> Optional.ofNullable(input.getPlannerContext().getExecutionContext())
            .map(c -> !c.isInternalSystemSql())
            .orElse(true);

    public static final MppPlanChecker SUBQUERY_CHECKER = input -> !input.getPlannerContext().isInSubquery();

    public static final MppPlanChecker QUERY_CHECKER = input -> input.getPlannerContext()
        .getSqlKind()
        .belongsTo(SqlKind.QUERY);

    public static final MppPlanChecker CTE_CHECKER = input -> !input.getPlannerContext().isHasRecursiveCte();

    public static final MppPlanChecker SIMPLE_QUERY_PLAN_CHECKER =
        input -> !RelUtils.isSimpleQueryPlan(input.getOriginalPlan());

    public static final MppPlanChecker EXPLAIN_EXECUTE_CHECKER =
        input -> Optional.ofNullable(input.getPlannerContext().getExecutionContext())
            .map(c -> !ExplainResult.isExplainExecute(c.getExplain()))
            .orElse(true);

    public static final MppPlanChecker SELECT_INTO_OUT_STATISTICS_CHECKER =
        input -> !(input.getOriginalPlan() instanceof LogicalOutFile);

    public static final MppPlanChecker BASIC_CHECKERS =
        input -> Lists.newArrayList(MPP_ENABLED_CHECKER, SUBQUERY_CHECKER, QUERY_CHECKER, INTERNAL_SYSTEM_SQL_CHECKER,
                EXPLAIN_EXECUTE_CHECKER, CTE_CHECKER, SELECT_INTO_OUT_STATISTICS_CHECKER)
            .stream()
            .allMatch(c -> c.supportsMpp(input));

    public static boolean supportsMppPlan(RelNode plan, PlannerContext context, MppPlanChecker... checkers) {
        MppPlanCheckerInput input = new MppPlanCheckerInput(plan, context);

        return Arrays.stream(checkers)
            .allMatch(c -> c.supportsMpp(input));
    }

    public static boolean mppSupportTransaction(ExecutionContext context) {
        if (!ConfigDataMode.isMasterMode()) {
            //只读实例上任何事务策略下都可以跑 mpp
            return true;
        } else {
            boolean mppNotSupportTransaction = false;
            if (context.getTransaction() != null &&
                context.getTransaction().getTransactionClass().isA(EXPLICIT_TRANSACTION)) {
                mppNotSupportTransaction = true;
            } else if (!context.isAutoCommit()) {
                mppNotSupportTransaction = true;
            }
            return !mppNotSupportTransaction;
        }
    }
}
