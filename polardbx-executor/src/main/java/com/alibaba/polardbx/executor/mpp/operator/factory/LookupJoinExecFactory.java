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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LookupJoinExec;
import com.alibaba.polardbx.executor.operator.LookupJoinGsiExec;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils.existLookupGsiSide;

public class LookupJoinExecFactory extends ExecutorFactory {

    private Join join;
    private List<EquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private boolean maxOneRow;

    public LookupJoinExecFactory(Join join, ExecutorFactory outerFactory, ExecutorFactory innerFactory) {
        if (join instanceof SemiBKAJoin) {
            this.maxOneRow = join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.INNER;
        } else {
            this.maxOneRow = false;
        }
        this.join = join;
        this.allJoinKeys = EquiJoinUtils.buildEquiJoinKeys(join, join.getOuter(), join.getInner(),
            (RexCall) join.getCondition(), join.getJoinType());
        addInput(innerFactory);
        addInput(outerFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor ret;
        Executor inner;
        boolean allowMultiReadConn = ExecUtils.allowMultipleReadConns(context, null);

        Executor outer = getInputs().get(1).createExecutor(context, index);
        if (getInputs().get(1) instanceof LogicalViewExecutorFactory) {
            LogicalView outerLv = ((LogicalViewExecutorFactory) getInputs().get(1)).getLogicalView();
            allowMultiReadConn = allowMultiReadConn && ExecUtils.allowMultipleReadConns(context, outerLv);
        }

        int shardCount = -1;
        int parallelism = 1;
        if (getInputs().get(0) instanceof LogicalViewExecutorFactory) {
            LogicalViewExecutorFactory innerLvExecFactory = (LogicalViewExecutorFactory) getInputs().get(0);
            LogicalView innerLv = innerLvExecFactory.getLogicalView();
            allowMultiReadConn = allowMultiReadConn && ExecUtils.allowMultipleReadConns(context, innerLv);

            // 分库 -> List[List[一个物理 SQL 中的所有表]]]
            Map<String, List<List<String>>> targetTables = innerLv.getTargetTables(context);
            shardCount = targetTables.values().stream().mapToInt(List::size).sum();
            parallelism = innerLvExecFactory.getParallelism();
        }

        boolean isLookUpGsi = existLookupGsiSide(join);

        inner = getInputs().get(0).createExecutor(context, index);
        IExpression otherCondition = convertExpression(join.getCondition(), context);

        if (!isLookUpGsi) {
            ret = new LookupJoinExec(outer, inner, join.getJoinType(), maxOneRow, allJoinKeys, allJoinKeys,
                otherCondition, context, shardCount, parallelism, allowMultiReadConn);
        } else {
            ret = new LookupJoinGsiExec(outer, inner, join.getJoinType(), maxOneRow, allJoinKeys, allJoinKeys,
                otherCondition, context, shardCount, parallelism, allowMultiReadConn);
        }

        registerRuntimeStat(ret, join, context);
        return ret;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }
}
