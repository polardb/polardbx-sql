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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class TopNExecutorFactory extends ExecutorFactory {

    private TopN topN;
    private int parallelism;
    private SpillerFactory spillerFactory;
    private List<DataType> dataTypeList;
    private List<Executor> executors = new ArrayList<>();

    public TopNExecutorFactory(TopN topN, int parallelism, List<DataType> dataTypeList,
                               SpillerFactory spillerFactory) {
        this.topN = topN;
        this.parallelism = parallelism;
        this.spillerFactory = spillerFactory;
        this.dataTypeList = dataTypeList;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutors(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutors(context);
    }

    private synchronized List<Executor> createAllExecutors(ExecutionContext context) {
        if (executors.isEmpty()) {
            long fetch = -1, skip = 0;
            Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
            if (topN.fetch != null) {
                fetch = getRexParam(topN.fetch, params);
                if (topN.offset != null) {
                    skip = getRexParam(topN.offset, params);
                }
            }
            for (int j = 0; j < parallelism; j++) {
                List<RelFieldCollation> sortList = topN.getCollation().getFieldCollations();
                List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);

                Executor exec = new SpilledTopNExec(dataTypeList, orderBys, skip + fetch, context, spillerFactory);
                exec.setId(topN.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(topN, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }
}
