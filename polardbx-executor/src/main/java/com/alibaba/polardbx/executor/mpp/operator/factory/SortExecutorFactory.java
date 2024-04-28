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
import com.alibaba.polardbx.executor.operator.SortExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;

public class SortExecutorFactory extends ExecutorFactory {

    private MemSort sort;
    private int parallelism;
    private SpillerFactory spillerFactory;
    private List<Executor> executors = new ArrayList<>();
    private List<DataType> dataTypeList;

    public SortExecutorFactory(MemSort sort, int parallelism, List<DataType> dataTypeList,
                               SpillerFactory spillerFactory) {
        this.sort = sort;
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
            for (int j = 0; j < parallelism; j++) {
                List<RelFieldCollation> sortList = sort.getCollation().getFieldCollations();
                List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);

                SortExec sortExec = new SortExec(dataTypeList, orderBys, context, this.spillerFactory);
                registerRuntimeStat(sortExec, sort, context);
                executors.add(sortExec);
            }
        }
        return executors;
    }
}
