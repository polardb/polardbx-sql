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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.hash.HashMethodInfo;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.RuntimeFilterBuilderExec;
import com.alibaba.polardbx.executor.operator.util.BloomFilterProduce;
import com.alibaba.polardbx.executor.operator.util.minmaxfilter.MinMaxFilter;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RuntimeFilterUtil;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import io.airlift.http.client.HttpClient;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RuntimeFilterBuilderExecFactory extends ExecutorFactory {

    private RuntimeFilterBuilder filterBuilder;
    private HttpClient client;
    private URI uri;
    private BloomFilterProduce bloomFilterProduce;

    public RuntimeFilterBuilderExecFactory(RuntimeFilterBuilder filterBuilder, ExecutorFactory executorFactory,
                                           HttpClient httpClient,
                                           URI uri) {
        this.filterBuilder = filterBuilder;
        addInput(executorFactory);
        this.client = httpClient;
        this.uri = uri;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int idx) {
        if (bloomFilterProduce == null) {
            boolean useXxHash = context.getParamManager().getBoolean(ConnectionParams.ENABLE_RUNTIME_FILTER_XXHASH);
            HashMethodInfo hashMethodInfo = useXxHash ? HashMethodInfo.XXHASH_METHOD : HashMethodInfo.MURMUR3_METHOD;

            List<RexNode> conditions = RelOptUtil.conjunctions(filterBuilder.getCondition());
            List<List<Integer>> keyHash = new ArrayList<>();
            List<BloomFilter> bloomFilters = new ArrayList<>();
            List<List<Integer>> bloomfilterId = new ArrayList<>();
            List<List<MinMaxFilter>> minMaxFilters = new ArrayList<>();
            for (RexNode rexNode : conditions) {

                SqlRuntimeFilterBuildFunction buildFunction =
                    (SqlRuntimeFilterBuildFunction) ((RexCall) rexNode).getOperator();

                long ndv = Double.valueOf(buildFunction.getNdv()).longValue();

                if (context.getParamManager().getLong(ConnectionParams.BLOOM_FILTER_GUESS_SIZE) > 0) {
                    ndv = context.getParamManager().getLong(ConnectionParams.BLOOM_FILTER_GUESS_SIZE);
                }

                long bloomFilterSize = Math.max(
                    context.getParamManager().getLong(ConnectionParams.BLOOM_FILTER_MIN_SIZE), ndv);

                bloomFilterSize = Math.min(
                    context.getParamManager().getLong(ConnectionParams.BLOOM_FILTER_MAX_SIZE), bloomFilterSize);

                double fpp = RuntimeFilterUtil.findMinFpp(ndv, bloomFilterSize);

                List<Integer> keys = new ArrayList<>();
                List<MinMaxFilter> minMaxFilterList = new ArrayList<>();
                for (RexNode input : ((RexCall) rexNode).getOperands()) {
                    keys.add(((RexSlot) input).getIndex());
                    minMaxFilterList.add(MinMaxFilter.create(DataTypeUtil.calciteToDrdsType(input.getType())));
                }
                bloomfilterId.add(buildFunction.getRuntimeFilterIds());
                keyHash.add(keys);

                BloomFilter bloomFilter = BloomFilter.createEmpty(hashMethodInfo, bloomFilterSize, fpp);
                bloomFilters.add(bloomFilter);
                minMaxFilters.add(minMaxFilterList);
            }
            bloomFilterProduce = BloomFilterProduce.create(
                bloomfilterId, keyHash, bloomFilters, minMaxFilters, client, uri, context.getTraceId());
        }
        bloomFilterProduce.addCounter();
        Executor input = getInputs().get(0).createExecutor(context, idx);
        Executor exec = new RuntimeFilterBuilderExec(input, bloomFilterProduce, context, idx);

        exec.setId(filterBuilder.getRelatedId());
        if (context.getRuntimeStatistics() != null) {
            RuntimeStatHelper.registerStatForExec(filterBuilder, exec, context);
        }
        return exec;
    }
}
