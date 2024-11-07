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

package com.alibaba.polardbx.optimizer.partition.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRoutingContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import org.apache.calcite.sql.SqlCall;

import java.util.ArrayList;
import java.util.List;

/**
 * A Full Tuple Partition Router
 * for the internal usage of routing,
 * such as Split hot value.
 *
 * <pre>
 *     Notice:
 *          PartTupleRouter is non-thread-safe，
 *          so route tuple one by one.
 *     e.g:
 *     PartTupleRouter router = new PartTupleRouter(...);
 *      for each tuple of tupleList
 *            part = router.routeTuple(tuple)
 *
 * </pre>
 *
 * @author chenghui.lch
 */
public class PartTupleRouter extends AbstractLifecycle {
    private PartitionInfo partInfo;
    private PartitionByDefinition partByDef;
    private PartitionTupleRouteInfo routeInfo;
    private PartitionTupleRoutingContext routingContext;
    private ExecutionContext context;
    private boolean useSubPartBy = false;

    public PartTupleRouter(PartitionInfo partInfo, ExecutionContext ec) {
        this.partInfo = partInfo;
        this.context = ec.copy();
        this.useSubPartBy = partInfo.getPartitionBy().getSubPartitionBy() != null;
    }

    @Override
    protected void doInit() {
        String dbName = partInfo.getTableSchema();
        String tblName = partInfo.getTableName();
        List<ColumnMeta> fullPartColMetas = partInfo.getPartitionBy().getFullPartitionColumnMetas();
        PartitionTupleRoutingContext routingContext = PartitionTupleRoutingContext
            .buildPartitionTupleRoutingContext(dbName, tblName, partInfo, fullPartColMetas);
        this.routingContext = routingContext;
        SqlCall fullPartColValAst = routingContext.createPartColDynamicParamAst();
        PartitionTupleRouteInfo tupleRouteInfo =
            BuildPlanUtils.buildPartitionTupleRouteInfo(routingContext, fullPartColValAst, true, context);
        this.routeInfo = tupleRouteInfo;
    }

    /**
     * Route the target tuples to target partition
     * <p>
     * targetTuples[0]: the tuple of 1st-level-partition
     * targetTuples[1]: the tuple of 2nd-level-partition
     * <p>
     * Note: The targetTuple to be routed which must contain all partition column values
     */
    public PhysicalPartitionInfo routeTuple(List<List<Object>> targetTuples) {

        checkIfTupleValuesInvalid(targetTuples);

        Parameters valuesParams = routingContext.createPartColValueParametersByPartTupleAndSubPartTuple(targetTuples);
        List<PhysicalPartitionInfo> phyInfos =
            BuildPlanUtils.resetParamsAndDoPruning(routeInfo, context, true, 0, valuesParams);
        if (phyInfos.size() == 0) {
            return null;
        }
        return phyInfos.get(0);
    }

    private void checkIfTupleValuesInvalid(List<List<Object>> targetTuples) {
        if (useSubPartBy && targetTuples.size() != 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                "Tuple values should contains both partition value and subpartition value");
        }

        if (!useSubPartBy && targetTuples.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                "Tuple values should contains a partition value");
        }

        List<Object> partColTuple = targetTuples.get(0);
        if (partColTuple.size() != this.partInfo.getPartitionBy().getPartitionFieldList().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                "Tuple values does NOT match the partition columns");
        }

        List<Object> subPartColTuple = null;
        if (useSubPartBy) {
            subPartColTuple = targetTuples.get(1);
            if (subPartColTuple.size() != this.partInfo.getPartitionBy().getSubPartitionBy().getPartitionFieldList()
                .size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    "Tuple values does NOT match the subpartition columns");
            }
        }
    }

    /**
     * Calculate the searchDatum in partition search space, which has been finish computing all partition function result
     * <p>
     * targetTuples[0]: the tuple of 1st-level-partition
     * targetTuples[1]: the tuple of 2nd-level-partition
     *
     * @return the first SearchDatumInfo of results: the calc bound result of 1st-level-partition
     * the second SearchDatumInfo of results: the calc bound result of 2nd-level-partition
     */
    public List<SearchDatumInfo> calcSearchDatum(List<List<Object>> targetTuples) {
        checkIfTupleValuesInvalid(targetTuples);
        Parameters valuesParams = routingContext.createPartColValueParametersByPartTupleAndSubPartTuple(targetTuples);
        List<SearchDatumInfo> result =
            BuildPlanUtils.resetParamsAndDoCalcSearchDatum(routeInfo, context, true, 0, valuesParams);
        return result;
    }

    /**
     * Calculate the hashcode value for target tuple
     * <p>
     * targetTuples[0]: the tuple of 1st-level-partition
     * targetTuples[1]: the tuple of 2nd-level-partition
     *
     * @return the first Long[] of results: the hashcode value of 1st-level-partition
     * the second Long[] of results: the hashcode value of 2nd-level-partition
     */
    public List<Long[]> calcHashCode(List<List<Object>> targetTuples) {
        checkIfTupleValuesInvalid(targetTuples);
        List<SearchDatumInfo> dataums = calcSearchDatum(targetTuples);
        List<Long[]> hashResults = new ArrayList<>();
        for (int k = 0; k < dataums.size(); k++) {
            SearchDatumInfo dataum = dataums.get(k);
            int len = dataum.getDatumInfo().length;
            Long[] hashVals = new Long[len];
            for (int i = 0; i < len; i++) {
                hashVals[i] = dataum.getDatumInfo()[i].getValue().longValue();
            }
            hashResults.add(hashVals);
        }
        return hashResults;
    }
}
