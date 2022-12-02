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
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRoutingContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import org.apache.calcite.sql.SqlCall;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartTupleRouter extends AbstractLifecycle {
    private PartitionInfo partInfo;
    private PartitionTupleRouteInfo routeInfo;
    private PartitionTupleRoutingContext routingContext;
    private ExecutionContext context;
    private int hashCodeLength = 0;

    public PartTupleRouter(PartitionInfo partInfo, ExecutionContext ec) {
        this.partInfo = partInfo;
        this.context = ec.copy();
    }

    @Override
    protected void doInit() {
        String dbName = partInfo.getTableSchema();
        String tblName = partInfo.getTableName();

        if (partInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH) {
            this.hashCodeLength = 1;
        } else if (partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY) {
            this.hashCodeLength = partInfo.getPartitionBy().getPartitionFieldList().size();
        }

        List<ColumnMeta> targetValuesColMetas = partInfo.getPartitionBy().getPartitionFieldList();
        PartitionTupleRoutingContext routingContext = PartitionTupleRoutingContext
            .buildPartitionTupleRoutingContext(dbName, tblName, partInfo, targetValuesColMetas);
        this.routingContext = routingContext;

        SqlCall rowsAst = routingContext.createPartColDynamicParamAst();
        PartitionTupleRouteInfo tupleRouteInfo =
            BuildPlanUtils.buildPartitionTupleRouteInfo(routingContext, rowsAst, true, context);
        this.routeInfo = tupleRouteInfo;
    }

    /**
     * Route the target tuple to target partition
     *
     * Note: The targetTuple to be routed which must contain all partition column values
     *
     * @param targetTuple
     * @return
     */
    public PhysicalPartitionInfo routeTuple(List<Object> targetTuple) {
        if (targetTuple.size() != this.partInfo.getPartitionBy().getPartitionFieldList().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Tuple values does NOT match the partition columns");
        }
        Parameters valuesParams = routingContext.createPartColValueParameters(targetTuple);
        List<PhysicalPartitionInfo> phyInfos =
            BuildPlanUtils.resetParamsAndDoPruning(routeInfo, context, true, 0, valuesParams);
        if (phyInfos.size() == 0) {
            return null;
        }
        return phyInfos.get(0);
    }

    /**
     * Calculate the searchDatum in partition search space, which has been finish computing all partition function result
     *
     * @param targetTuple
     * @return
     */
    public SearchDatumInfo calcSearchDatum(List<Object> targetTuple) {
        if (targetTuple.size() != this.partInfo.getPartitionBy().getPartitionFieldList().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Tuple values does NOT match the partition columns");
        }
        Parameters valuesParams = routingContext.createPartColValueParameters(targetTuple);
        SearchDatumInfo result = BuildPlanUtils.resetParamsAndDoCalcSearchDatum(routeInfo, context, true, 0, valuesParams);
        return result;
    }

    /**
     *
     * Calculate the hashcode value for target tuple
     *
     * @param targetTuple
     * @return
     */
    public Long[] calcHashCode(List<Object> targetTuple) {
        PartitionStrategy strategy = this.partInfo.getPartitionBy().getStrategy();
        if ( strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.HASH ) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Computing hashcode for the partition policy range/list policy ");
        }
        SearchDatumInfo dataum = calcSearchDatum(targetTuple);
        int len = dataum.getDatumInfo().length;
        Long[] hashVals = new Long[len];
        for (int i = 0; i < len; i++) {
            hashVals[i] = dataum.getDatumInfo()[i].getValue().longValue();
        }
        return hashVals;
    }
}
