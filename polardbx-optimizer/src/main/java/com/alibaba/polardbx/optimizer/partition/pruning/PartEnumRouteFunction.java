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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterator;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterators;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A special Route function used for enum range query
 *
 * @author chenghui.lch
 */
public class PartEnumRouteFunction extends PartRouteFunction {

    protected PartitionInfo partInfo;
    protected StepIntervalInfo rangeIntervalInfo;
    protected boolean inclMin;
    protected boolean inclMax;
    protected boolean containPartIntFunc = false;
    protected PartitionIntFunction partIntFunc;

    public PartEnumRouteFunction(PartitionInfo partInfo,
                                 StepIntervalInfo rangeIntervalInfo,
                                 boolean inclMin,
                                 boolean inclMax,
                                 PartKeyLevel partKeyLevel) {
        this.partInfo = partInfo;
        this.rangeIntervalInfo = rangeIntervalInfo;
        this.matchLevel = partKeyLevel;
        this.inclMin = inclMin;
        this.inclMax = inclMax;
        this.partIntFunc = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY ?
            partInfo.getPartitionBy().getSubPartitionBy().getPartIntFunc() : partInfo.getPartitionBy().getPartIntFunc();
        this.containPartIntFunc = partIntFunc != null;
    }

    @Override
    public PartRouteFunction copy() {
        PartEnumRouteFunction routeFunction =
            new PartEnumRouteFunction(this.partInfo, this.rangeIntervalInfo.copy(), this.inclMin,
                this.inclMax, this.matchLevel);
        return routeFunction;
    }

    @Override
    public PartPrunedResult routePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {
        SearchDatumInfo min = rangeIntervalInfo.getMinVal().getBndValue();
        SearchDatumInfo max = rangeIntervalInfo.getMaxVal().getBndValue();
        Set<Integer> partPosiSet = new HashSet<>();

        // Get the target router from partInfo and parentPartPosi
        Integer parentPartPosi =
            parentPartPosiSet == null || parentPartPosiSet.isEmpty() ? null : parentPartPosiSet.get(0);
        PartitionRouter router = getRouterByPartInfo(this.matchLevel, parentPartPosi, this.partInfo);
        BitSet allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);
        try {
            PartitionByDefinition partBy = partInfo.getPartitionBy();
            if (this.matchLevel == PartKeyLevel.SUBPARTITION_KEY) {
                partBy = partInfo.getPartitionBy().getSubPartitionBy();
            }

            DataType fldDataType = partBy.getQuerySpaceComparator().getDatumDrdsDataTypes()[0];
            int partitionCount = partBy.getPartitions().size();
            PartitionFieldIterator iterator =
                PartitionFieldIterators.getIterator(fldDataType, partBy.getIntervalType(),
                    partIntFunc);
            iterator.range(min.getSingletonValue().getValue(), max.getSingletonValue().getValue(), inclMin, inclMax);

            boolean isListOrListCol = partBy.getStrategy() == PartitionStrategy.LIST
                || partBy.getStrategy() == PartitionStrategy.LIST_COLUMNS;
            while (iterator.hasNext()) {
                PartitionField partPruningFld = null;
                Object evalObj = iterator.next();
                if (containPartIntFunc) {
                    partPruningFld = PartitionPrunerUtils.buildPartField(evalObj,
                        DataTypes.LongType, partIntFunc.getReturnType(), null, context,
                        PartFieldAccessType.QUERY_PRUNING);
                } else {
                    partPruningFld = PartitionPrunerUtils.buildPartField(evalObj,
                        fldDataType, fldDataType, null, context, PartFieldAccessType.QUERY_PRUNING);
                }
                SearchDatumInfo tmpSearchDatumInfo = SearchDatumInfo.createFromField(partPruningFld);
                PartitionRouter.RouterResult result =
                    router.routePartitions(context, ComparisonKind.EQUAL, tmpSearchDatumInfo);
                if (!isListOrListCol) {
                    // Only routing for equal-expr, so just use partStartPosi for range/range_columns
                    partPosiSet.add(result.partStartPosi);
                } else {
                    partPosiSet.addAll(result.partPosiSet);
                }
                if (partitionCount == partPosiSet.size()) {
                    break;
                }
            }
            PartitionPrunerUtils
                .setPartBitSetForPartList(allPartBitSet, partPosiSet, true);
        } catch (InvalidTypeConversionException ex) {
            // Type cast Exception, use full scan instead
            allPartBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSetByPartRouter(router);
        }
        return PartPrunedResult.buildPartPrunedResult(partInfo, allPartBitSet, this.matchLevel, parentPartPosi, false);
    }

    @Override
    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    @Override
    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public StepIntervalInfo getRangeIntervalInfo() {
        return rangeIntervalInfo;
    }

}
