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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterator;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterators;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * A special Route function used for enum range query
 *
 * @author chenghui.lch
 */
public class PartEnumRouteFunction extends PartRouteFunction {

    protected PartitionInfo partInfo;
    protected StepIntervalInfo rangeIntervalInfo;
    protected PartitionRouter router;
    protected boolean inclMin;
    protected boolean inclMax;
    protected boolean containPartIntFunc = false;
    protected PartitionIntFunction partIntFunc;

    public PartEnumRouteFunction(PartitionInfo partInfo,
                                 PartitionRouter hashRouter,
                                 StepIntervalInfo rangeIntervalInfo,
                                 boolean inclMin,
                                 boolean inclMax) {
        this.partInfo = partInfo;
        this.router = hashRouter;
        this.rangeIntervalInfo = rangeIntervalInfo;
        this.matchLevel = PartKeyLevel.PARTITION_KEY;
        this.inclMin = inclMin;
        this.inclMax = inclMax;
        this.partIntFunc = partInfo.getPartitionBy().getPartIntFunc();
        this.containPartIntFunc = partIntFunc != null;
        
        
    }

    @Override
    public PartRouteFunction copy() {
        PartEnumRouteFunction routeFunction =
            new PartEnumRouteFunction(this.partInfo, this.router, this.rangeIntervalInfo.copy(), this.inclMin,
                this.inclMax);
        return routeFunction;
    }

    @Override
    public BitSet routePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {
        SearchDatumInfo min = rangeIntervalInfo.getMinVal().getBndValue();
        SearchDatumInfo max = rangeIntervalInfo.getMaxVal().getBndValue();
        BitSet allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
        Set<Integer> partPosiSet = new HashSet<>();
        try {
            DataType fldDataType = partInfo.getPartitionBy().getQuerySpaceComparator().getDatumDrdsDataTypes()[0];
            int partitionCount = partInfo.getPartitionBy().getPartitions().size();
            PartitionFieldIterator iterator =
                PartitionFieldIterators.getIterator(fldDataType, partInfo.getPartitionBy().getIntervalType());
            iterator.range(min.getSingletonValue().getValue(), max.getSingletonValue().getValue(), inclMin, inclMax);
            
            boolean isListOrListCol = partInfo.getPartitionBy().getStrategy() == PartitionStrategy.LIST || partInfo.getPartitionBy().getStrategy() == PartitionStrategy.LIST_COLUMNS;
            while (iterator.hasNext()) {
                PartitionField partPruningFld = null;
                Object evalObj = iterator.next();
                if (containPartIntFunc) {
                    partPruningFld = PartitionPrunerUtils.buildPartField(evalObj,
                        DataTypes.LongType, partIntFunc.getReturnType(), null, context, PartFieldAccessType.QUERY_PRUNING);
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
                .setPartBitSetForPartList(allPartBitSet, partPosiSet, matchLevel, partCount, -1, true);
        } catch (InvalidTypeConversionException ex) {
            // Type cast Exception, use full scan instead
            allPartBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSet(partInfo);
        }
        return allPartBitSet;
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

    @Override
    public PartitionRouter getRouter() {
        return router;
    }

    public void setRouter(HashPartRouter router) {
        this.router = router;
    }
}
