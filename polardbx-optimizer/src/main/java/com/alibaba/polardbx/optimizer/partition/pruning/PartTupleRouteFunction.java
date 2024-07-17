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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Route one tuple for the special key level of partitioned table
 *
 * @author chenghui.lch
 */
public class PartTupleRouteFunction extends PartRouteFunction {

    /**
     * The partition expression exec info for each part columns
     */
    protected PartClauseExprExec[] partClauseExprExecArr;

    /**
     * The tuple route info of subpartition
     */
    protected PartTupleRouteFunction subPartDispatchFunc;

    public PartTupleRouteFunction() {
    }

    @Override
    public PartRouteFunction copy() {

        PartTupleRouteFunction newFn = new PartTupleRouteFunction();
        newFn.setCmpKind(this.cmpKind);
        newFn.setStrategy(this.strategy);
        newFn.setMatchLevel(this.matchLevel);
        newFn.setPartInfo(this.partInfo);
        newFn.setPartClauseExprExecArr(this.partClauseExprExecArr);
        newFn.setSubPartDispatchFunc(this.subPartDispatchFunc);
        return newFn;
    }

    protected SearchDatumHasher getHasherByPartInfo(PartKeyLevel partLevel,
                                                    PartitionInfo partInfo) {

        if (partLevel == PartKeyLevel.PARTITION_KEY) {
            return partInfo.getPartitionBy().getHasher();
        }
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            return partInfo.getPartitionBy().getSubPartitionBy().getHasher();
        }
        return null;
    }

    @Override
    public PartPrunedResult routePartitions(ExecutionContext ec, PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {
        return routePartitionsInner(ec, pruningCtx, parentPartPosiSet == null ? null : parentPartPosiSet.get(0));
    }

    protected PartPrunedResult routePartitionsInner(ExecutionContext ec,
                                                    PartPruneStepPruningContext pruningCtx,
                                                    Integer parentPartPosi) {

        PartitionRouter router = getRouterByPartInfo(this.matchLevel, parentPartPosi, this.partInfo);
        BitSet partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);

        // build the datum of the search value
        SearchDatumInfo finalVal = buildSearchDatumInfoForTupleData(ec, pruningCtx);

        // Route and build bitset by tuple value
        PartitionRouter.RouterResult result = router.routePartitions(ec, cmpKind, finalVal);

        // Put the pruned result into the partition bitset
        if (result.strategy != PartitionStrategy.LIST && result.strategy != PartitionStrategy.LIST_COLUMNS) {
            PartitionPrunerUtils.setPartBitSetByStartEnd(partBitSet, result.partStartPosi, result.pasrEndPosi, true);
        } else {
            PartitionPrunerUtils.setPartBitSetForPartList(partBitSet, result.partPosiSet, true);
        }

        // Route and build bitset for subpartition by tuple value
        if (subPartDispatchFunc != null) {
            List<PartitionSpec> partSpecList = this.partInfo.getPartitionBy().getPartitions();
            int partCnt = router.getPartitionCount();
            BitSet phyPartBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
            for (int i = partBitSet.nextSetBit(0); i >= 0; i = partBitSet.nextSetBit(i + 1)) {
                if (i >= partCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "Find pruned partition error");
                }

                PartitionSpec ps = partSpecList.get(i);
                PartPrunedResult subPartPruneRs =
                    subPartDispatchFunc.routePartitionsInner(ec, pruningCtx, ps.getPosition().intValue());

                /**
                 * Change the bitset of sub-parts of one part to the bitset of sub-parts of all parts
                 */
                BitSet oneSubPartBitSet = subPartPruneRs.getPartBitSet();
                List<PartitionSpec> subPartSpecList = ps.getSubPartitions();
                for (int j = oneSubPartBitSet.nextSetBit(0); j >= 0; j = oneSubPartBitSet.nextSetBit(j + 1)) {
                    PartitionSpec subPartSpec = subPartSpecList.get(j);
                    phyPartBitSet.set(subPartSpec.getPhyPartPosition().intValue() - 1);
                }
            }
            PartPrunedResult prunedResult =
                PartPrunedResult.buildPartPrunedResult(partInfo, phyPartBitSet, PartKeyLevel.SUBPARTITION_KEY,
                    parentPartPosi, true);
            return prunedResult;
        } else {
            PartPrunedResult prunedResult =
                PartPrunedResult.buildPartPrunedResult(partInfo, partBitSet, this.matchLevel, parentPartPosi, false);
            return prunedResult;
        }
    }

    /**
     * Calculate the actual SearchDatum for a query value that is used to search the target partition
     * if the partition policy is key or hash,
     * the calculating result are the final hashcode values that are used to search the hash partition
     * <p>
     * if the partition policy is range/rangeCol/list/listCol,
     * the calculating result are the values after finish computing partition function
     */
    public List<SearchDatumInfo> calcSearchDatum(ExecutionContext ec,
                                                 PartPruneStepPruningContext pruningCtx) {

        // build the datum of the search value after calc partition expression
        SearchDatumInfo finalVal = buildSearchDatumInfoForTupleData(ec, pruningCtx);

        List<SearchDatumInfo> calcResult = new ArrayList<>();
        PartitionStrategy strategy = this.strategy;
        SearchDatumInfo hashValSearchDatumInfo = null;
        if (strategy == PartitionStrategy.KEY) {
            SearchDatumHasher hasher = getHasherByPartInfo(this.matchLevel, this.partInfo);
            hashValSearchDatumInfo = KeyPartRouter.buildHashSearchDatumInfo(finalVal, hasher, ec);
            calcResult.add(hashValSearchDatumInfo);
        } else if (strategy == PartitionStrategy.HASH) {
            SearchDatumHasher hasher = getHasherByPartInfo(this.matchLevel, this.partInfo);
            hashValSearchDatumInfo = HashPartRouter.buildHashSearchDatumInfo(finalVal, hasher, ec);
            calcResult.add(hashValSearchDatumInfo);
        } else if (strategy == PartitionStrategy.CO_HASH) {
            SearchDatumHasher hasher = getHasherByPartInfo(this.matchLevel, this.partInfo);
            hashValSearchDatumInfo = CoHashPartRouter.buildHashSearchDatumInfo(finalVal, hasher, ec);
            calcResult.add(hashValSearchDatumInfo);
        } else {
            // return directly
            calcResult.add(finalVal);
        }
        if (subPartDispatchFunc == null) {
            return calcResult;
        }

        List<SearchDatumInfo> subPartFinalVals = subPartDispatchFunc.calcSearchDatum(ec, pruningCtx);
        calcResult.add(subPartFinalVals.get(0));
        return calcResult;
    }

    protected SearchDatumInfo buildSearchDatumInfoForTupleData(ExecutionContext context,
                                                               PartPruneStepPruningContext pruningCtx) {
        SearchDatumInfo datumInfo;
        int partColCnt = partClauseExprExecArr.length;
        PartitionBoundVal[] datumValArr = new PartitionBoundVal[partColCnt];
        for (int i = 0; i < partColCnt; i++) {
            PartClauseExprExec partClauseExprExec = partClauseExprExecArr[i];

            /**
             * Eval the  expression value of the partition column
             */
            PartitionField partField = partClauseExprExec.evalPredExprVal(context, pruningCtx, null);

            /**
             * Eval the partIntFunc value for the partition column if partIntFunc exists
             */
            PartitionField newPartField;
            PartitionIntFunction partIntFunc = partClauseExprExec.getPartIntFunc();

            if (partIntFunc != null) {
                newPartField = PartitionPrunerUtils
                    .evalPartFuncVal(partField, partIntFunc, getStrategy(), context, null,
                        PartFieldAccessType.DML_PRUNING);
            } else {
                newPartField = partField;
            }

            /**
             * Construct the PartitionBoundVal
             */
            PartitionBoundVal onePartColVal = PartitionBoundVal.createPartitionBoundVal(newPartField,
                PartitionBoundValueKind.DATUM_NORMAL_VALUE);

            datumValArr[i] = onePartColVal;
        }
        datumInfo = new SearchDatumInfo(datumValArr);
        return datumInfo;
    }

    public PartTupleRouteFunction getSubPartDispatchFunc() {
        return subPartDispatchFunc;
    }

    public void setSubPartDispatchFunc(
        PartTupleRouteFunction subPartDispatchFunc) {
        this.subPartDispatchFunc = subPartDispatchFunc;
    }

    public PartClauseExprExec[] getPartClauseExprExecArr() {
        return partClauseExprExecArr;
    }

    public void setPartClauseExprExecArr(
        PartClauseExprExec[] partClauseExprExecArr) {
        this.partClauseExprExecArr = partClauseExprExecArr;
    }
}
