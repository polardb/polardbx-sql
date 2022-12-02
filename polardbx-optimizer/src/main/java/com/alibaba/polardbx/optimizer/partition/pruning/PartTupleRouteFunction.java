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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;

import java.util.BitSet;

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

    public PartTupleRouteFunction(PartClauseExprExec[] partExprExecArr) {
        this.partClauseExprExecArr = partExprExecArr;
    }

    @Override
    public PartRouteFunction copy() {
        PartTupleRouteFunction routeFunction = new PartTupleRouteFunction(this.partClauseExprExecArr);
        return routeFunction;
    }

    @Override
    public BitSet routePartitions(ExecutionContext ec, PartPruneStepPruningContext pruningCtx) {

        BitSet partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);

        // build the datum of the search value
        SearchDatumInfo finalVal = buildSearchDatumInfoForTupleData(ec, pruningCtx);

        // Route and build bitset by tuple value
        PartitionRouter.RouterResult result = router.routePartitions(ec, cmpKind, finalVal);

        // Put the pruned result into the partition bitset
        if (result.strategy != PartitionStrategy.LIST && result.strategy != PartitionStrategy.LIST_COLUMNS) {
            PartitionPrunerUtils
                .setPartBitSetByStartEnd(partBitSet, result.partStartPosi, result.pasrEndPosi, matchLevel, partCount,
                    subPartCount, true);
        } else {
            PartitionPrunerUtils
                .setPartBitSetForPartList(partBitSet, result.partPosiSet, matchLevel, partCount, subPartCount, true);
        }

        return partBitSet;
    }

    /**
     * Calculate the actual SearchDatum for a query value that is used to search the target partition
     * if the partition policy is key or hash,
     * the calculating result are the final hashcode values that are used to search the hash partition
     *
     * if the partition policy is range/rangeCol/list/listCol,
     * the calculating result are the values after finish computing partition function
     *
     * @param ec
     * @param pruningCtx
     * @return
     */
    public SearchDatumInfo calcSearchDatum(ExecutionContext ec, PartPruneStepPruningContext pruningCtx) {

        // build the datum of the search value after calc partition expression
        SearchDatumInfo finalVal = buildSearchDatumInfoForTupleData(ec, pruningCtx);

        PartitionStrategy strategy = partInfo.getPartitionBy().getStrategy();
        SearchDatumInfo hashValSearchDatumInfo = null;
        if (strategy == PartitionStrategy.KEY) {
            hashValSearchDatumInfo = ((KeyPartRouter)router).buildHashSearchDatumInfo(finalVal, ec);
            return hashValSearchDatumInfo;
        } else if (strategy == PartitionStrategy.HASH) {
            hashValSearchDatumInfo = ((HashPartRouter)router).buildHashSearchDatumInfo(finalVal, ec);
            return hashValSearchDatumInfo;
        } else {
            // return directly
            return finalVal;
        }
    }

    public SearchDatumInfo buildTupleSearchDatumInfo(ExecutionContext ec, PartPruneStepPruningContext pruningCtx) {
        return buildSearchDatumInfoForTupleData(ec, pruningCtx);
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
                    .evalPartFuncVal(partField, partIntFunc, context, null, PartFieldAccessType.DML_PRUNING);
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

}
