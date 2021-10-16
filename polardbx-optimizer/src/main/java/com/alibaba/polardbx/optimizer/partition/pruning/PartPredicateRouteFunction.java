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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.BitSet;

/**
 * Route one simple interval(maybe a multi-column range)
 * for the special key level of partitioned table
 * <p>
 * <pre>
 *
 * e.g
 *     route the interval (partKey) <= (?+?) for range partitions
 * e.g
 *     route the interval (partKey1, partKey2, partKey3) <= (?+?, ?, ?)
 *     for range columns partitions
 * e.g
 *     route the interval (partKey1, partKey2, partKey3) = (?, ?, ?)
 *     for Hash key partitions
 *
 * </pre>
 *
 * @author chenghui.lch
 */
public class PartPredicateRouteFunction extends PartRouteFunction {

    protected ExprContextProvider contextHolder;

    /**
     * The flag that label if the part routing need do map interval from query space to search space
     */
    protected boolean needMapInterval = false;

    /**
     * The flag that label if need eval the part func value by using the expr value of predicate
     */
    protected boolean needEvalPartFunc = false;

    /**
     * label if the part int func is a not-monotonic func
     */
    protected boolean isNonMonotonic = false;

    /**
     * The search expr from predicate
     * <pre>
     * e.g Assume (p1,p2,p3) are the whole part columns for range columns/list columns,
     *     the predicate expr list may be just a part column prefix, such as
     *     predicate1: p1 = const1*const4
     *     predicate2: p2 < const2+const3 ,
     *     that means that this PartPredicateRouteFunction will performance the interval ranges of
     *     "p1=const1*const4 and  p2<const2+const3" .
     *     , then the SearchExprInfo will be "(p1,p2,p3) < (const1*const4, const2+const3, min)"
     * </pre>
     */
    protected SearchExprInfo searchExprInfo = null;

    /**
     * The charset of predicate expr of part key 0, just for range/list/hash
     */
    protected SqlOperator partFuncOperator = null;

    /**
     * the return data type of the partFuncOperator
     */
    protected DataType partExprReturnType = null;

    /**
     * The partition strategy
     */
    protected PartitionStrategy strategy;

    public PartPredicateRouteFunction(PartitionInfo partInfo,
                                      SqlOperator partFuncOperator,
                                      PartKeyLevel keyLevel,
                                      SearchExprInfo searchExprInfo,
                                      ExprContextProvider contextHolder) {
        this.partInfo = partInfo;
        this.contextHolder = contextHolder;
        this.matchLevel = keyLevel;
        this.searchExprInfo = searchExprInfo;
        this.partFuncOperator = partFuncOperator;
        this.cmpKind = searchExprInfo.getCmpKind();
        initRouteFunction();
    }

    protected void initRouteFunction() {

        // Get the partCount and subPartCount of subPartition template
        this.partCount = partInfo.getPartitionBy().getPartitions().size();
        this.subPartCount = -1;
        if (this.partInfo.getSubPartitionBy() != null) {
            this.subPartCount = this.partInfo.getPartitionBy().getPartitions().get(0).getSubPartitions().size();
        }

        // Router should be cached
        if (matchLevel == PartKeyLevel.PARTITION_KEY) {
            this.router = partInfo.getPartitionBy().getRouter();
            this.strategy = partInfo.getPartitionBy().getStrategy();
        } else if (matchLevel == PartKeyLevel.SUBPARTITION_KEY) {
            throw new NotSupportException("Not support subpartitions");
        }

        // Check if need do interval mapping
        if (matchLevel == PartKeyLevel.PARTITION_KEY) {
            SqlNode partExpr = partInfo.getPartitionBy().getPartitionExprList().get(0);
            if (this.strategy == PartitionStrategy.RANGE || this.strategy == PartitionStrategy.LIST
                || this.strategy == PartitionStrategy.HASH) {
                if (partExpr instanceof SqlCall) {
                    /**
                     * The part columns is wrapped with func, such year(partCol)...
                     * , so need do map interval
                     */
                    this.needEvalPartFunc = true;
                    RelDataType partExprDt = this.partInfo.getPartitionBy().getPartitionExprTypeList().get(0);
                    this.partExprReturnType = DataTypeUtil.calciteToDrdsType(partExprDt);
                    Monotonicity monotonicity = this.partInfo.getPartitionBy().getPartIntFuncMonotonicity();
                    this.isNonMonotonic = monotonicity == Monotonicity.NON_MONOTONIC;
                    if (this.cmpKind != ComparisonKind.EQUAL && this.cmpKind != ComparisonKind.NOT_EQUAL) {
                        if (this.strategy == PartitionStrategy.RANGE || this.strategy == PartitionStrategy.LIST) {
                            this.needMapInterval = true;
                        }
                    }
                }
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Not support do partition pruning with subpartitions");
        }

    }

    @Override
    public BitSet routePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {
        BitSet allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
        return routeAndBuildPartBitSet(context, pruningCtx, allPartBitSet);
    }

    protected SearchDatumInfo buildSearchDatumInfoForPredData(ExecutionContext context,
                                                              PartPruneStepPruningContext pruningCtx,
                                                              ComparisonKind[] cmdKindOutput) {

        PartClauseExprExec[] predExprExecArr = this.searchExprInfo.getExprExecArr();
        ComparisonKind cmpKind = this.searchExprInfo.getCmpKind();
        int partColNum = predExprExecArr.length;
        PartitionBoundVal[] searchValArr = new PartitionBoundVal[partColNum];
        if (partColNum > 1) {
            SearchExprEvalResult exprEvalResult =
                PartitionPrunerUtils.evalExprValsAndBuildOneDatum(context, pruningCtx, this.searchExprInfo);
            cmdKindOutput[0] = exprEvalResult.getComparisonKind();
            SearchDatumInfo searchDatumInfo = exprEvalResult.getSearchDatumInfo();
            return searchDatumInfo;
        } else {
            PartClauseExprExec exprExec = predExprExecArr[0];
            boolean isNull = exprExec.isAlwaysNullValue();
            boolean[] epInfo = PartFuncMonotonicityUtil.buildIntervalEndPointInfo(cmpKind);
            PartitionBoundValueKind valKind = exprExec.getValueKind();

            // Compute the const expr val for part predicate
            PartitionField exprValPartField = exprExec.evalPredExprVal(context, pruningCtx, epInfo);

            // Do the interval mapping from predicate query space to partition search space
            // and put the mapping result into the cmdKindOutput.
            PartitionField newPartField = doIntervalMapping(exprExec, context, exprValPartField, cmdKindOutput, epInfo);

            // Build the PartitionBoundVal
            PartitionBoundVal searchVal = PartitionBoundVal.createPartitionBoundVal(newPartField, valKind, isNull);
            searchValArr[0] = searchVal;
            SearchDatumInfo searchDatumInfo = new SearchDatumInfo(searchValArr);
            return searchDatumInfo;
        }
    }

    /**
     *
     *
     *
     * @param exprExec
     * @param context
     * @param exprValPartField
     * @param cmdKindOutput
     * @param endpoints
     * @return
     */
    private PartitionField doIntervalMapping(PartClauseExprExec exprExec,
                                             ExecutionContext context,
                                             PartitionField exprValPartField,
                                             ComparisonKind[] cmdKindOutput,
                                             boolean[] endpoints) {
        ComparisonKind finalCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(endpoints);
        PartitionField newPartField;
        if (this.needEvalPartFunc) {
            PartitionIntFunction partIntFunc = exprExec.getPartIntFunc();
            PartitionField partFuncVal = PartitionPrunerUtils
                .evalPartFuncVal(exprValPartField, partIntFunc, context, endpoints,
                    PartFieldAccessType.QUERY_PRUNING);
            if (this.needMapInterval) {

                // leftEndPoint[0]=false => <
                /**
                 * <pre>
                 * leftEndPoint=true  <=>  const < col or const <= col, so const is the left end point,
                 * leftEndPoint=false <=>  col < const or col <= const, so const is NOT the left end point,
                 *
                 * includeEndPoint=true <=> const <= col or col <= const
                 * includeEndPoint=false <=> const < col or col < const
                 * </pre>
                 *
                 */
                boolean leftEndPoint = endpoints[0];
                boolean includeEndPoint = endpoints[1];
                if (leftEndPoint && !includeEndPoint) {
                    /**
                     * Handle the case :  const < col, if the monotonicity of part func is increase
                     */
                    long longVal = partFuncVal.longValue();
                    endpoints[1] = true;
                    newPartField =
                        PartitionPrunerUtils
                            .buildPartField(longVal + 1, partExprReturnType, partExprReturnType, null, context,
                                PartFieldAccessType.QUERY_PRUNING);
                } else {
                    newPartField = partFuncVal;
                }
                finalCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(endpoints);

            } else {

                newPartField = partFuncVal;

                if (isNonMonotonic) {
                    /**
                     * For the partInfFunc of Monotonicity.NON_MONOTONIC, 
                     * if it is a range query, then do full scan directly
                     */
                    // Use ComparisonKind.NOT_EQUAL to do full scan
                    finalCmpKind = ComparisonKind.NOT_EQUAL;
                }
            }
        } else {
            newPartField = exprValPartField;
        }
        cmdKindOutput[0] = finalCmpKind;
        return newPartField;
    }

    protected BitSet routeAndBuildPartBitSet(ExecutionContext context, PartPruneStepPruningContext pruningCtx,
                                             BitSet allPartBitSet) {

        ComparisonKind[] cmpKindOutput = new ComparisonKind[1];
        SearchDatumInfo finalVal = null;
        try {
            // Compute the const expr val for part predicate
            finalVal = buildSearchDatumInfoForPredData(context, pruningCtx, cmpKindOutput);
        } catch (Throwable ex) {
            if (ex instanceof InvalidTypeConversionException) {
                /**
                 *  when it is failed to compute its SearchDatumInfo because of 
                 *  unsupported type conversion exception,
                 *  the SearchExprInfo should be treated as Always-True expr, 
                 *  so generate a full scan bitset
                 */
                return PartitionPrunerUtils.buildFullScanPartitionsBitSet(this.partInfo);

            } else {
                throw ex;
            }
        }

        // find the target partition position set
        PartitionRouter.RouterResult result = router.routePartitions(context, cmpKindOutput[0], finalVal);

        // Save the pruned result into bitset
        if (result.strategy != PartitionStrategy.LIST && result.strategy != PartitionStrategy.LIST_COLUMNS) {
            PartitionPrunerUtils
                .setPartBitSetByStartEnd(allPartBitSet, result.partStartPosi, result.pasrEndPosi, matchLevel, partCount,
                    subPartCount, true);
        } else {
            PartitionPrunerUtils
                .setPartBitSetForPartList(allPartBitSet, result.partPosiSet, matchLevel, partCount, subPartCount, true);
        }

        return allPartBitSet;
    }

    public ExprContextProvider getContextHolder() {
        return contextHolder;
    }

    public boolean isNeedMapInterval() {
        return needMapInterval;
    }

    public SearchExprInfo getSearchExprInfo() {
        return searchExprInfo;
    }

    public SqlOperator getPartFuncOperator() {
        return partFuncOperator;
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public PartPredicateRouteFunction copy() {
        PartPredicateRouteFunction routeFunction =
            new PartPredicateRouteFunction(this.partInfo, this.partFuncOperator, this.matchLevel,
                this.searchExprInfo.copy(), this.contextHolder);
        return routeFunction;
    }
}
