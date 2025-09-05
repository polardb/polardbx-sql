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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.FullScanTableBlackListManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruner {

    //=========== Build Phase ============

    /**
     * Methods for generating PartitionPruneStep
     */
    public static PartitionPruneStep generatePartitionPrueStepInfo(PartitionInfo partInfo,
                                                                   RelNode relPlan,
                                                                   RexNode partPredInfo,
                                                                   ExecutionContext ec) {
        return PartitionPruneStepBuilder.generatePartitionPruneStepInfo(partInfo,
            relPlan == null ? null : relPlan.getRowType(), partPredInfo, ec);
    }

    /**
     * Methods for generating TupleRouteInfo by LogicalInsert
     */
    public static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(LogicalInsert insert,
                                                                            PartitionInfo partitionInfo) {
        return PartitionTupleRouteInfoBuilder.genPartTupleRoutingInfo(insert, partitionInfo);
    }

    /**
     * Methods for generating TupleRouteInfo by special row values
     *
     * @param tupleValRowType tupleValRowType will contain the datatype of all partition/subpartition columns
     * @param tupleValAst tupleValAst will contain the dynamic params of all partition/subpartition columns
     */
    public static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(String schemaName,
                                                                            String logTbName,
                                                                            PartitionInfo specificPartInfo,
                                                                            RelDataType tupleValRowType,
                                                                            List<List<SqlNode>> tupleValAst,
                                                                            ExecutionContext ec) {
        return PartitionTupleRouteInfoBuilder
            .genPartTupleRoutingInfo(schemaName, logTbName, specificPartInfo, tupleValRowType, tupleValAst, ec);
    }

    //========== Pruning Phase =============

    /**
     * Methods for pruning by stepInfo ( for query with condition )
     */
    public static PartPrunedResult doPruningByStepInfo(PartitionPruneStep stepInfo, ExecutionContext context) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        FullScanTableBlackListManager.getInstance().checkIfAllowFullScan(stepInfo);
        boolean enablePartPruning = context.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTITION_PRUNING);
        if (!enablePartPruning) {
            PartitionInfo partInfo = stepInfo.getPartitionInfo();
            PartitionPruneStep fullScanStep =
                PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                    partInfo.getPartitionBy().getPhysicalPartLevel(), true);
            pruningCtx.setRootStep(fullScanStep);
            PartPrunedResult prunedResult = fullScanStep.prunePartitions(context, pruningCtx, null);
            PartitionPrunerUtils.logStepExplainInfo(context, prunedResult.getPartInfo(), pruningCtx);
            return prunedResult;
        }
        pruningCtx.setRootStep(stepInfo);
        PartPrunedResult prunedResult = stepInfo.prunePartitions(context, pruningCtx, null);
        invalidPartitionFilter(stepInfo.getPartitionInfo(), prunedResult);
        PartitionPrunerUtils.logStepExplainInfo(context, prunedResult.getPartInfo(), pruningCtx);
        return prunedResult;
    }

    public static void invalidPartitionFilter(PartitionInfo partitionInfo, PartPrunedResult prunedResult) {
        if (!prunedResult.getPartBitSet().isEmpty()) {
            List<PartitionSpec> phyPartSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
            int partCnt = phyPartSpecs.size();
            for (int i = prunedResult.getPartBitSet().nextSetBit(0); i >= 0;
                 i = prunedResult.getPartBitSet().nextSetBit(i + 1)) {
                if (i >= partCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "Find pruned partition error");
                }
                PartitionSpec phySpec = phyPartSpecs.get(i);
                if (phySpec.getStatus() != null
                    && phySpec.getStatus() == TablePartitionRecord.PARTITION_STATUS_PARTITION_OFFLINE) {
                    prunedResult.getPartBitSet().clear(i);
                }
            }
        }
    }

    /**
     * Methods for pruning by tupleRouteInfo ( for insert / replace )
     */
    public static PartPrunedResult doPruningByTupleRouteInfo(PartitionTupleRouteInfo tupleRouteInfo, int tupleIndex,
                                                             ExecutionContext context) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        pruningCtx.setPruningByTuple(true);
        /**
         * disable const expr eval cache
         */
        pruningCtx.setEnableConstExprEvalCache(false);
        PartPrunedResult rs = tupleRouteInfo.routeTuple(tupleIndex, context, pruningCtx);
        pruningCtx.setRootTuple(tupleRouteInfo);
        invalidPartitionFilter(tupleRouteInfo.getPartInfo(), rs);
        PartitionPrunerUtils.logStepExplainInfo(context, rs.getPartInfo(), pruningCtx);
        return rs;
    }

    /**
     * Methods for calculating partition func expression by tupleRouteInfo
     */
    public static List<SearchDatumInfo> doCalcSearchDatumByTupleRouteInfo(PartitionTupleRouteInfo tupleRouteInfo,
                                                                          int tupleIndex,
                                                                          ExecutionContext context) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        pruningCtx.setPruningByTuple(true);
        /**
         * disable const expr eval cache
         */
        pruningCtx.setEnableConstExprEvalCache(false);
        return tupleRouteInfo.calcSearchDatum(tupleIndex, context, pruningCtx);
    }

    /**
     * Do pruning by plan and context, only used by LogicalView
     */
    public static List<PartPrunedResult> prunePartitions(RelNode relPlan, ExecutionContext context) {

        if (relPlan instanceof LogicalView) {
            LogicalView logicalView = (LogicalView) relPlan;

            boolean useSelectPartitions = logicalView.useSelectPartitions();
            if (logicalView.isJoin()) {
                List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
                for (int i = 0; i < logicalView.getTableNames().size(); i++) {

                    PartitionPruneStep pruneStepInfo = null;
                    RelShardInfo relShardInfo = logicalView.getRelShardInfo(i, context);
                    if (!useSelectPartitions)  {
                        pruneStepInfo = relShardInfo.getPartPruneStepInfo();
                    } else {
                        PartitionInfo partInfo = relShardInfo.getPartPruneStepInfo().getPartitionInfo();
                        pruneStepInfo = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                            partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                    }

                    /**
                     * do pruning partitions by context
                     */
                    PartPrunedResult tbPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);
                    allTbPrunedResults.add(tbPrunedResult);
                }

                boolean bitSetSame = checkIfBitSetTheSameForPrunedResults(allTbPrunedResults);
                if (!bitSetSame) {
                    /**
                     * <pre>
                     *     When bitSetSame = false, that means:
                     *     a. LogicalView contains a Pushed Join with at least 2 tables;
                     *     b. the predicates of left tbl of join and
                     *        the predicates of right tbl of join are NOT the same,
                     *        so the pruning bitset of left tbl and right tbl are different;
                     *     c. As building the physical of the pushed join of logicalVew must
                     *        be sure that the pruning result of left tbl and right tbl
                     *        are the same, so here have to generate full scan for both
                     *        left tbl and right tbl ignoring there actual pruning result.
                     * <pre/>
                     *
                     *
                     */
                    List<PartPrunedResult> fullScanResults = new ArrayList<>();
                    for (int i = 0; i < logicalView.getTableNames().size(); i++) {
                        String tblName = logicalView.getTableNames().get(i);
                        String schemaName = logicalView.getSchemaName();
                        PartitionInfo partInfo = null;
                        if (context != null) {
                            partInfo = context.getSchemaManager(schemaName).getTable(tblName).getPartitionInfo();
                        }
                        PartitionPruneStep partitionPruneStep =
                            PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                                partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                        PartPrunedResult tbPrunedResult =
                            PartitionPruner.doPruningByStepInfo(partitionPruneStep, context);
                        fullScanResults.add(tbPrunedResult);
                    }
                    return fullScanResults;
                }

                return allTbPrunedResults;
            } else {
                PartPrunedResult tbPrunedResult = null;
                RelShardInfo relShardInfo = logicalView.getRelShardInfo(0, context);
                if (!useSelectPartitions) {
                    PartitionPruneStep pruneStepInfo = relShardInfo.getPartPruneStepInfo();
                    /**
                     * do pruning partitions by context
                     */
                    tbPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);
                } else {
                    PartitionInfo partInfo = relShardInfo.getPartPruneStepInfo().getPartitionInfo();
                    PartitionPruneStep fullScanStep = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                        partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                    tbPrunedResult = PartitionPruner.doPruningByStepInfo(fullScanStep, context);
                }
                List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
                allTbPrunedResults.add(tbPrunedResult);
                return allTbPrunedResults;
            }
        } else {
            throw GeneralUtil.nestedException(new NotSupportException("Not support to non logical view"));
        }
    }

    private static boolean checkIfBitSetTheSameForPrunedResults(List<PartPrunedResult> allTbPrunedResults) {
        BitSet bitSet = null;
        boolean bitSetSame = true;
        for (PartPrunedResult partPrunedResult : allTbPrunedResults) {
            PartitionInfo partitionInfo = partPrunedResult.getPartInfo();
            if (partitionInfo.isBroadcastTable()) {
                continue;
            }

            if (bitSet == null) {
                bitSet = partPrunedResult.getPartBitSet();
            } else if (!bitSet.equals(partPrunedResult.getPartBitSet())) {
                bitSetSame = false;
                break;
            }
        }
        return bitSetSame;
    }

    public static List<PartPrunedResult> pruneCciPartitions(RelNode relPlan, ExecutionContext context,
                                                            PartitionInfo cciPartInfo) {
        if (!(relPlan instanceof OSSTableScan)) {
            throw GeneralUtil.nestedException(new NotSupportException("Not support to non OSS table scan"));
        }
        OSSTableScan ossTableScan = (OSSTableScan) relPlan;
        boolean useSelectPartitions = ossTableScan.useSelectPartitions();
        PartPrunedResult tbPrunedResult;
        if (!useSelectPartitions) {
            RelShardInfo relShardInfo = ossTableScan.getCciRelShardInfo(context, cciPartInfo);
            tbPrunedResult = PartitionPruner.doPruningByStepInfo(relShardInfo.getPartPruneStepInfo(), context);
        } else {
            PartitionPruneStep fullScanStep = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(cciPartInfo,
                cciPartInfo.getPartitionBy().getPartLevel(), true);
            tbPrunedResult = PartitionPruner.doPruningByStepInfo(fullScanStep, context);
        }
        return Collections.singletonList(tbPrunedResult);
    }
}
