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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenghui.lch
 */
public class PartPruneStepBuildingContext {

    protected AtomicInteger constExprIdGenerator = new AtomicInteger(0);
    protected ExprContextProvider exprCtxHolder = new ExprContextProvider();
    protected PartitionInfo partInfo;
    protected Set<String> allPartColSet;
    protected int partColCnt = 1;
    protected Map<String, Integer> partColIdxMap;
    protected Map<String, Integer> subPartColIdxMap;

    /**
     * Key: the a digest of const expr of a partition predicate in PartClauseInfo
     * Val: PartPredConstExprInfo ( included the RexNode of the constExpr and its referenced count)
     */
    protected Map<String, PredConstExprReferenceInfo> constExprReferenceInfoMaps;

    /**
     * Key: the a digest of parttion pruning step
     * Val: PartPruneStepReferenceInfo ( the reference info of step)
     */
    protected Map<String, PartPruneStepReferenceInfo> stepReferenceInfoMaps;

    /**
     * DP Algorithm.
     * <p>
     * <pre>
     * a prefixPathInfoCache[i] means
     *  all the prefix from partKeyIdx=0 from partKeyIdx=i-1.
     *
     *  e.g. (p1,p2,p3) is the multi-columns part keys.
     *
     *  For the followed partition predicated
     *  p1<=a and p2<=b and p2<c and p3=d and p3>e and p3<=f
     *  ,
     *  we have
     *
     *  prefixPathInfoCache[0]:
     *      p1<=a ===> (p1,p2,p3)<=(a,max,max)
     *      (
     *          p1<=a
     *          ===> (p1 < a or p1 = a)
     *          ===> ((p1,p2,p3) < (a,min,min) OR (a,min,min)<=(p1,p2,p3)<=(a,max,max))
     *          ===> (p1,p2,p3)<=(a,max,max)
     *
     *      )
     *  ,
     *  prefixPathInfoCache[1]:
     *      p1=a and p2<=b ===> (p1,p2,p3)<=(a,b,max),
     *      p1=a and p2<c ===> (p1,p2,p3)<(a,c,min),
     *  ,
     *  prefixPathInfoCache[2]:
     *      p1=a and p2=b and p3=d ===> (p1,p2,p3)=(a,b,d),
     *      p1=a and p2=b and p3>e ===> (p1,p2,p3)>(a,b,e),
     *      p1=a and p2=b and p3<=f ===> (p1,p2,p3)<=(a,b,f),
     * </pre>
     */
    protected PrefixPartPredPathInfo[] prefixPredPathInfoCtx;

    /**
     * Label if the output predicate after rewriting is a DNF formula
     */
    protected boolean isDnfFormula = true;

    /**
     * switch if enable the interval merging for pruning
     */
    protected boolean enableIntervalMerging = true;

    /**
     * Allow const expression as sharding condition
     */
    private boolean enableConstExpr = true;

    /**
     * When the predicate expr is too complex
     * and its the prune step count is more than pruneStepCountLimit,
     * then it will give up pruning and return full scan instead
     */
    private int pruneStepOpCountLimit = Integer.MAX_VALUE;
    
    private boolean useFastSinglePointIntervalMerging = true;

    public static PartPruneStepBuildingContext getNewPartPruneStepContext(PartitionInfo partInfo,
                                                                          AtomicInteger constExprIdGenerator,
                                                                          ExprContextProvider exprCtxHolder,
                                                                          ExecutionContext ec) {
        PartPruneStepBuildingContext
            newStepCtx = new PartPruneStepBuildingContext(partInfo, constExprIdGenerator, exprCtxHolder, ec);
        return newStepCtx;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    private PartPruneStepBuildingContext(PartitionInfo partInfo, AtomicInteger constExprIdGenerator,
                                         ExprContextProvider exprCtxHolder, ExecutionContext ec) {
        this.partInfo = partInfo;
        this.partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();

        this.constExprReferenceInfoMaps = new HashMap<>();
        this.stepReferenceInfoMaps = new HashMap<>();
        this.prefixPredPathInfoCtx = new PrefixPartPredPathInfo[partColCnt];
        for (int i = 0; i < partColCnt; i++) {
            prefixPredPathInfoCtx[i] = new PrefixPartPredPathInfo(this, i);
        }

        this.allPartColSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (partInfo.getPartitionBy() != null) {
            partColIdxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            List<String> colList = partInfo.getPartitionBy().getPartitionColumnNameList();
            for (int i = 0; i < colList.size(); i++) {
                partColIdxMap.put(colList.get(i), i);
            }
            allPartColSet.addAll(partColIdxMap.keySet());
        }
        this.constExprIdGenerator = constExprIdGenerator;
        this.exprCtxHolder = exprCtxHolder;

        initBuildingProperties(ec);
    }

    protected void initBuildingProperties(ExecutionContext ec) {

        if (ec == null) {
            return;
        }

        String enableShardConstExprStr = GeneralUtil.getPropertyString(ec.getExtraCmds(),
            ConnectionProperties.ENABLE_SHARD_CONST_EXPR, Boolean.TRUE.toString());
        boolean enableShardConstExpr = Boolean.parseBoolean(enableShardConstExprStr);
        this.enableConstExpr = enableShardConstExpr;
        this.pruneStepOpCountLimit = ec.getParamManager().getInt(ConnectionParams.PARTITION_PRUNING_STEP_COUNT_LIMIT);
        this.useFastSinglePointIntervalMerging =
            ec.getParamManager().getBoolean(ConnectionParams.USE_FAST_SINGLE_POINT_INTERVAL_MERGING);

    }

    protected void addPartPredIntoContext(PartClauseInfo partClause) {
        int keyIndex = partClause.getPartKeyIndex();

        /**
         * Put the partition predicate of current key index into context
         */
        PartClauseIntervalInfo clauseInterval =
            new PartClauseIntervalInfo(partClause, this.partInfo, this.exprCtxHolder);
        this.prefixPredPathInfoCtx[keyIndex].addPartClauseIntervalInfo(clauseInterval);
    }

    /**
     * Check if new const expr does already exists in cache,
     * if exists, then reuse;
     * if does NOT exists, the put it into cache
     */
    public PredConstExprReferenceInfo buildPredConstExprReferenceInfo(RexNode newExprConstExpr) {
        String newExprConstDigest = newExprConstExpr.toString();
        PredConstExprReferenceInfo constExprInfoAlreadyExisted = constExprReferenceInfoMaps.get(newExprConstDigest);
        if (constExprInfoAlreadyExisted == null) {
            constExprInfoAlreadyExisted =
                new PredConstExprReferenceInfo(this.constExprIdGenerator.incrementAndGet(), newExprConstExpr);
            constExprReferenceInfoMaps.put(newExprConstDigest, constExprInfoAlreadyExisted);
            return constExprInfoAlreadyExisted;
        } else {
            constExprInfoAlreadyExisted.getReferencedCount().addAndGet(1);
            return constExprInfoAlreadyExisted;
        }
    }

    protected PartPruneStepReferenceInfo buildPruneStepReferenceInfo(PartitionPruneStep step) {
        String stepDigest = step.getStepDigest();
        PartPruneStepReferenceInfo stepReferenceInfo = stepReferenceInfoMaps.get(stepDigest);
        if (stepReferenceInfo == null) {
            stepReferenceInfo = new PartPruneStepReferenceInfo(step);
            stepReferenceInfoMaps.put(stepDigest, stepReferenceInfo);
            return stepReferenceInfo;
        } else {
            stepReferenceInfo.getReferencedCount().addAndGet(1);
            return stepReferenceInfo;
        }
    }

    /**
     * Generate part prune steps by 'prefix' predicates
     * <p>
     * For each clause under consideration for a given strategy,
     * we collect expressions from clauses for earlier keys, whose
     * operator strategy is inclusive, into a list called
     * 'prefix'. By appending the clause's own expression to the
     * 'prefix', we'll generate one step using the so generated
     * vector and assign the current strategy to it.  Actually,
     * 'prefix' might contain multiple clauses for the same key,
     * in which case, we must generate steps for various
     * combinations of expressions of different keys, which
     * generatePruneSteps takes care of for us.
     */
    protected List<PartitionPruneStep> enumPrefixPredAndGenPruneSteps() {

        List<PartitionPruneStep> outputSteps = new ArrayList<>();
        int partColCnt = this.allPartColSet.size();
        for (int keyIndex = 0; keyIndex < partColCnt; keyIndex++) {

            int lastPartKeyIdx = keyIndex - 1;
            boolean isMaxPartKeyIdx = keyIndex == partColCnt - 1;

            /**
             * Get all the prefix predicate path of last part key idx by "this.partPrefixPathContext[lastPartKeyIdx]"
             */
            PrefixPartPredPathInfo prefixPathInfosOfCurrPartKeyIdx = this.prefixPredPathInfoCtx[keyIndex];
            if (keyIndex == 0) {

                if (prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().isEmpty()) {
                    /**
                     * No found any predicates that contains the first partition column( keyIndex = 0),
                     * So in this situations should return a full-scan-step instead
                     */
                    PartitionPruneStep step = PartitionPruneStepBuilder.generateFullScanPrueStepInfo(partInfo);
                    outputSteps.add(step);
                    return outputSteps;
                }

                /**
                 * For first part key, they have no any prefix predicate paths,
                 * so build PartPruneStep just by itself predicates
                 */
                for (int i = 0; i < prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().size(); i++) {
                    PartClauseIntervalInfo partClauseInfo =
                        prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().get(i);
                    ComparisonKind cmpKindOfCurrPartKey = partClauseInfo.getCmpKind();
                    PartPredPathItem newPathItem = new PartPredPathItem(null, partClauseInfo, keyIndex);
                    PartPredPathInfo newPathInfo = new PartPredPathInfo(newPathItem, 0, keyIndex, cmpKindOfCurrPartKey,
                        keyIndex == partColCnt - 1);
                    prefixPathInfosOfCurrPartKeyIdx.addPrefixPathInfo(newPathInfo);

                    if (!cmpKindOfCurrPartKey.containEqual()) {
                        // For "<" or ">"
                        buildPartPruneStepByPartPredPath(newPathInfo, outputSteps);
                    } else {
                        /**
                         *    cmpKindOfCurrPartKey will be only '=' or '>' or '<' here
                         */
                        if (isMaxPartKeyIdx) {
                            buildPartPruneStepByPartPredPath(newPathInfo, outputSteps);
                        }
                    }
                }

            } else {

                PrefixPartPredPathInfo prefixPathInfosOfLastPartKeyIdx = this.prefixPredPathInfoCtx[lastPartKeyIdx];
                boolean noFindPartClause = prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().isEmpty();
                for (int i = 0; i < prefixPathInfosOfLastPartKeyIdx.getPrefixPathInfos().size(); i++) {
                    PartPredPathInfo pathInfoOfLastPartKey =
                        prefixPathInfosOfLastPartKeyIdx.getPrefixPathInfos().get(i);
                    ComparisonKind cmpKindOfLastPartKeyIdx = pathInfoOfLastPartKey.getCmpKind();
                    if (cmpKindOfLastPartKeyIdx.containEqual()) {
                        if (noFindPartClause) {

                            /**
                             * If current part key index has not any partition predicates,
                             * then will generate the range partition predicates with "max/min" values for
                             * all the predicates of last part key index that contains equal symbol.
                             *
                             * e.g:
                             *   p1=a ==> (a,min) <= (p1,p2) <= (a,max)
                             */
                            buildPartPruneStepByPartPredPath(pathInfoOfLastPartKey, outputSteps);

                        } else {
                            /**
                             * For each new predicate of current part key idx, build a new prefix predicate path
                             * according to the predicate path of last part key idx
                             */
                            for (int j = 0; j < prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().size();
                                 j++) {
                                PartClauseIntervalInfo partClauseInfoOfCurPartKeyIdx =
                                    prefixPathInfosOfCurrPartKeyIdx.getPartClauseIntervalInfos().get(j);
                                ComparisonKind cmpKindOfCurPartKeyIdx = partClauseInfoOfCurPartKeyIdx.getCmpKind();
                                PartPredPathItem pathItemOfCurPartKeyIdx =
                                    new PartPredPathItem(pathInfoOfLastPartKey.getPrefixPathItem(),
                                        partClauseInfoOfCurPartKeyIdx, keyIndex);
                                PartPredPathInfo newPathInfo =
                                    new PartPredPathInfo(pathItemOfCurPartKeyIdx, 0, keyIndex, cmpKindOfCurPartKeyIdx,
                                        keyIndex == partColCnt - 1);

                                prefixPathInfosOfCurrPartKeyIdx.addPrefixPathInfo(newPathInfo);
                                if (!cmpKindOfCurPartKeyIdx.containEqual()) {
                                    buildPartPruneStepByPartPredPath(newPathInfo, outputSteps);
                                } else {
                                    if (isMaxPartKeyIdx) {
                                        buildPartPruneStepByPartPredPath(newPathInfo, outputSteps);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return outputSteps;
    }

    protected void buildPartPruneStepByPartPredPath(PartPredPathInfo newPathInfo,
                                                    List<PartitionPruneStep> outputSteps) {
        List<PartitionPruneStep> steps =
            PartitionPruneStepBuilder.genPartitionPruneStepByPartPredPathInfo(this, newPathInfo);
        outputSteps.addAll(steps);
    }

    protected void resetPrefixPartPredPathCtx() {
        prefixPredPathInfoCtx = new PrefixPartPredPathInfo[this.partColCnt];
        for (int i = 0; i < partColCnt; i++) {
            prefixPredPathInfoCtx[i] = new PrefixPartPredPathInfo(this, i);
        }
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public Set<String> getAllPartColSet() {
        return allPartColSet;
    }

    public Map<String, Integer> getPartColIdxMap() {
        return partColIdxMap;
    }

    public void setPartColIdxMap(Map<String, Integer> partColIdxMap) {
        this.partColIdxMap = partColIdxMap;
    }

    public Map<String, Integer> getSubPartColIdxMap() {
        return subPartColIdxMap;
    }

    public void setSubPartColIdxMap(Map<String, Integer> subPartColIdxMap) {
        this.subPartColIdxMap = subPartColIdxMap;
    }

    public ExprContextProvider getExprCtxHolder() {
        return exprCtxHolder;
    }

    public void setExprCtxHolder(ExprContextProvider exprCtxHolder) {
        this.exprCtxHolder = exprCtxHolder;
    }

    public boolean isDnfFormula() {
        return isDnfFormula;
    }

    public void setDnfFormula(boolean dnfFormula) {
        this.isDnfFormula = dnfFormula;
    }

    public boolean isEnableIntervalMerging() {
        return enableIntervalMerging;
    }

    public boolean isEnableConstExpr() {
        return enableConstExpr;
    }

    public int getPruneStepOpCountLimit() {
        return pruneStepOpCountLimit;
    }


    public boolean isUseFastSinglePointIntervalMerging() {
        return useFastSinglePointIntervalMerging;
    }
}
