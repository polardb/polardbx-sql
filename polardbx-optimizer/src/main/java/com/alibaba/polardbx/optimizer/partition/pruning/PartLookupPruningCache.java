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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartLookupPruningCache {

    /**
     * its params will be modified for each sharding
     */
    protected ExecutionContext dynamicParamsContext;
    protected PartitionInfo partInfo;
    protected LogicalView lookupLogicalView;
    protected int[] partCol2LookupSideInputRef;
    protected List<DataType> lookupValExprDataTypes;

    /**
     * the partition columns that exists in equi-join-keys
     */
    protected List<ColumnMeta> lookupPartColMetas;
    protected PartitionPruneStep pruningStep;
    protected List<ParameterContext> paramCtxList;

    protected boolean allowPerformInValuesPruning = false;

    /**
     * <pre>
     *
     *      case1: equi-join-keys contains all part-cols
     *          e.g: t1 partCols: c1,c2,c3
     *               joinKeys: t1.c1=t2.a and t1.c3=t2.c and t1.c2=t2.b and t1.d=t2.d
     *               pruning cols: t1.c1, t2.c2, t2.c3
     *               pruning step: c1=?1,c2=?2,c3=?3 ==> (c1,c2,c3) = (?1,?2,?3)
     *
     *      case2: equi-join-keys contains some prefix part-cols
     *          e.g: t1 partCols: c1,c2,c3
     *               joinKeys: t1.c1=t2.a and t1.c2=t2.b and t1.d=t2.d
     *               pruning cols: t1.c1, t2.c2
     *               pruning step: c1=?1,c2=?2  ==>  (?1,?2,min) <= (c1,c2) <= (?1,?2,max)
     *
     *      case3: equi-join-keys contains some non-prefix part-cols
     *          e.g: t1 partCols: c1,c2,c3
     *               joinKeys: t1.c2=t2.a and t1.c3=t2.b and t1.d=t2.d
     *               pruning cols: no cols, full scan
     *               pruning step: full scan step
     *
     *      case4: equi-join-keys contains not any part-cols (full scan)
     *          e.g: t1 partCols: c1,c2,c3
     *               joinKeys: t1.d=t2.d
     *               pruning cols: no cols, full scan
     *               pruning step: full scan step
     * </pre>
     */
    public PartLookupPruningCache(ExecutionContext dynamicParamsContext,
                                  PartitionInfo partInfo,
                                  LogicalView lv,
                                  List<ColumnMeta> lookupPartColMetas,
                                  int[] partCol2LookupSideInputRef,
                                  boolean shouldPerformInValuesPruning) {
        this.dynamicParamsContext = dynamicParamsContext.copy();
        this.partInfo = partInfo;
        this.lookupLogicalView = lv;
        this.lookupPartColMetas = lookupPartColMetas;
        this.lookupValExprDataTypes = new ArrayList<>();
        for (int i = 0; i < lookupPartColMetas.size(); i++) {
            DataType dt = lookupPartColMetas.get(i).getDataType();
            lookupValExprDataTypes.add(dt);
        }
        this.partCol2LookupSideInputRef = partCol2LookupSideInputRef;
        this.pruningStep = buildLookupPredPruneStep();
        this.allowPerformInValuesPruning = shouldPerformInValuesPruning;

    }



    protected PartitionPruneStep buildLookupPredPruneStep() {
        // build new step
        List<ColumnMeta> shardingKeyMetas = lookupPartColMetas;
        int skCnt = shardingKeyMetas.size();
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> equals = new ArrayList<>();
        for (int k = 0; k < skCnt; k++) {
            int partInputRef = partCol2LookupSideInputRef[k];
            RelDataType partColRelDataType =
                lookupLogicalView.getRowType().getFieldList().get(partCol2LookupSideInputRef[k]).getType();
            RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(partColRelDataType, k);
            RexNode equal = rexBuilder.makeCall(TddlOperatorTable.EQUALS,
                rexBuilder.makeInputRef(lookupLogicalView, partInputRef), dynamicParam);
            equals.add(equal);
        }
        RexNode condition;
        if (equals.size() == 1) {
            condition = equals.get(0);
        } else {
            condition = rexBuilder.makeCall(TddlOperatorTable.AND, equals);
        }
        PartitionPruneStep step =
            PartitionPruner.generatePartitionPrueStepInfo(partInfo, lookupLogicalView, condition, dynamicParamsContext);
        return step;
    }

    protected void prepareParamsContexts(List<Object> oneLookupVal) {
        /**
         * Prepare params contexts
         */
        Map<Integer, ParameterContext> newParams = new HashMap<>();
        paramCtxList = new ArrayList<>();
        for (int i = 0; i < oneLookupVal.size(); i++) {
            int dynamicIndex = i + 1;
            ParameterContext ctx = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                dynamicIndex, oneLookupVal.get(i)
            });
            newParams.put(dynamicIndex, ctx);
            paramCtxList.add(ctx);
        }
        dynamicParamsContext.getParams().setParams(newParams);
    }

    public PartPrunedResult doLookupPruning(List<Object> oneLookupVal) {
        /**
         * Prepare lookup dynamic params of context for pruneStep
         */
        refreshPruningParams(oneLookupVal);

        /**
         * Do pruning by using step
         */
        PartPrunedResult partPrunedResult = PartitionPruner.doPruningByStepInfo(pruningStep, dynamicParamsContext);

        return partPrunedResult;
    }

    protected void refreshPruningParams(List<Object> oneLookupVal) {
        if (paramCtxList == null) {
            prepareParamsContexts(oneLookupVal);
        } else {
            for (int i = 0; i < oneLookupVal.size(); i++) {
                paramCtxList.get(i).setValue(oneLookupVal.get(i));
            }
        }
    }

    public boolean isAllowPerformInValuesPruning() {
        return allowPerformInValuesPruning;
    }

}
