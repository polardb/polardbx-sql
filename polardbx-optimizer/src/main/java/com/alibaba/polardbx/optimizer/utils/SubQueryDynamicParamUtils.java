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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ScalarSubQueryExecContext;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.Map;

/**
 * @author chenghui.lch
 */
public class SubQueryDynamicParamUtils {

    private static IScalarSubqueryExecHelper scalarSubQueryExecHelper;

    public static void initScalarSubQueryExecHelper(IScalarSubqueryExecHelper scalarSubqueryExecHelper) {
        if (SubQueryDynamicParamUtils.scalarSubQueryExecHelper == null) {
            SubQueryDynamicParamUtils.scalarSubQueryExecHelper = scalarSubqueryExecHelper;
        }
    }

    /**
     * Save the result of scalar subQuery into ScalarSubQueryExecContext
     */
    public static boolean saveScalarSubQueryComputedValue(Map<Integer, ScalarSubQueryExecContext> scalarSubQueryCtxMaps,
                                                          RexDynamicParam subQueryDynamicParam,
                                                          Object subQueryValue
    ) {
        if (subQueryDynamicParam.getIndex() != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
            return false;
        }

        Integer relatedId = subQueryDynamicParam.getRel().getRelatedId();
        ScalarSubQueryExecContext ctx = new ScalarSubQueryExecContext();
        ctx.setMaxOneRow(subQueryDynamicParam.isMaxOnerow());
        ctx.setReturnMultiColRowExpr(false);

        /**
         * Use convertToDynamicValue to process null value
         */
        Object sbResult =  convertToDynamicValue(subQueryValue);

        ctx.setSubQueryResult(sbResult);
        scalarSubQueryCtxMaps.put(relatedId, ctx);
        return true;
    }

    private static Object convertToDynamicValue(Object rawValue) {
        Object dynamicFinalValue = null;
        if (rawValue == null) {
            dynamicFinalValue = RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY;
        } else if (rawValue instanceof Slice) {
            dynamicFinalValue = ((Slice) rawValue).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
        } else if (rawValue instanceof UInt64) {
            dynamicFinalValue = ((UInt64) rawValue).toBigInteger();
        } else {
            dynamicFinalValue = rawValue;
        }
        return dynamicFinalValue;
    }

    public static boolean tryExecAndFetchScalarSubQueryConstantValue(RexDynamicParam sbRex, ExecutionContext ec, Object[] valueResults) {
        IScalarSubqueryExecHelper execHelper = SubQueryDynamicParamUtils.scalarSubQueryExecHelper;
        if (execHelper == null) {
            return false;
        }
        ExecutionContext tmpEc = ec.copy();
        execHelper.buildScalarSubqueryValue(ImmutableList.of(sbRex), tmpEc);
        return fetchScalarSubQueryConstantValue(sbRex, tmpEc.getScalarSubqueryCtxMap(), false, valueResults);
    }

    /**
     * When
     *  index >= 0, it means the content of RexDynamicParam is the params value can be fetched from ExecutionContext
     *  index = -1, it means the content of RexDynamicParam is phy table name;
     *  index = -2, it means the content of RexDynamicParam is scalar subquery;
     *  index = -3, it means the content of RexDynamicParam is apply subquery.
     */
    /**
     * @return true if fetch successfully, or else return false
     */
    public static boolean fetchScalarSubQueryConstantValue(Object scalarSubQueryRex,
                                                           Map<Integer, ScalarSubQueryExecContext> scalarSubQueryCtxMaps,
                                                           boolean forceMaxOneRow,
                                                           Object[] valueResults) {
        if (!(scalarSubQueryRex instanceof RexDynamicParam)) {
            return false;
        }
        RexDynamicParam scalarSubQueryParam = (RexDynamicParam) scalarSubQueryRex;
        if (scalarSubQueryParam.getIndex() != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
            return false;
        }
        if (forceMaxOneRow && !scalarSubQueryParam.isMaxOnerow()) {
            return false;
        }
        RelNode sbRel = scalarSubQueryParam.getRel();
        Enum valType = scalarSubQueryParam.getDynamicType();

        int relatedId = sbRel.getRelatedId();
        ScalarSubQueryExecContext sbExecCtx = scalarSubQueryCtxMaps.get(relatedId);
        if (sbExecCtx == null) {
            return false;
        }

        Object val;
        if (valType == RexDynamicParam.DYNAMIC_TYPE_VALUE.SUBQUERY_TEMP_VAR) {
            val = sbExecCtx.nextValue();
        } else {
            val  = sbExecCtx.getSubQueryResult();
        }

        if (val == null) {
            return false;
        }

        if (val == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
            /**
             * The query result of subquery is empty
             */
            valueResults[0] = null;
        } else {
            valueResults[0] = val;
        }
        return true;
    }

    public static boolean isNonMaxOneRowScalarSubQueryConstant(RexNode rexNode) {
        if (!isScalarSubQueryConstant(rexNode)) {
            return false;
        }
        return !((RexDynamicParam) rexNode).isMaxOnerow();
    }

    public static boolean isMaxOneRowScalarSubQueryConstant(RexNode rexNode) {
        if (!isScalarSubQueryConstant(rexNode)) {
            return false;
        }
        return ((RexDynamicParam) rexNode).isMaxOnerow();
    }

    public static boolean isScalarSubQueryConstant(RexNode rexNode) {
        if (!(rexNode instanceof RexDynamicParam)) {
            return false;
        }
        RexDynamicParam dynamicParam = (RexDynamicParam) rexNode;
        if (dynamicParam.getIndex() != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
            return false;
        }
        if (dynamicParam.getDynamicType() != RexDynamicParam.DYNAMIC_TYPE_VALUE.DEFAULT) {
            return false;
        }
        return true;
    }

    public static boolean isApplySubQueryConstant(RexNode rexNode) {
        if (!(rexNode instanceof RexDynamicParam)) {
            return false;
        }
        RexDynamicParam dynamicParam = (RexDynamicParam) rexNode;
        if (dynamicParam.getIndex() != PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
            return false;
        }
        return true;
    }

}
