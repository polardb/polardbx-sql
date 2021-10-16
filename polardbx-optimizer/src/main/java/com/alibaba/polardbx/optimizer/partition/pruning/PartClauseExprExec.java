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
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;

/**
 * The execution form fo partition clause expression
 *
 * @author chenghui.lch
 */
public class PartClauseExprExec {

    public static final PartClauseExprExec MAX_VAL = new PartClauseExprExec(PartitionBoundValueKind.DATUM_MAX_VALUE);

    public static final PartClauseExprExec MIN_VAL = new PartClauseExprExec(PartitionBoundValueKind.DATUM_MIN_VALUE);

    /**
     * A mock proxy to provide ExecutionContext when building IExpression for relNode
     */
    protected ExprContextProvider exprCtxHolder;

    /**
     * The exec form of predicate expr for part key i of part column prefix
     * <pre>
     *   e.g
     *       Assume that the part func is partition by range (year(gmtCreated))
     *       and the SearchExprInfo is "(gmtCreated) < DATEADD(day, 2, ?)" ,
     *       then the exec form of PartClauseInfo is
     *       the IExpression obj of DATEADD(day,2, ?)
     *
     * </pre>
     */
    protected IExpression exprExec;

    /**
     * The corresponding PartitionIntFunction of the part key that is referenced by target predicate expr
     * if a PartitionIntFunction is used to do partition.
     * <pre>
     *       Assume that the part func is partition by range (year(gmtCreated))
     *       and the SearchExprInfo is "(gmtCreated) < DATEADD(day, 2, ?)" ,
     *       then the corresponding PartitionIntFunction of curr PartClauseInfo is
     *       YEAR()
     * </pre>
     */
    protected PartitionIntFunction partIntFunc;

    /**
     * The return datatype of the predicate expr
     */
    protected DataType predExprReturnType;

    /**
     * The datatype of partition column definition
     */
    protected DataType partColDataType;

    protected PartitionBoundValueKind valueKind;

    /**
     * Label if the const expr is a always null value expr
     */
    protected boolean alwaysNullValue = false;

    protected PartClauseInfo clauseInfo;

    /**
     * Label if the const expr of PartClause is a dynamic expression only
     */
    protected boolean dynamicConstExprOnly = false;

    /**
     * For routing for query,  a expression may be calculated may times, so need open cache
     * For routing for insert, a expression will be calculated only once, so need NOT open cache
     */
    protected boolean needOpenEvalResultCache = true;

    /**
     * the Scenario （Query/Insert/DDL） of PartClauseExprExec
     */
    protected PartFieldAccessType partFldAccessType;

    public PartClauseExprExec(PartitionBoundValueKind valueKind) {
        this.valueKind = valueKind;
    }

    protected PartitionField evalPredExprVal(ExecutionContext context, PartPruneStepPruningContext pruningCtx,
                                             boolean[] fldEndpoints) {

        if (valueKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
            return null;
        }
        if (alwaysNullValue) {
            PartitionField nullFld = PartitionFieldBuilder.createField(DataTypes.LongType);
            nullFld.setNull(true);
            return nullFld;
        }
        boolean enableEvalCache = pruningCtx != null ? pruningCtx.isEnableConstExprEvalCache() : false;
        Integer constExprId = null;
        if (this.clauseInfo != null) {
            constExprId = this.clauseInfo.getConstExprId();

            /**
             * If const expr is a dynamic-params only expr, no need to cache const eval result.
             */
            boolean isDynamicParamOnly = this.clauseInfo.isDynamicConstOnly();
            enableEvalCache &= !isDynamicParamOnly;
        } else {
            enableEvalCache = false;
        }

        /**
         * Compute the const val for part predicate expr and build the part field
         */
        ConstExprEvalParams exprEvalParams = new ConstExprEvalParams();
        exprEvalParams.calcExpr = exprExec;
        exprEvalParams.needGetTypeFromDynamicExpr = dynamicConstExprOnly;
        exprEvalParams.exprReturnType = predExprReturnType;
        exprEvalParams.partColType = partColDataType;
        exprEvalParams.executionContext = context;
        exprEvalParams.pruningCtx = pruningCtx;
        exprEvalParams.fldEndpoints = fldEndpoints;
        exprEvalParams.accessType = this.partFldAccessType;
        exprEvalParams.constExprId = constExprId;
        exprEvalParams.needCacheEvalResult = needOpenEvalResultCache && enableEvalCache;
        PartitionField partField = PartitionPrunerUtils.evalExprValAndCache(exprEvalParams);
        return partField;
    }

    public ExprContextProvider getExprCtxHolder() {
        return exprCtxHolder;
    }

    public void setExprCtxHolder(ExprContextProvider exprCtxHolder) {
        this.exprCtxHolder = exprCtxHolder;
    }

    public IExpression getExprExec() {
        return exprExec;
    }

    public void setExprExec(IExpression exprExec) {
        this.exprExec = exprExec;
    }

    public PartitionBoundValueKind getValueKind() {
        return valueKind;
    }

    public void setValueKind(PartitionBoundValueKind valueKind) {
        this.valueKind = valueKind;
    }
    
    public void setAlwaysNullValue(boolean alwaysNullValue) {
        this.alwaysNullValue = alwaysNullValue;
    }

    public PartClauseInfo getClauseInfo() {
        return clauseInfo;
    }

    public void setClauseInfo(PartClauseInfo clauseInfo) {
        this.clauseInfo = clauseInfo;
    }

    public PartitionIntFunction getPartIntFunc() {
        return partIntFunc;
    }

    public void setPartIntFunc(PartitionIntFunction partIntFunc) {
        this.partIntFunc = partIntFunc;
    }

    public DataType getPredExprReturnType() {
        return predExprReturnType;
    }

    public void setPredExprReturnType(DataType predExprReturnType) {
        this.predExprReturnType = predExprReturnType;
    }

    public DataType getPartColDataType() {
        return partColDataType;
    }

    public void setPartColDataType(DataType partColDataType) {
        this.partColDataType = partColDataType;
    }

    public boolean isDynamicConstExprOnly() {
        return dynamicConstExprOnly;
    }

    public void setDynamicConstExprOnly(boolean dynamicConstExprOnly) {
        this.dynamicConstExprOnly = dynamicConstExprOnly;
    }

    public boolean isAlwaysNullValue() {
        return alwaysNullValue;
    }

    public boolean isNeedOpenEvalResultCache() {
        return needOpenEvalResultCache;
    }

    public void setNeedOpenEvalResultCache(boolean needOpenEvalResultCache) {
        this.needOpenEvalResultCache = needOpenEvalResultCache;
    }

    public PartFieldAccessType getPartFldAccessType() {
        return partFldAccessType;
    }

    public void setPartFldAccessType(PartFieldAccessType partFldAccessType) {
        this.partFldAccessType = partFldAccessType;
    }
}
