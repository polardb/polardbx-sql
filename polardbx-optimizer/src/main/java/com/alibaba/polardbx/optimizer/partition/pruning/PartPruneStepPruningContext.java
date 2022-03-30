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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartPruneStepPruningContext {

    /**
     * Allow to cache the eval result of const expr in partition predicate: such as 23+45
     */
    private boolean enableConstExprEvalCache = true;

    /**
     * Allow to auto merge intervals
     */
    private boolean enableAutoMergeIntervals = true;

    /**
     * Allow to do the enumeration for merged intervals
     */
    private boolean enableIntervalEnumeration = true;

    /**
     * The max length of the enumerable interval
     */
    private long maxEnumerableIntervalLength = PartitionPruneStepBuilder.DEFAULT_MAX_ENUMERABLE_INTERVAL_LENGTH;

    protected static class ExprEvalResult {
        protected Object rawVal;
        protected PartitionField valFld;
        protected boolean isNull = false;

        public ExprEvalResult(Object rawVal, PartitionField valFld) {
            this.rawVal = rawVal;
            this.valFld = valFld;
            this.isNull = rawVal == null;
        }
    }

    protected Map<Integer, ExprEvalResult> partClauseEvalResultCache = new HashMap<>();

    public static PartPruneStepPruningContext initPruningContext(ExecutionContext ec) {
        PartPruneStepPruningContext pruningContext = new PartPruneStepPruningContext();
        pruningContext.setEnableAutoMergeIntervals(
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_MERGE_INTERVALS_IN_PRUNING));
        pruningContext.setEnableIntervalEnumeration(
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_INTERVAL_ENUMERATION_IN_PRUNING));
        pruningContext.setEnableConstExprEvalCache(
            ec.getParamManager().getBoolean(ConnectionParams.ENABLE_CONST_EXPR_EVAL_CACHE));
        pruningContext.setMaxEnumerableIntervalLength(
            ec.getParamManager().getLong(ConnectionParams.MAX_ENUMERABLE_INTERVAL_LENGTH));
        return pruningContext;
    }

    public PartPruneStepPruningContext() {
    }

    public ExprEvalResult getEvalResult(Integer partClauseId) {
        return partClauseEvalResultCache.get(partClauseId);
    }

    public void putEvalResult(Integer partClauseId, ExprEvalResult result) {
        partClauseEvalResultCache.put(partClauseId, result);
    }
    
    public boolean isEnableConstExprEvalCache() {
        return enableConstExprEvalCache;
    }

    public void setEnableConstExprEvalCache(boolean enableConstExprEvalCache) {
        this.enableConstExprEvalCache = enableConstExprEvalCache;
    }

    public boolean isEnableAutoMergeIntervals() {
        return enableAutoMergeIntervals;
    }

    public void setEnableAutoMergeIntervals(boolean enableAutoMergeIntervals) {
        this.enableAutoMergeIntervals = enableAutoMergeIntervals;
    }

    public boolean isEnableIntervalEnumeration() {
        return enableIntervalEnumeration;
    }

    public void setEnableIntervalEnumeration(boolean enableIntervalEnumeration) {
        this.enableIntervalEnumeration = enableIntervalEnumeration;
    }

    public long getMaxEnumerableIntervalLength() {
        return maxEnumerableIntervalLength;
    }

    public void setMaxEnumerableIntervalLength(long maxEnumerableIntervalLength) {
        this.maxEnumerableIntervalLength = maxEnumerableIntervalLength;
    }

}
