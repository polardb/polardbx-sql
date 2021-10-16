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

/**
 * @author chenghui.lch
 */
public class SearchExprInfo {

    /**
     * The partition number of all partition columns
     */
    protected Integer partColNum;

    /**
     * The exec form of the part clause from partition predicate
     */
    protected PartClauseExprExec[] exprExecArr;

    /**
     * the comparison of partition predicate expr in search space of query
     */
    protected ComparisonKind cmpKind;
    
    public SearchExprInfo(PartClauseExprExec[] exprExecArr, ComparisonKind cmpKind) {

        int partColNum = exprExecArr.length;
        this.partColNum = partColNum;
        this.cmpKind = cmpKind;
        this.exprExecArr = exprExecArr;
    }

    public PartClauseExprExec[] getExprExecArr() {
        return exprExecArr;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }

    public SearchExprInfo copy() {
        SearchExprInfo exprInfo = new SearchExprInfo(this.exprExecArr, this.cmpKind);
        return exprInfo;
    }

    public void setCmpKind(ComparisonKind cmpKind) {
        this.cmpKind = cmpKind;
    }
}
