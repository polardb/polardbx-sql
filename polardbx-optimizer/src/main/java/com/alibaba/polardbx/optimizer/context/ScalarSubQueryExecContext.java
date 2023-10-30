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

package com.alibaba.polardbx.optimizer.context;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class ScalarSubQueryExecContext {

    /**
     * The execution result of the scalar subQuery
     */
    private volatile Object subQueryResult = null;

    /**
     * Label if the scalar subQuery return a max one row result
     */
    private volatile boolean maxOneRow = false;

    /**
     * <pre>
     * Label if the scalar subQuery is as the value of a InExpr, such as
     *    ... where (x,y,z) in (
     *                   select (a,b,c) from t1 order by a,b,c limit 5,100
     *              )
     * <pre/>
     */
    private volatile boolean subQueryInExpr = false;

    /**
     * <pre>
     * Label if the scalar subQuery return a multi-columns row expr, such as
     *    ... where (x,y,z) in (
     *                   select (a,b,c) from t1 order by a,b,c limit 5,100
     *              )
     * <pre/>
     */
    private volatile boolean returnMultiColRowExpr = false;

    /**
     * The row num of the next value to be pruning of current execution result of the scalar subQuery
     * <pre>
     *      For partitioned table, if a scalar subQuery is in a in-expr,such as
     *              ... where (x,y,z) in (
     *                   select (a,b,c) from t1 order by a,b,c limit 5,100
     *              )
     *      , it will be converted to the follows to perform pruning:
     *              foreach row values (v1,v2,v3) of the return of the scalar subQuery
     *                     set params(?1).val = v1;
     *                     set params(?2).val = v2;
     *                     set params(?3).val = v3;
     *                     partSet += pruning( a=params(?1) and params(?2) and params(?3) )
     *      , so the nextPruningRowNum is the row number of the value (v1,v2,v3) .
     * <pre/>
     */
    private volatile int nextPruningRowNum = -1;

    public ScalarSubQueryExecContext() {
    }

    public Object nextValue() {
        if (nextPruningRowNum == -1 || subQueryResult == null) {
            return null;
        }
        if (!(subQueryResult instanceof List)) {
            return subQueryResult;
        }
        Object tarVal = (((List) subQueryResult).get(nextPruningRowNum));
        return tarVal;
    }

    public Object getSubQueryResult() {
        return subQueryResult;
    }

    public ScalarSubQueryExecContext setSubQueryResult(Object subQueryResult) {
        this.subQueryResult = subQueryResult;
        return this;
    }

    public boolean isMaxOneRow() {
        return maxOneRow;
    }

    public void setMaxOneRow(boolean maxOneRow) {
        this.maxOneRow = maxOneRow;
    }

    public boolean isSubQueryInExpr() {
        return subQueryInExpr;
    }

    public void setSubQueryInExpr(boolean subQueryInExpr) {
        this.subQueryInExpr = subQueryInExpr;
    }

    public boolean isReturnMultiColRowExpr() {
        return returnMultiColRowExpr;
    }

    public void setReturnMultiColRowExpr(boolean returnMultiColRowExpr) {
        this.returnMultiColRowExpr = returnMultiColRowExpr;
    }

    public int getNextPruningRowNum() {
        return nextPruningRowNum;
    }

    public void setNextPruningRowNum(int nextPruningRowNum) {
        this.nextPruningRowNum = nextPruningRowNum;
    }

    @Override
    public String toString() {
        return "ScalarSubQueryExecContext{" +
            "subQueryResult=" + subQueryResult +
            ", maxOneRow=" + maxOneRow +
            ", subQueryInExpr=" + subQueryInExpr +
            ", returnMultiColRowExpr=" + returnMultiColRowExpr +
            ", nextPruningRowNum=" + nextPruningRowNum +
            '}';
    }
}
