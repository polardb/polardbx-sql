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

import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * The clause info of a partition predicate expression
 *
 * @author chenghui.lch
 */
public class PartClauseInfo {

    protected Integer id;

    /**
     * the operator of part predicate
     */
    protected SqlOperator op;

    /**
     * the kind of part predicate
     */
    protected SqlKind opKind;

    /**
     * The rel row type of plan which is referred by constExpr and input
     */
    protected RelDataType planRelRowType;

    /**
     * the input of part predicate
     */
    protected RexNode input;

    /**
     * the const val of part predicate
     */
    protected RexNode constExpr;
    /**
     * a id to label constExpr uniquely to avoiding computing the constExpr repeatedly
     */
    protected Integer constExprId;

    /**
     * the original predicate of curr PartClauseInfo
     */
    protected RexNode originalPredicate;

    /**
     * The flag that label if the constExpr is null value
     */
    protected boolean isNull = false;

    /**
     * The index of const expr in tuple, used by insert
     */
    protected int indexInTuple;

    /**
     * the part level that the col of input belong to
     */
    protected PartKeyLevel partKeyLevel;

    /**
     * the part strategy that the col of input belong to
     */
    protected PartitionStrategy strategy;

    /**
     * the part key index that the col of input in part key, the index is start from 0.
     */
    protected int partKeyIndex;

    /**
     * the field def of the part key
     */
    protected RelDataType partKeyDataType;

    /**
     * label if const expr is a dynamic expression only, such as pk=?
     */
    protected boolean isDynamicConstOnly = false;

    protected boolean isSubQueryInExpr = false;

    /**
     * Label if const expr is a virtual expr that col= anyValues
     */
    protected boolean isAnyValueEqCond = false;

    protected PartClauseInfo() {
    }

    public SqlKind getOpKind() {
        return opKind;
    }

    public void setOpKind(SqlKind opKind) {
        this.opKind = opKind;
    }

    public RexNode getInput() {
        return input;
    }

    public void setInput(RexNode input) {
        this.input = input;
    }

    public RexNode getConstExpr() {
        return constExpr;
    }

    public void setConstExpr(RexNode constExpr) {
        this.constExpr = constExpr;
    }

    public int getIndexInTuple() {
        return indexInTuple;
    }

    public void setIndexInTuple(int indexInTuple) {
        this.indexInTuple = indexInTuple;
    }

    public PartKeyLevel getPartKeyLevel() {
        return partKeyLevel;
    }

    public void setPartKeyLevel(PartKeyLevel partKeyLevel) {
        this.partKeyLevel = partKeyLevel;
    }

    public int getPartKeyIndex() {
        return partKeyIndex;
    }

    public void setPartKeyIndex(int partKeyIndex) {
        this.partKeyIndex = partKeyIndex;
    }

    public RelDataType getPartKeyDataType() {
        return partKeyDataType;
    }

    public void setPartKeyDataType(RelDataType partKeyDataType) {
        this.partKeyDataType = partKeyDataType;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Get the digest that desc the predicate expr of PartClauseInfo
     */
    protected String getDigest() {
        return getDigest(false);
    }

    protected String getDigest(boolean isIgnoreOp) {
        StringBuilder sb = new StringBuilder("");
        sb.append(!isIgnoreOp ? opKind.sql : "op");
        sb.append("(");
        sb.append(input.toString());
        sb.append(",");
        sb.append(isAnyValueEqCond ? "any" : isNull ? "null" : constExpr.toString());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public SqlOperator getOp() {
        return op;
    }

    public void setOp(SqlOperator op) {
        this.op = op;
    }

    public RexNode getOriginalPredicate() {
        return originalPredicate;
    }

    public void setOriginalPredicate(RexNode originalPredicate) {
        this.originalPredicate = originalPredicate;
    }

    public boolean isDynamicConstOnly() {
        return isDynamicConstOnly;
    }

    public void setDynamicConstOnly(boolean dynamicConstOnly) {
        isDynamicConstOnly = dynamicConstOnly;
    }

    public Integer getConstExprId() {
        return constExprId;
    }

    public void setConstExprId(Integer constExprId) {
        this.constExprId = constExprId;
    }

    public boolean isSubQueryInExpr() {
        return isSubQueryInExpr;
    }

    public void setSubQueryInExpr(boolean subQueryInExpr) {
        isSubQueryInExpr = subQueryInExpr;
    }

    public boolean isAnyValueEqCond() {
        return isAnyValueEqCond;
    }

    public void setAnyValueEqCond(boolean anyValueEqCond) {
        isAnyValueEqCond = anyValueEqCond;
    }

    public RelDataType getPlanRelRowType() {
        return planRelRowType;
    }

    public void setPlanRelRowType(RelDataType planRelRowType) {
        this.planRelRowType = planRelRowType;
    }
}
