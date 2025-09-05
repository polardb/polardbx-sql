/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlSelect extends SqlCall {
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int FROM_OPERAND = 2;
    public static final int WHERE_OPERAND = 3;
    public static final int HAVING_OPERAND = 5;

    public enum LockMode {
        UNDEF, SHARED_LOCK, EXCLUSIVE_LOCK;

        public static LockMode getLockMode(SqlNode sqlNOde) {
            LockMode lockMode = UNDEF;
            if (sqlNOde instanceof TDDLSqlSelect) {
                lockMode = ((TDDLSqlSelect) sqlNOde).getLockMode();
            }
            return lockMode;
        }
    }

    private LockMode lockMode = LockMode.UNDEF;

    OptimizerHint optimizerHint = new OptimizerHint();
    SqlNodeList keywordList;
    SqlNodeList selectList;
    SqlNode from;
    SqlNode where;
    SqlNodeList groupBy;
    SqlNode having;
    SqlNodeList windowDecls;
    SqlNodeList orderBy;
    SqlNode offset;
    SqlNode fetch;
    /**
     * computedFetch should be a SqlNumericLiteral
     * while fetch is a SqlBasicCall with SqlDynamicParam
     */
    SqlNode computedFetch;
    SqlMatchRecognize matchRecognize;
    OutFileParams outFileParams;

    //~ Constructors -----------------------------------------------------------

    public SqlSelect(SqlParserPos pos,
                     SqlNodeList keywordList,
                     SqlNodeList selectList,
                     SqlNode from,
                     SqlNode where,
                     SqlNodeList groupBy,
                     SqlNode having,
                     SqlNodeList windowDecls,
                     SqlNodeList orderBy,
                     SqlNode offset,
                     SqlNode fetch) {
        super(pos);
        this.keywordList = Preconditions.checkNotNull(keywordList != null
            ? keywordList : new SqlNodeList(pos));
        this.selectList = selectList;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windowDecls = Preconditions.checkNotNull(windowDecls != null
            ? windowDecls : new SqlNodeList(pos));
        this.orderBy = orderBy;
        this.offset = offset;
        this.fetch = fetch;
    }

    public SqlSelect(SqlParserPos pos,
                     SqlNodeList keywordList,
                     SqlNodeList selectList,
                     SqlNode from,
                     SqlNode where,
                     SqlNodeList groupBy,
                     SqlNode having,
                     SqlNodeList windowDecls,
                     SqlNodeList orderBy,
                     SqlNode offset,
                     SqlNode fetch,
                     OutFileParams outFileParams) {
        super(pos);
        this.keywordList = Preconditions.checkNotNull(keywordList != null
            ? keywordList : new SqlNodeList(pos));
        this.selectList = selectList;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windowDecls = Preconditions.checkNotNull(windowDecls != null
            ? windowDecls : new SqlNodeList(pos));
        this.orderBy = orderBy;
        this.offset = offset;
        this.fetch = fetch;
        this.outFileParams = outFileParams;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlOperator getOperator() {
        return SqlSelectOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SELECT;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(keywordList, selectList, from, where,
            groupBy, having, windowDecls, orderBy, offset, fetch);
    }

    /**
     * switch position of offset and fetch, cause different order is
     * accepted by parametrize and physical sql generation
     */
    public List<SqlNode> getParameterizableOperandList() {
        if (isDynamicFetch()) {
            return ImmutableNullableList.of(keywordList, selectList, from, where,
                groupBy, having, windowDecls, orderBy, computedFetch, offset);
        } else {
            return ImmutableNullableList.of(keywordList, selectList, from, where,
                groupBy, having, windowDecls, orderBy, fetch, offset);
        }
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
        case 0:
            keywordList = Preconditions.checkNotNull((SqlNodeList) operand);
            break;
        case 1:
            selectList = (SqlNodeList) operand;
            break;
        case 2:
            from = operand;
            break;
        case 3:
            where = operand;
            break;
        case 4:
            groupBy = (SqlNodeList) operand;
            break;
        case 5:
            having = operand;
            break;
        case 6:
            windowDecls = Preconditions.checkNotNull((SqlNodeList) operand);
            break;
        case 7:
            orderBy = (SqlNodeList) operand;
            break;
        case 8:
            offset = operand;
            break;
        case 9:
            fetch = operand;
            break;
        default:
            throw new AssertionError(i);
        }
    }

    public final boolean isDistinct() {
        return getModifierNode(SqlSelectKeyword.DISTINCT) != null;
    }

    public final SqlNode getModifierNode(SqlSelectKeyword modifier) {
        for (SqlNode keyword : keywordList) {
            SqlSelectKeyword keyword2 =
                ((SqlLiteral) keyword).symbolValue(SqlSelectKeyword.class);
            if (keyword2 == modifier) {
                return keyword;
            }
        }
        return null;
    }

    public final SqlNode getFrom() {
        return from;
    }

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    public final SqlNodeList getGroup() {
        return groupBy;
    }

    public void setGroupBy(SqlNodeList groupBy) {
        this.groupBy = groupBy;
    }

    public final SqlNode getHaving() {
        return having;
    }

    public void setHaving(SqlNode having) {
        this.having = having;
    }

    public final SqlNodeList getSelectList() {
        return selectList;
    }

    public void setSelectList(SqlNodeList selectList) {
        this.selectList = selectList;
    }

    public final SqlNode getWhere() {
        return where;
    }

    public void setWhere(SqlNode whereClause) {
        this.where = whereClause;
    }

    @Nonnull
    public final SqlNodeList getWindowList() {
        return windowDecls;
    }

    public final SqlNodeList getOrderList() {
        return orderBy;
    }

    public void setOrderBy(SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    public final SqlNode getOffset() {
        return offset;
    }

    public void setOffset(SqlNode offset) {
        this.offset = offset;
    }

    public final SqlNode getFetch() {
        return fetch;
    }

    public void setFetch(SqlNode fetch) {
        this.fetch = fetch;
    }

    /**
     * computed fetch should be set only once as a DynamicParam
     * if it is set concurrently, make sure the param index is identical
     */
    public void setComputedFetch(SqlNode computedFetch) {
        if (this.computedFetch != null) {
            Preconditions.checkArgument(((SqlDynamicParam) this.computedFetch).index ==
                ((SqlDynamicParam) computedFetch).index, "Computed fetch should be set at the exact same index");
        } else {
            this.computedFetch = computedFetch;
        }
    }

    public SqlNode getComputedFetch() {
        return computedFetch;
    }

    public boolean isDynamicFetch() {
        return fetch != null && fetch.getKind() == SqlKind.PLUS && computedFetch != null;
    }

    public SqlMatchRecognize getMatchRecognize() {
        return matchRecognize;
    }

    public void setMatchRecognize(SqlMatchRecognize matchRecognize) {
        this.matchRecognize = matchRecognize;
    }

    public OutFileParams getOutFileParams() {
        return outFileParams;
    }

    public void setOutFileParams(OutFileParams outFileParams) {
        this.outFileParams = outFileParams;
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    // Override SqlCall, to introduce a sub-query frame.
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!writer.inQuery()) {
            // If this SELECT is the topmost item in a sub-query, introduce a new
            // frame. (The topmost item in the sub-query might be a UNION or
            // ORDER. In this case, we don't need a wrapper frame.)
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
            getOperator().unparse(writer, this, 0, 0);
            if (this.lockMode == LockMode.EXCLUSIVE_LOCK) {
                writer.print("FOR UPDATE");
            } else if (this.lockMode == LockMode.SHARED_LOCK) {
                writer.print("LOCK IN SHARE MODE");
            }
            writer.endList(frame);
        } else {
            getOperator().unparse(writer, this, leftPrec, rightPrec);
            if (this.lockMode == LockMode.EXCLUSIVE_LOCK) {
                writer.print("FOR UPDATE");
            } else if (this.lockMode == LockMode.SHARED_LOCK) {
                writer.print("LOCK IN SHARE MODE");
            }
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        final List<SqlNode> operandList = getOperandList();
        SqlNode sqlNode = getOperator().createCall(getFunctionQuantifier(), pos,
            operandList.toArray(new SqlNode[operandList.size()]));
        if (sqlNode instanceof SqlSelect) {
            ((SqlSelect) sqlNode).setLockMode(lockMode);
            ((SqlSelect) sqlNode).optimizerHint = optimizerHint.clone(SqlParserPos.ZERO);
        }
        return sqlNode;
    }

    public boolean hasOrderBy() {
        return orderBy != null && orderBy.size() != 0;
    }

    public boolean hasLimit() {
        return offset != null || fetch != null;
    }

    public boolean hasWhere() {
        return where != null;
    }

    public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
        return getModifierNode(targetKeyWord) != null;
    }

    public boolean withLock() {
        return lockMode != LockMode.UNDEF;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = lockMode;
    }

    public SqlSelect shallowClone(LockMode lockMode) {
        SqlSelect sqlSelect =
            new SqlSelect(this.pos, this.keywordList, this.selectList, this.from, this.where, this.groupBy,
                this.having, this.windowDecls, this.orderBy, this.offset, this.fetch);
        sqlSelect.setLockMode(lockMode);
        return sqlSelect;
    }

    public OptimizerHint getOptimizerHint() {
        return optimizerHint;
    }

    public void setOptimizerHint(OptimizerHint optimizerHint) {
        this.optimizerHint = optimizerHint;
    }

}

// End SqlSelect.java
