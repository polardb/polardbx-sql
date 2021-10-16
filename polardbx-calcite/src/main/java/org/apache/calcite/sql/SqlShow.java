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

package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class SqlShow extends SqlDal {

    protected static final SqlSpecialOperator OPERATOR = new SqlShowOperator();

    public final SqlNodeList selectList;
    public final SqlNode like;
    public final SqlNode where;
    public final SqlNode orderBy;
    public final SqlNode limit;

    public final SqlSelect fakeSelect;

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers) {
        this(pos,
            specialIdentifiers,
            ImmutableList.<SqlNode>of(),
            SqlNodeList.EMPTY,
            null,
            null,
            null,
            null,
            ImmutableList.<Integer>of(),
            ImmutableList.<Integer>of(),
            ImmutableList.<Boolean>of());
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands) {
        this(pos,
            specialIdentifiers,
            operands,
            SqlNodeList.EMPTY,
            null,
            null,
            null,
            null,
            ImmutableList.<Integer>of(),
            ImmutableList.<Integer>of(),
            ImmutableList.<Boolean>of());
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                   SqlNode like, SqlNode where) {
        this(pos,
            specialIdentifiers,
            operands,
            SqlNodeList.EMPTY,
            like,
            where,
            null,
            null,
            ImmutableList.<Integer>of(),
            ImmutableList.<Integer>of(),
            ImmutableList.<Boolean>of());
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                   SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        this(pos,
            specialIdentifiers,
            operands,
            SqlNodeList.EMPTY,
            like,
            where,
            orderBy,
            limit,
            ImmutableList.<Integer>of(),
            ImmutableList.<Integer>of(),
            ImmutableList.<Boolean>of());
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                   SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, Integer tableIndex) {
        this(pos,
            specialIdentifiers,
            operands,
            SqlNodeList.EMPTY,
            like,
            where,
            orderBy,
            limit,
            ImmutableList.of(tableIndex),
            ImmutableList.<Integer>of(),
            ImmutableList.<Boolean>of());
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                   SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, Integer tableIndex, Integer dbIndex,
                   Boolean dbWithFrom) {
        this(pos,
            specialIdentifiers,
            operands,
            SqlNodeList.EMPTY,
            like,
            where,
            orderBy,
            limit,
            tableIndex < 0 ? ImmutableList.<Integer>of() : ImmutableList.of(tableIndex),
            dbIndex < 0 ? ImmutableList.<Integer>of() : ImmutableList.of(dbIndex),
            dbIndex < 0 ? ImmutableList.<Boolean>of() : ImmutableList.of(dbWithFrom));
    }

    public SqlShow(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                   SqlNodeList selectList, SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit,
                   List<Integer> tableIndex,
                   List<Integer> dbIndex, List<Boolean> dbWithFrom) {
        super(pos);
        this.selectList = selectList;
        this.like = like;
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.tableIndexes = tableIndex;
        this.dbIndexes = dbIndex;
        this.dbWithFrom = dbWithFrom;

        final List<SqlNode> tmpOperands = new ArrayList<>();
        for (SqlSpecialIdentifier specialIdentifier : specialIdentifiers) {
            tmpOperands.add(SqlLiteral.createSymbol(specialIdentifier, SqlParserPos.ZERO));
        }
        tmpOperands.addAll(operands);

        this.operands = tmpOperands;

        if (logicalShowWithUpperNode()) {
            fakeSelect = doConvertToSelect();
        } else {
            fakeSelect = null;
        }
    }

    private boolean logicalShowWithUpperNode() {
        return getShowKind().belongsTo(SqlKind.LOGICAL_SHOW_QUERY)
            && (null != orderBy || null != limit || null != where || !SqlNodeList.isEmptyList(selectList));
    }

    public boolean canConvertToSelect() {
        return false;
    }

    public SqlSelect convertToSelect() {
        throw new UnsupportedOperationException(getClass() + " can't convert to sql select!");
    }

    protected SqlSelect doConvertToSelect() {
        SqlNode offset = null;
        SqlNode fetch = null;
        if (limit != null) {
            offset = ((SqlNodeList) limit).get(0);
            fetch = ((SqlNodeList) limit).get(1);
        }

        return new SqlSelect(SqlParserPos.ZERO,
            null,
            SqlNodeList.isEmptyList(selectList) ?
                new SqlNodeList(ImmutableList.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO) :
                selectList,
            this,
            where,
            null,
            null,
            null,
            (SqlNodeList) orderBy,
            offset,
            fetch);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        LinkedList<SqlNode> operandList = new LinkedList<>();
        if (operands.size() > 0) {
            operandList.addAll(operands);
        }
        if (null != like) {
            operandList.add(like);
        } else if (null != where) {
            operandList.add(where);

            if (null != orderBy) {
                operandList.add(orderBy);
            }
            if (null != limit) {
                operandList.add(limit);
            }
        }
        return operandList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SHOW");

        if (null != this.operands) {
            for (SqlNode operand : this.operands) {
                if (null != operand) {
                    operand.unparse(writer, leftPrec, rightPrec);
                }
            }
        }

        if (selectList.size() != 0) {
            selectList.unparse(writer, leftPrec, rightPrec);
        }

        unparseSearchCondition(writer, leftPrec, rightPrec);

        writer.endList(selectFrame);
    }

    protected void unparseSearchCondition(SqlWriter writer, int leftPrec, int rightPrec) {
        if (null != like) {
            writer.sep("LIKE");
            like.unparse(writer, leftPrec, rightPrec);
        } else if (null != where && showWhere()) {
            writer.sep("WHERE");
            where.unparse(writer, leftPrec, rightPrec);
        }

        if (showSort()) {
            if (null != orderBy) {
                writer.sep("ORDER BY");
                orderBy.unparse(writer, leftPrec, rightPrec);
            }

            if (null != limit) {
                writer.sep("LIMIT");
                limit.unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    protected boolean showWhere() {
        return true;
    }

    protected boolean showSort() {
        return showWhere();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SHOW;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (null != fakeSelect) {
            validator.validateQuery(fakeSelect, scope, validator.getUnknownType());
        }
    }

    public SqlKind getShowKind() {
        return SqlKind.SHOW;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlShow(pos,
            ImmutableList.<SqlSpecialIdentifier>of(),
            operands,
            selectList,
            like,
            where,
            orderBy,
            limit,
            tableIndexes,
            dbIndexes,
            dbWithFrom);
    }

    public SqlNode clone(SqlParserPos pos, SqlNode newWhere) {
        return new SqlShow(pos,
            ImmutableList.<SqlSpecialIdentifier>of(),
            operands,
            selectList,
            like,
            newWhere,
            orderBy,
            limit,
            tableIndexes,
            dbIndexes,
            dbWithFrom);
    }

    public SqlNode removeLWOL(SqlParserPos pos) {
        return new SqlShow(pos,
            ImmutableList.<SqlSpecialIdentifier> of(),
            operands,
            selectList,
            null,
            null,
            null,
            null,
            tableIndexes,
            dbIndexes,
            dbWithFrom);
    }

    public static class SqlShowOperator extends SqlSpecialOperator {

        public SqlShowOperator() {
            super("SHOW", SqlKind.SHOW);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("SHOW_RESULT",
                    0,
                    columnType)));
        }
    }
}
