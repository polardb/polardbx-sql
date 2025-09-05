package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author luoyanxin.pt
 */
public class SqlCheckTableGroup extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlCheckTableGroupOperator();
    public final SqlNode where;
    public final SqlNode orderBy;
    public final SqlNode limit;
    public final SqlSelect fakeSelect;

    private List<SqlNode> tableGroupNames;

    public SqlCheckTableGroup(SqlParserPos pos, List<SqlNode> tableNames,
                              SqlNode where, SqlNode orderBy,
                              SqlNode limit) {
        super(pos);
        this.tableGroupNames = tableNames;

        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        final List<SqlNode> tmpOperands = new ArrayList<>();
        List<SqlSpecialIdentifier> specialIdentifiers = ImmutableList.of(SqlSpecialIdentifier.CHECK_TABLEGROUP);
        for (SqlSpecialIdentifier specialIdentifier : specialIdentifiers) {
            tmpOperands.add(SqlLiteral.createSymbol(specialIdentifier, SqlParserPos.ZERO));
        }
        tmpOperands.addAll(operands);
        this.operands = tmpOperands;
        if (null != orderBy || null != limit || null != where) {
            this.fakeSelect = doConvertToSelect();
        } else {
            this.fakeSelect = null;
        }
    }

    public List<SqlNode> getTableGroupNames() {
        return tableGroupNames;
    }

    public void setTableGroupNames(List<SqlNode> tableGroupNames) {
        this.tableGroupNames = tableGroupNames;
    }

    public SqlNode getWhere() {
        return where;
    }

    public SqlNode getOrderBy() {
        return orderBy;
    }

    public SqlNode getLimit() {
        return limit;
    }

    public SqlSelect getFakeSelect() {
        return fakeSelect;
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
            new SqlNodeList(ImmutableList.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
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
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (null != fakeSelect) {
            validator.validateQuery(fakeSelect, scope, validator.getUnknownType());
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        LinkedList<SqlNode> operandList = new LinkedList<>();
        if (operands.size() > 0) {
            operandList.addAll(operands);
        }
        if (null != where) {
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
        writer.sep("CHECK TABLEGROUP");

        if (tableGroupNames.size() > 0) {
            tableGroupNames.get(0).unparse(writer, leftPrec, rightPrec);
            for (int index = 1; index < tableGroupNames.size(); index++) {
                writer.print(", ");
                tableGroupNames.get(index).unparse(writer, leftPrec, rightPrec);
            }
        }
        unparseSearchCondition(writer, leftPrec, rightPrec);
        writer.endList(selectFrame);
    }

    protected void unparseSearchCondition(SqlWriter writer, int leftPrec, int rightPrec) {
        if (null != where) {
            writer.sep("WHERE");
            where.unparse(writer, leftPrec, rightPrec);
        }
        if (null != orderBy) {
            writer.sep("ORDER BY");
            orderBy.unparse(writer, leftPrec, rightPrec);
        }

        if (null != limit) {
            writer.sep("LIMIT");
            limit.unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_TABLEGROUP;
    }

    public static class SqlCheckTableGroupOperator extends SqlSpecialOperator {

        public SqlCheckTableGroupOperator() {
            super("CHECK_TABLEGROUP", SqlKind.CHECK_TABLEGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("TableGroup", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("match_tables", 1, typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            columns.add(
                new RelDataTypeFieldImpl("total_tables", 2, typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            columns.add(new RelDataTypeFieldImpl("Status", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckTableGroup(this.pos, tableGroupNames, where, orderBy, limit);
    }
}
