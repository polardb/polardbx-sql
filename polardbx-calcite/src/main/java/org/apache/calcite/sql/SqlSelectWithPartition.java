package org.apache.calcite.sql;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlSelectWithPartition extends SqlNode {
    SqlSelect select;
    SqlPhysicalPartition physicalPartition;

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    public SqlSelectWithPartition(SqlSelect select, SqlPhysicalPartition physicalPartition, SqlParserPos pos) {
        super(pos);
        this.select = select;
        this.physicalPartition = physicalPartition;
    }

    public static SqlSelectWithPartition create(SqlSelect sqlSelect, String physicalPartition) {
        return new SqlSelectWithPartition(
            sqlSelect,
            new SqlPhysicalPartition(new SqlIdentifier(physicalPartition, SqlParserPos.ZERO), SqlParserPos.ZERO),
            SqlParserPos.ZERO
        );
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        SqlSelect sqlSelect = (SqlSelect) this.select.clone(pos);
        SqlPhysicalPartition sqlPhysicalPartition = (SqlPhysicalPartition) this.physicalPartition.clone(pos);
        return new SqlSelectWithPartition(sqlSelect, sqlPhysicalPartition, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (!writer.inQuery()) {
            // If this SELECT is the topmost item in a sub-query, introduce a new
            // frame. (The topmost item in the sub-query might be a UNION or
            // ORDER. In this case, we don't need a wrapper frame.)
            final SqlWriter.Frame frame =
                writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
            doUnParse(writer, 0, 0);
            writer.endList(frame);
        } else {
            doUnParse(writer, leftPrec, rightPrec);
        }
        if (this.select.getLockMode() == SqlSelect.LockMode.EXCLUSIVE_LOCK) {
            writer.print("FOR UPDATE");
        } else if (this.select.getLockMode() == SqlSelect.LockMode.SHARED_LOCK) {
            writer.print("LOCK IN SHARE MODE");
        }
    }

    public void doUnParse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        SqlSelect select = this.select;
        final SqlWriter.Frame selectFrame =
            writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SELECT");
        if (select.getOptimizerHint() != null) {
            select.getOptimizerHint().unparse(writer, leftPrec, rightPrec);
        }
        for (int i = 0; i < select.keywordList.size(); i++) {
            final SqlNode keyword = select.keywordList.get(i);
            keyword.unparse(writer, 0, 0);
        }

        SqlNode selectClause = select.selectList;
        if (selectClause == null) {
            selectClause = SqlIdentifier.star(SqlParserPos.ZERO);
        }
        final SqlWriter.Frame selectListFrame =
            writer.startList(SqlWriter.FrameTypeEnum.SELECT_LIST);
        SqlSelectOperator.INSTANCE.unparseListClause(writer, selectClause);
        writer.endList(selectListFrame);

        if (SqlSelectOperator.INSTANCE.isDualTable(select, writer)) {
            SqlSelect newSelect = new TDDLSqlSelect(SqlParserPos.ZERO,
                null,
                new SqlNodeList(Lists.newArrayList(new SqlLiteral(1, SqlTypeName.INTEGER, SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
            select.from = new SqlBasicCall(new SqlAsOperator(),
                new SqlNode[] {newSelect, new SqlIdentifier("DUAL", SqlParserPos.ZERO)},
                SqlParserPos.ZERO);
        }

        if (SqlSelectOperator.INSTANCE.needUnparseFrom(select.from, writer)) {
            // Calcite SQL requires FROM but MySQL does not.
            writer.sep("FROM");

            // for FROM clause, use precedence just below join operator to make
            // sure that an un-joined nested select will be properly
            // parenthesized
            final SqlWriter.Frame fromFrame =
                writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).pushOptStack(SqlWriter.FrameTypeEnum.FROM_LIST);
            }
            select.from.unparse(
                writer,
                SqlJoin.OPERATOR.getLeftPrec() - 1,
                SqlJoin.OPERATOR.getRightPrec() - 1);

            if (this.physicalPartition != null) {
                this.physicalPartition.unparse(writer, 0, 0);
            }

            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).popOptStack();
            }
            writer.endList(fromFrame);
        }

        if (select.where != null) {
            writer.sep("WHERE");

            if (!writer.isAlwaysUseParentheses()) {
                SqlNode node = select.where;

                // decide whether to split on ORs or ANDs
                SqlKind whereSepKind = SqlKind.AND;
                if ((node instanceof SqlCall)
                    && node.getKind() == SqlKind.OR) {
                    whereSepKind = SqlKind.OR;
                }

                // unroll whereClause
                final List<SqlNode> list = new ArrayList<>(0);
                while (node.getKind() == whereSepKind) {
                    assert node instanceof SqlCall;
                    final SqlCall call1 = (SqlCall) node;
                    list.add(0, call1.operand(1));
                    node = call1.operand(0);
                }
                list.add(0, node);

                // unparse in a WhereList frame
                final SqlWriter.Frame whereFrame =
                    writer.startList(SqlWriter.FrameTypeEnum.WHERE_LIST);
                SqlSelectOperator.INSTANCE.unparseListClause(
                    writer,
                    new SqlNodeList(
                        list,
                        select.where.getParserPosition()),
                    whereSepKind);
                writer.endList(whereFrame);
            } else {
                select.where.unparse(writer, 0, 0);
            }
        }
        if (select.groupBy != null) {
            writer.sep("GROUP BY");
            final SqlWriter.Frame groupFrame =
                writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST);
            if (select.groupBy.getList().isEmpty()) {
                final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
                writer.endList(frame);
            } else {
                SqlSelectOperator.INSTANCE.unparseListClause(writer, select.groupBy);
            }
            writer.endList(groupFrame);
        }
        if (select.having != null) {
            writer.sep("HAVING");
            select.having.unparse(writer, 0, 0);
        }
        if (select.windowDecls.size() > 0) {
            writer.sep("WINDOW");
            final SqlWriter.Frame windowFrame =
                writer.startList(SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST);
            for (SqlNode windowDecl : select.windowDecls) {
                writer.sep(",");
                windowDecl.unparse(writer, 0, 0);
            }
            writer.endList(windowFrame);
        }
        if (select.orderBy != null && select.orderBy.size() > 0) {
            writer.sep("ORDER BY");
            final SqlWriter.Frame orderFrame =
                writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY_LIST);
            SqlSelectOperator.INSTANCE.unparseListClause(writer, select.orderBy);
            writer.endList(orderFrame);
        }
        if (select.isDynamicFetch()) {
            writer.fetchOffset(select.computedFetch, select.offset);
        } else {
            writer.fetchOffset(select.fetch, select.offset);
        }
        writer.endList(selectFrame);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validate(this.select);
        validator.validate(this.physicalPartition);
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        this.physicalPartition.accept(visitor);
        return this.select.accept(visitor);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        return this.select.equalsDeep(node, litmus) && this.physicalPartition.equalsDeep(node, litmus);
    }
}
