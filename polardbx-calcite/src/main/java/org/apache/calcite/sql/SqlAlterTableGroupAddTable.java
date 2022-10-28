package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupAddTable extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER_TABLEGROUP_ADD_TABLE", SqlKind.ALTER_TABLEGROUP_ADD_TABLE);

    protected final List<SqlNode> tables;
    protected final boolean force;

    protected SqlNode parent;

    public SqlAlterTableGroupAddTable(SqlParserPos pos, List<SqlNode> tables, boolean force) {
        super(pos);
        this.tables = tables;
        this.force = force;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return this.tables;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD", "");

        writer.keyword("TABLES");
        final SqlWriter.Frame partFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "(", ")");
        int i = 0;
        for (SqlNode sqlNode : tables) {
            sqlNode.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < tables.size()) {
                writer.sep(",");
            }
        }
        if (force) {
            writer.keyword("FORCE");
        }
        writer.endList(partFrame);

        writer.endList(frame);
    }

    public List<SqlNode> getTables() {
        return tables;
    }

    public boolean isForce() {
        return force;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }
}
