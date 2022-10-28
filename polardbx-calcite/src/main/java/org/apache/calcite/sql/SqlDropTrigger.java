package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropTriggerStatement;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlDropTrigger extends SqlDrop {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP TRIGGER", SqlKind.DROP_TRIGGER);

    private SQLDropTriggerStatement sqlDropTriggerStatement;

    private String tableName;

    public SqlDropTrigger(SQLDropTriggerStatement sqlDropTriggerStatement) {
        super(OPERATOR, SqlParserPos.ZERO, false);
        this.sqlDropTriggerStatement = sqlDropTriggerStatement;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
    }

    public String getText() {
        return sqlDropTriggerStatement.toString();
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        writer.print(sqlDropTriggerStatement.toString());
    }

    @Override
    public String toString() {
        return sqlDropTriggerStatement.toString();
    }


    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlDropTrigger(sqlDropTriggerStatement);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>();
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (node == this) {
            return true;
        }
        if (node instanceof SqlDropTrigger) {
            sqlDropTriggerStatement.toString().equals(((SqlDropTrigger) node).getSqlDropTriggerStatement().toString());
            return super.equalsDeep(node, litmus);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public SQLDropTriggerStatement getSqlDropTriggerStatement() {
        return sqlDropTriggerStatement;
    }
}
