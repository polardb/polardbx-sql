package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTriggerStatement;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlCreateTrigger extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TRIGGER", SqlKind.CREATE_TRIGGER);

    private SQLCreateTriggerStatement sqlCreateTriggerStatement;

    private String tableName;

    public SqlCreateTrigger(SQLCreateTriggerStatement sqlCreateTriggerStatement) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.sqlCreateTriggerStatement = sqlCreateTriggerStatement;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        writer.print(sqlCreateTriggerStatement.toString());
    }

    @Override
    public String toString() {
        return sqlCreateTriggerStatement.toString();
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCreateTrigger(sqlCreateTriggerStatement);
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
        if (node instanceof SqlCreateTrigger) {
            tableName.equals(((SqlCreateTrigger) node).getTableName());
            sqlCreateTriggerStatement.toString().equals(((SqlCreateTrigger) node).getSqlCreateTriggerStatement().toString());
            return super.equalsDeep(node, litmus);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public SQLCreateTriggerStatement getSqlCreateTriggerStatement() {
        return sqlCreateTriggerStatement;
    }
}

