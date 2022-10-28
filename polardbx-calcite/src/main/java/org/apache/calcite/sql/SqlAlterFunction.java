package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class SqlAlterFunction extends SqlAlterDdl{
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER FUNCTION", SqlKind.ALTER_FUNCTION);

    private String text;

    private String tableName;

    private String functionName;

    public SqlAlterFunction(String text, String functionName) {
        super(OPERATOR, SqlParserPos.ZERO);
        this.text = text;
        this.tableName = "_NONE_";
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.functionName = functionName;
    }

    public String getText() {
        return text;
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        writer.print(text);
    }

    @Override
    public String toString() {
        return text;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlAlterFunction(text, functionName);
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
        if (node instanceof SqlAlterFunction) {
            if (!text.equals(((SqlAlterFunction) node).getText())) {
                return false;
            }
            return super.equalsDeep(node, litmus);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        return;
    }
}
