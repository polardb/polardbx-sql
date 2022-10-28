package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */

public class SqlCreateFunction extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

    private String tableName;

    private String functionName;

    private String text;

    private boolean canPush;

    public SqlCreateFunction(String text, String functionName, boolean canPush) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.tableName = "_NONE_";
        this.text = text;
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.functionName = functionName;
        this.canPush = canPush;
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
        return new SqlCreateFunction(text, functionName, canPush);
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
        if (node instanceof SqlCreateFunction) {
            if (!text.equals(((SqlCreateFunction) node).getText())) {
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

    public boolean isCanPush() {
        return canPush;
    }
}

