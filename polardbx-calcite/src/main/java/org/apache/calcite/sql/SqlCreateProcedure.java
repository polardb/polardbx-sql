package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */

public class SqlCreateProcedure extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE PROCEDURE", SqlKind.CREATE_PROCEDURE);

    private String tableName;

    private String text;

    private SQLName procedureName;

    public SqlCreateProcedure(String text, SQLName procedureName) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.tableName = "_NONE_";
        this.text = text;
        this.name = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        this.procedureName = procedureName;
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
        return new SqlCreateProcedure(text, procedureName);
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
        if (node instanceof SqlCreateProcedure) {
            if (!text.equals(((SqlCreateProcedure) node).getText())) {
                return false;
            }
            return super.equalsDeep(node, litmus);
        }
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public SQLName getProcedureName() {
        return procedureName;
    }
}


