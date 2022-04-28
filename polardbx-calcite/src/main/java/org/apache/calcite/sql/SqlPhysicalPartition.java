package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlPhysicalPartition extends SqlNode {
    SqlIdentifier name;

    public SqlPhysicalPartition(SqlIdentifier name, SqlParserPos sqlParserPos) {
        super(sqlParserPos);
        this.name = name;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlPhysicalPartition(this.name, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, " PARTITION(", ")");
        writer.sep(this.name.toString());
        writer.endList(frame);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validate(this.name);
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this.name);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (this == node) {
            return true;
        }

        if (node == null) {
            return false;
        }

        if (node.getClass() != this.getClass()) {
            return false;
        }

        SqlPhysicalPartition objPartBy = (SqlPhysicalPartition) node;

        if (!equalDeep(this.name, objPartBy.name, litmus)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(" PARTITION(");
        sb.append(name);
        sb.append(")");

        return sb.toString();
    }
}
