package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * @author chenghui.lch
 */
public class SqlTimeToLiveJobExpr extends SqlNode {

    protected SqlNode cron;
    protected SqlNode timezone;

    public SqlTimeToLiveJobExpr() {
        super(SqlParserPos.ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("CRON ");
        writer.print(cron.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        if (timezone != null) {
            writer.print(" ");
            writer.print("TIMEZONE ");
            writer.print(timezone.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        return false;
    }

    public SqlNode getCron() {
        return cron;
    }

    public void setCron(SqlNode cron) {
        this.cron = cron;
    }

    public SqlNode getTimezone() {
        return timezone;
    }

    public void setTimezone(SqlNode timezone) {
        this.timezone = timezone;
    }
}
