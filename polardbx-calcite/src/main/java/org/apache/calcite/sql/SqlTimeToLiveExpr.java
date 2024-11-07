package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.google.common.base.Joiner;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenghui.lch
 */
public class SqlTimeToLiveExpr  extends SqlNode {

    protected SqlNode column;
    protected SqlNode expireAfter;
    protected SqlNode unit;
    protected SqlNode timezone;

    public SqlTimeToLiveExpr() {
        super(SqlParserPos.ZERO);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

//        final SqlWriter.Frame startFrame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);

        writer.print(column.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        writer.print(" ");
        writer.print("EXPIRE AFTER ");
        writer.print(expireAfter.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        writer.print(" ");
        writer.print(SQLUtils.normalize(unit.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql()));
        if (timezone != null) {
            writer.print(" ");
            writer.print("TIMEZONE ");
            writer.print(timezone.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql());
        }
//        writer.endList(startFrame);
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

    public SqlNode getColumn() {
        return column;
    }

    public void setColumn(SqlNode column) {
        this.column = column;
    }

    public SqlNode getExpireAfter() {
        return expireAfter;
    }

    public void setExpireAfter(SqlNode expireAfter) {
        this.expireAfter = expireAfter;
    }

    public SqlNode getUnit() {
        return unit;
    }

    public void setUnit(SqlNode unit) {
        this.unit = unit;
    }

    public SqlNode getTimezone() {
        return timezone;
    }

    public void setTimezone(SqlNode timezone) {
        this.timezone = timezone;
    }
}
