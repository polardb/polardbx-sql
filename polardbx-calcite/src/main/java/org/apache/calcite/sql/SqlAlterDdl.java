package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author dylan
 */
public abstract class SqlAlterDdl extends SqlDdl {

    /** Creates a SqlCreate. */
    public SqlAlterDdl(SqlOperator operator, SqlParserPos pos) {
        super(operator, pos);
    }

    public SqlNode getName() {
        return name;
    }

    /**
     * @return the identifier for the target table of the update
     */
    @Override
    public SqlNode getTargetTable() {
        return name;
    }


    /**
     * @return the identifier for the target table of the update
     */
    @Override
    public void setTargetTable(SqlNode sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateDdl(this , validator.getUnknownType(), scope);
    }
}