package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class SqlAlterTableGroupSetLocality extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SET LOCALITY", SqlKind.SET_LOCALITY);

    private final SqlNode targetLocality;

    private SqlAlterTableGroup parent;

    private Boolean isLogical;

    public Boolean getLogical() {
        return isLogical;
    }

    public void setLogical(Boolean logical) {
        isLogical = logical;
    }


    public SqlAlterTableGroupSetLocality(SqlParserPos pos, SqlNode targetLocality) {
        super(pos);
        this.targetLocality = targetLocality;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }


    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public String getTargetLocality(){
        if (targetLocality == null) {
            return "";
        }
        String localityString = targetLocality.toString();
        localityString = StringUtils.strip(localityString, "'");
        return localityString;
    }

    @Override
    public String toString(){
        return getTargetLocality();
    }
}
