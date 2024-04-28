package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class SqlCreateSecurityLabel extends SqlDal{

    private static final SqlOperator OPERATOR = new SqlCreateSecurityLabelOperator();

    private SqlIdentifier labelName;
    private SqlIdentifier policyName;
    private SqlCharStringLiteral labelContent;

    protected SqlCreateSecurityLabel(SqlParserPos pos) {
        super(pos);
    }

    public SqlCreateSecurityLabel(SqlParserPos pos, SqlIdentifier labelName, SqlIdentifier policyName,
                                  SqlCharStringLiteral labelContent) {
        super(pos);
        this.labelName = labelName;
        this.policyName = policyName;
        this.labelContent = labelContent;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SECURITY_LABEL;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SECURITY LABEL");
        writer.keyword(policyName.toString() + "." + labelName.toString());
        labelContent.unparse(writer, leftPrec, rightPrec);
    }

    public SqlIdentifier getLabelName() {
        return labelName;
    }

    public void setLabelName(SqlIdentifier labelName) {
        this.labelName = labelName;
    }

    public SqlIdentifier getPolicyName() {
        return policyName;
    }

    public void setPolicyName(SqlIdentifier policyName) {
        this.policyName = policyName;
    }

    public SqlCharStringLiteral getLabelContent() {
        return labelContent;
    }

    public void setLabelContent(SqlCharStringLiteral labelContent) {
        this.labelContent = labelContent;
    }

    public static class SqlCreateSecurityLabelOperator extends SqlSpecialOperator {

        public SqlCreateSecurityLabelOperator() {
            super("CREATE_SECURITY_LABEL", SqlKind.CREATE_SECURITY_LABEL);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {
            RelDataTypeFactory typeFactory = validator.getTypeFactory();
            RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SECURITY_LABEL",
                    0,
                    columnType)));
        }
    }

}
