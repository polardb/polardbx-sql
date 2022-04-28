package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsUnArchiveStatement;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * @author Shi Yuxuan
 */
public class SqlUnArchive extends SqlDdl {

    String schemaName;

    private static final SqlSpecialOperator OPERATOR = new SqlUnArchiveOperator();

    private DrdsUnArchiveStatement.UnArchiveTarget target;
    private SqlNode node;

    /**
     * Creates a SqlDdl.
     */
    public SqlUnArchive(SqlParserPos pos) {
        super(OPERATOR, pos);
    }

    public void setTable(SqlNode table) {
        this.target = DrdsUnArchiveStatement.UnArchiveTarget.TABLE;
        this.node = table;
        SqlIdentifier tableName = (SqlIdentifier)table;
        if (!tableName.isSimple()) {
            this.schemaName = tableName.names.get(0);
        }
    }

    public void setTableGroup(SqlNode tableGroup) {
        this.target = DrdsUnArchiveStatement.UnArchiveTarget.TABLE_GROUP;
        this.node = tableGroup;
        this.schemaName = null;
    }

    public void setDatabase(SqlNode database) {
        this.target = DrdsUnArchiveStatement.UnArchiveTarget.DATABASE;
        this.node = database;
        this.schemaName = Util.last(((SqlIdentifier)database).names);
    }

    public DrdsUnArchiveStatement.UnArchiveTarget getTarget() {
        return target;
    }

    public SqlNode getNode() {
        return node;
    }

    public String getSchemaName() {
        return schemaName;
    }
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("UNARCHIVE ");

        switch (target) {
        case TABLE:
            writer.keyword("TABLE ");
            break;
        case TABLE_GROUP:
            writer.keyword("TABLEGROUP ");
            break;
        case DATABASE:
            writer.keyword("DATABASE ");
            break;
        }
        node.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.UNARCHIVE;
    }

    public static class SqlUnArchiveOperator extends SqlSpecialOperator {

        public SqlUnArchiveOperator() {
            super("UnArchive", SqlKind.UNARCHIVE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("UnArchive",
                    0,
                    columnType)));
        }
    }
}
