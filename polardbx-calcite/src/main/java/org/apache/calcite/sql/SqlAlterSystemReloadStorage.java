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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SqlAlterSystemReloadStorage extends SqlDal {

    public static class SqlAlterSystemReloadStorageOperator extends SqlSpecialOperator {

        public SqlAlterSystemReloadStorageOperator() {
            super("ALTER_SYSTEM_RELOAD_STORAGE", SqlKind.ALTER_SYSTEM_RELOAD_STORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {

            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("DN", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("RW_DN", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("KIND", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            columns.add(new RelDataTypeFieldImpl("NODE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("USER", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PASSWD_ENC", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ROLE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            columns.add(new RelDataTypeFieldImpl("IS_VIP", 7, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
            columns.add(new RelDataTypeFieldImpl("FROM", 8, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));




            return typeFactory.createStructType(columns);

//            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);
//            return typeFactory.createStructType(
//                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_SYSTEM_RELOAD_STORAGE_RESULT",
//                    0,
//                    columnType)));
        }
    }

    private static final SqlSpecialOperator OPERATOR = new SqlAlterSystemReloadStorage.SqlAlterSystemReloadStorageOperator();


    protected List<SqlNode> storageList = new ArrayList<>();
    public SqlAlterSystemReloadStorage(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.print("ALTER SYSTEM RELOAD STORAGE");

        if (storageList.size() > 0) {
            writer.print(" ");
            for (int i = 0; i < storageList.size(); i++) {
                if (i > 0) {
                    writer.print(",");
                }
                storageList.get(i).unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getStorageList() {
        return storageList;
    }

    public void setStorageList(List<SqlNode> storageList) {
        this.storageList = storageList;
    }
}
