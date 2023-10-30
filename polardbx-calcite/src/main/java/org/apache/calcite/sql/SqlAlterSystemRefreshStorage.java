/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SqlAlterSystemRefreshStorage extends SqlDal{

    public static class SqlAlterSystemRefreshStorageOperator extends SqlSpecialOperator {

        public SqlAlterSystemRefreshStorageOperator() {
            super("ALTER_SYSTEM_REFRESH_STORAGE", SqlKind.ALTER_SYSTEM_REFRESH_STORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);
            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_SYSTEM_REFRESH_STORAGE_RESULT",
                    0,
                    columnType)));
        }
    }

    private static final SqlSpecialOperator OPERATOR = new SqlAlterSystemRefreshStorageOperator();

    // target dn id
    protected SqlNode targetStorage;
    protected List<Pair<SqlNode, SqlNode>> assignItems = new ArrayList<>();

    public SqlAlterSystemRefreshStorage(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.print("ALTER SYSTEM REFRESH STORAGE ");

        targetStorage.unparse(writer, leftPrec, rightPrec);
        writer.print(" SET ");
        for (int i = 0; i < assignItems.size(); i++) {
            if (i > 0) {
                writer.print(",");
            }
            assignItems.get(i).getKey().unparse(writer, leftPrec, rightPrec);
            writer.print(" = ");
            assignItems.get(i).getValue().unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(selectFrame);
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlNode getTargetStorage() {
        return targetStorage;
    }

    public void setTargetStorage(SqlNode targetStorage) {
        this.targetStorage = targetStorage;
    }

    public List<Pair<SqlNode, SqlNode>> getAssignItems() {
        return assignItems;
    }

    public void setAssignItems(List<Pair<SqlNode, SqlNode>> assignItems) {
        this.assignItems = assignItems;
    }

}
