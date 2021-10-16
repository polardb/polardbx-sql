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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlCreateTableGroup extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlCreateTableGroupOperator();

    final boolean ifNotExists;
    final String locality;

    final String tableGroupName;
    final String schemaName;

    public SqlCreateTableGroup(SqlParserPos pos, boolean ifNotExists, String schemaName,
                               String tableGroupName, String locality) {
        super(OPERATOR, pos);
        this.ifNotExists = ifNotExists;
        this.schemaName = schemaName;
        this.tableGroupName = tableGroupName;
        this.locality = locality;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE TABLEGROUP");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        if (schemaName != null) {
            writer.keyword(schemaName + ".");
        }
        writer.keyword(tableGroupName);

        if (TStringUtil.isNotEmpty(locality)) {
            writer.keyword("LOCALITY = ");
            writer.print(TStringUtil.quoteString(locality));
        }
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getLocality() {
        return locality;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlCreateTableGroupOperator extends SqlSpecialOperator {

        public SqlCreateTableGroupOperator() {
            super("CREATE_TABLEGROUP", SqlKind.CREATE_TABLEGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_TABLEGROUP_RESULT",
                    0,
                    columnType)));
        }
    }
}

