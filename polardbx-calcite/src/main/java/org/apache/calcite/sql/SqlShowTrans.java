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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlShowTrans extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean columnar;

    public SqlShowTrans(SqlParserPos pos, boolean columnar) {
        super(pos, ImmutableList.of());
        this.columnar = columnar;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowTransOperator(columnar);
        }

        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_TRANS;
    }

    public boolean isColumnar() {
        return columnar;
    }

    public static class SqlShowTransOperator extends SqlSpecialOperator {

        private boolean columnar;

        public SqlShowTransOperator(boolean columnar) {
            super("SHOW_TRANS", SqlKind.SHOW_TRANS);
            this.columnar = columnar;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("TRANS_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TYPE", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("DURATION_MS", 2, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("STATE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PROCESS_ID", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            if (columnar) {
                columns.add(new RelDataTypeFieldImpl("TSO", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            }
            return typeFactory.createStructType(columns);
        }
    }
}
