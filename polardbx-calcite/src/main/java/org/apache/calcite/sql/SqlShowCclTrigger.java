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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author busu
 * date: 2021/4/18 4:40 下午
 */
public class SqlShowCclTrigger extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlShowCclRulesOperator();

    private boolean showAll;

    private List<SqlIdentifier> names;

    public SqlShowCclTrigger(SqlParserPos pos,
                             List<SqlSpecialIdentifier> specialIdentifiers, boolean showAll,
                             List<SqlIdentifier> names) {
        super(pos, specialIdentifiers);
        this.showAll = showAll;
        this.names = names;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CCL_TRIGGER;
    }

    public static class SqlShowCclRulesOperator extends SqlSpecialOperator {

        public SqlShowCclRulesOperator() {
            super("SHOW_CCL_TRIGGER", SqlKind.SHOW_CCL_TRIGGER);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Trigger_Name", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Ccl_Rule_Count", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Database", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Conditions", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Rule_Config", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Rule_Upgrade", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("Max_Ccl_Rule", 6, typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            columns.add(new RelDataTypeFieldImpl("Max_Sql_Size", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Created_Time", 8, typeFactory.createSqlType(SqlTypeName.DATETIME)));
            return typeFactory.createStructType(columns);
        }
    }

    public boolean isShowAll() {
        return showAll;
    }

    public void setShowAll(boolean showAll) {
        this.showAll = showAll;
    }

    public List<SqlIdentifier> getNames() {
        return names;
    }

    public void setNames(List<SqlIdentifier> names) {
        this.names = names;
    }
}
