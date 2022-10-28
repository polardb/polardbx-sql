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

import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/9 下午5:27
 */
public class SqlShowSlow extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowSlowOperator();
    private boolean isFull;
    private boolean isPhysical;

    public SqlShowSlow(SqlParserPos pos,
                       List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                       SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean isFull, boolean isPhysical) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.isFull = isFull;
        this.isPhysical = isPhysical;
    }

    public static SqlShowSlow create(SqlParserPos pos, boolean isFull, boolean isPhysical, SqlNode where,
                                     SqlNode orderBy, SqlNode limit) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (isFull) {
            specialIdentifiers.add(SqlSpecialIdentifier.FULL);
            if (null == orderBy) {
                // Generate order by if not exists
                List<SqlNode> orderColumns = new LinkedList<>();
                orderColumns.add(new SqlIdentifier("START_TIME", SqlParserPos.ZERO));
                orderBy = new SqlNodeList(orderColumns, SqlParserPos.ZERO);
            }
        }

        if (isPhysical) {
            specialIdentifiers.add(SqlSpecialIdentifier.PHYSICAL_SLOW);
        } else {
            specialIdentifiers.add(SqlSpecialIdentifier.SLOW);
        }
        return new SqlShowSlow(pos,
            specialIdentifiers,
            ImmutableList.<SqlNode>of(),
            null,
            where,
                orderBy,
            limit,
            isFull,
            isPhysical);
    }

    public boolean isFull() {
        return isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public boolean isPhysical() {
        return isPhysical;
    }

    public void setPhysical(boolean physical) {
        isPhysical = physical;
    }

    @Override
    protected boolean showSort() {
        return false;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_SLOW;
    }

    @Override
    public boolean canConvertToSelect() {
        return true;
    }

    @Override
    public SqlSelect convertToSelect() {
        return doConvertToSelect();
    }

    public static class SqlShowSlowOperator extends SqlSpecialOperator {

        public SqlShowSlowOperator(){
            super("SHOW_SLOW", SqlKind.SHOW_SLOW);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("TRACE_ID"      , 0 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("USER"          , 1 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("HOST"          , 2 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("DB"            , 3 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("START_TIME"    , 4 , typeFactory.createSqlType(SqlTypeName.DATETIME)));
            columns.add(new RelDataTypeFieldImpl("EXECUTE_TIME"  , 5 , typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("AFFECT_ROW"    , 6 , typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("SQL"           , 7 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));


            return typeFactory.createStructType(columns);
        }
    }
}
