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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chenghui.lch
 * @date 2019/7/28 下午11:29
 */
public class SqlShowProfile extends SqlShow {

    public static final String MEMORY_TYPE = "MEMORY";
    public static final String CPU_TYPE = "CPU";

    private SqlSpecialOperator operator;

    protected List<String> types;
    protected SqlNode forQuery;

    public SqlShowProfile(SqlParserPos pos,
                          List<SqlSpecialIdentifier> specialIdentifiers,
                          List<String> types,
                          SqlNode forQuery,
                          SqlNode limit) {
        super(pos, specialIdentifiers, ImmutableList.<SqlNode>of(), null, null, null, limit);
        this.types = types;
        if (types == null || types.isEmpty()) {
            this.types = new ArrayList<>();
            this.types.add(CPU_TYPE);
        }
        this.forQuery = forQuery;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowProfileOperator(this.types, this.forQuery);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_PROFILE;
    }

    public List<String> getTypes() {
        return types;
    }

    public SqlNode getForQuery() {
        return forQuery;
    }

    public static class SqlShowProfileOperator extends SqlSpecialOperator {

        protected List<String> types;
        protected SqlNode forQuery;

        public SqlShowProfileOperator(List<String> typeList, SqlNode forQuery) {
            super("SHOW_PROFILE", SqlKind.SHOW_PROFILE);
            this.types = typeList;
            this.forQuery = forQuery;
        }

        protected RelDataType deriveTypeInner(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {

            if (types.size() == 1 && types.get(0).equals(MEMORY_TYPE)) {

                if (forQuery == null) {
                    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
                    List<RelDataTypeFieldImpl> columns = new LinkedList<>();
                    columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                    columns
                        .add(new RelDataTypeFieldImpl("TRACE_ID", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("DB", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("MEMORY", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("SQL", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    return typeFactory.createStructType(columns);
                } else {
                    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
                    List<RelDataTypeFieldImpl> columns = new LinkedList<>();
                    columns.add(new RelDataTypeFieldImpl("STAGE", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("MEMORY", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns
                        .add(new RelDataTypeFieldImpl("HIT_CACHED", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    return typeFactory.createStructType(columns);
                }

            } else if (types.size() == 1 && types.get(0).equals(CPU_TYPE)) {

                if (forQuery == null) {
                    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
                    List<RelDataTypeFieldImpl> columns = new LinkedList<>();
                    columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                    columns
                        .add(new RelDataTypeFieldImpl("TRACE_ID", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("DB", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns
                        .add(new RelDataTypeFieldImpl("TIME_COST", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns.add(new RelDataTypeFieldImpl("SQL", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    return typeFactory.createStructType(columns);
                } else {
                    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
                    List<RelDataTypeFieldImpl> columns = new LinkedList<>();
                    columns.add(new RelDataTypeFieldImpl("STAGE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns
                        .add(new RelDataTypeFieldImpl("TIME_COST", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    columns
                        .add(new RelDataTypeFieldImpl("ROW_COUNT", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                    return typeFactory.createStructType(columns);
                }

            } else {
                final RelDataTypeFactory typeFactory = validator.getTypeFactory();
                List<RelDataTypeFieldImpl> columns = new LinkedList<>();
                columns.add(new RelDataTypeFieldImpl("QUERY_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                return typeFactory.createStructType(columns);
            }
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            return deriveTypeInner(validator, scope, call);
        }
    }
}
