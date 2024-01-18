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
public class SqlShowProfiles extends SqlShow {
    private SqlSpecialOperator operator;
    public SqlShowProfiles(SqlParserPos pos) {
        super(pos, ImmutableList.of(), ImmutableList.of(), null, null, null, null);
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowProfilesOperator();
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_PROFILES;
    }

    public static class SqlShowProfilesOperator extends SqlSpecialOperator {

        public SqlShowProfilesOperator() {
            super("SHOW_PROFILES", SqlKind.SHOW_PROFILES);
        }

        protected RelDataType deriveTypeInner(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Query_ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("Duration", 1, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
            columns.add(new RelDataTypeFieldImpl("Query", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            return deriveTypeInner(validator, scope, call);
        }
    }
}
