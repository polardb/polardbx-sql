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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 */
public class SqlInspectDdlJobCache extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlInspectDdlJobCacheOperator();

    public SqlInspectDdlJobCache(SqlParserPos pos) {
        super(pos);
        this.operands = new ArrayList<>(0);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("INSPECT DDL CACHE");
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.INSPECT_DDL_JOB_CACHE;
    }

    public static class SqlInspectDdlJobCacheOperator extends SqlSpecialOperator {

        public SqlInspectDdlJobCacheOperator() {
            super("INSPECT_DDL_JOB_CACHE", SqlKind.INSPECT_DDL_JOB_CACHE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("JOB_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

}
