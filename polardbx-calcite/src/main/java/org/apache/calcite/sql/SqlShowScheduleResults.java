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
public class SqlShowScheduleResults extends SqlShow {

    private SqlSpecialOperator operator;

    private long scheduleId;

    public SqlShowScheduleResults(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, long scheduleId) {
        super(pos, specialIdentifiers);
        this.scheduleId = scheduleId;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowScheduleResultOperator();
        }
        return operator;
    }

    public long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final long scheduleId) {
        this.scheduleId = scheduleId;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_SCHEDULE_RESULTS;
    }

    public static class SqlShowScheduleResultOperator extends SqlSpecialOperator {

        public SqlShowScheduleResultOperator() {
            super("SHOW_SCHEDULE_RESULTS", SqlKind.SHOW_SCHEDULE_RESULTS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            int index = 0;
            columns.add(new RelDataTypeFieldImpl("SCHEDULE_ID", index++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("TIME_ZONE", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("FIRE_TIME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("START_TIME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("FINISH_TIME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATE", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("REMARK", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("RESULT_MSG", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
