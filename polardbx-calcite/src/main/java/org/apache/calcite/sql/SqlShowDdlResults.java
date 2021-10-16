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
public class SqlShowDdlResults extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean all;
    private List<Long> jobIds = new ArrayList<>();

    public SqlShowDdlResults(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, boolean all) {
        super(pos, specialIdentifiers);
        this.all = all;
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public List<Long> getJobIds() {
        return jobIds;
    }

    public void setJobIds(List<Long> jobIds) {
        this.jobIds.clear();
        this.jobIds.addAll(jobIds);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowDdlResultsOperator(this.all);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_DDL_RESULTS;
    }

    public static class SqlShowDdlResultsOperator extends SqlSpecialOperator {

        private boolean full;

        public SqlShowDdlResultsOperator(boolean full) {
            super("SHOW_DDL_RESULTS", SqlKind.SHOW_DDL_RESULTS);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("JOB_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            if (full) {
                columns
                    .add(new RelDataTypeFieldImpl("PARENT_JOB_ID", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            }
            columns.add(new RelDataTypeFieldImpl("SERVER", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("OBJECT_SCHEMA", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("OBJECT_NAME", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(
                    new RelDataTypeFieldImpl("NEW_OBJECT_NAME", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            columns.add(new RelDataTypeFieldImpl("JOB_TYPE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PHASE", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATE", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PROGRESS", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(new RelDataTypeFieldImpl("DDL_STMT", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            columns.add(new RelDataTypeFieldImpl("GMT_CREATED", 11, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
            columns.add(new RelDataTypeFieldImpl("GMT_MODIFIED", 12, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
            columns.add(new RelDataTypeFieldImpl("REMARK", 13, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns
                .add(new RelDataTypeFieldImpl("BACKFILL_PROGRESS", 14, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
