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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/13 下午1:08
 */
public class SqlShowStats extends SqlShow {

    private boolean              full;
    private SqlShowStatsOperator operator;

    public SqlShowStats(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                        SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowStatsOperator(full);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_STATS;
    }

    public static class SqlShowStatsOperator extends SqlSpecialOperator {

        private boolean full;

        public SqlShowStatsOperator(boolean full){
            super("SHOW_STATS", SqlKind.SHOW_STATS);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            if (full) {
                columns.add(new RelDataTypeFieldImpl("QPS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("RDS_QPS", 1, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("SLOW_QPS", 2, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("PHYSICAL_SLOW_QPS",
                    3,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("ERROR_PER_SECOND",
                    4,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("VIOLATION_PER_SECOND",
                    5,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("MERGE_QUERY_PER_SECOND",
                    6,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("ACTIVE_CONNECTIONS",
                    7,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("CONNECTION_CREATE_PER_SECOND",
                    8,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("RT(ms)", 9, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("RDS_RT(ms)", 10, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("NET_IN(KB/S)", 11, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("NET_OUT(KB/S)", 12, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("THREAD_RUNNING",
                    13,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("HINT_USED_PER_SECOND",
                    14,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("HINT_USED_COUNT",
                    15,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("AGGREGATE_QUERY_PER_SECOND",
                    16,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("AGGREGATE_QUERY_COUNT",
                    17,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("TEMP_TABLE_CREATE_PER_SECOND",
                    18,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("TEMP_TABLE_CREATE_COUNT",
                    19,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("MULTI_DB_JOIN_PER_SECOND",
                    20,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("MULTI_DB_JOIN_COUNT",
                    21,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("CPU", 22, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("FREEMEM", 23, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("FULLGCCOUNT", 24, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("FULLGCTIME", 25, typeFactory.createSqlType(SqlTypeName.BIGINT)));

            } else {
                columns.add(new RelDataTypeFieldImpl("QPS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("RDS_QPS", 1, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("SLOW_QPS", 2, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("PHYSICAL_SLOW_QPS",
                    3,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("ERROR_PER_SECOND",
                    4,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("MERGE_QUERY_PER_SECOND",
                    5,
                    typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("ACTIVE_CONNECTIONS",
                    6,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("RT(ms)", 7, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("RDS_RT(ms)", 8, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("NET_IN(KB/S)", 9, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("NET_OUT(KB/S)", 10, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
                columns.add(new RelDataTypeFieldImpl("THREAD_RUNNING",
                    11,
                    typeFactory.createSqlType(SqlTypeName.BIGINT)));
            }

            return typeFactory.createStructType(columns);
        }
    }
}
