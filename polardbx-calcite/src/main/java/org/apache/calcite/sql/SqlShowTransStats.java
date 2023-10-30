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

import com.alibaba.polardbx.stats.TransStatsColumn;
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

import static com.alibaba.polardbx.stats.TransStatsColumn.SORTED_COLUMNS;

/**
 * @author yaozhili
 */
public class SqlShowTransStats extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlShowTransStatsOperator();

    public SqlShowTransStats(SqlParserPos pos) {
        super(pos, ImmutableList.of());
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
        return SqlKind.SHOW_TRANS_STATS;
    }

    public static class SqlShowTransStatsOperator extends SqlSpecialOperator {

        public SqlShowTransStatsOperator() {
            super("SHOW_TRANS_STATS", SqlKind.SHOW_TRANS_STATS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            for (TransStatsColumn.ColumnDef column : SORTED_COLUMNS) {
                RelDataType type;
                switch (column.columnType) {
                case LONG:
                    type = typeFactory.createSqlType(SqlTypeName.BIGINT);
                    break;
                case DOUBLE:
                    type = typeFactory.createSqlType(SqlTypeName.DOUBLE);
                    break;
                default:
                    type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                }
                columns.add(new RelDataTypeFieldImpl(column.name, column.index, type));
            }
            return typeFactory.createStructType(columns);
        }
    }
}

