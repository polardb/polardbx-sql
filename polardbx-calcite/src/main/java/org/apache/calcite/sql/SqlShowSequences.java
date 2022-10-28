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

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NO;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_YES;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;

/**
 * @author chenmo.cm
 */
public class SqlShowSequences extends SqlShow {

    private SqlShowSequencesOperator operator = null;

    public SqlShowSequences(SqlParserPos pos,
                            List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                            SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
    }

    public String getSql() {
        String select = ((SqlShowSequencesOperator) getOperator()).getSql();
        if (null == where && null == orderBy && null == limit) {
            return select;
        }

        StringBuilder sb = new StringBuilder("SELECT * FROM (").append(select).append(") t");

        if (null != where) {
            sb.append(" WHERE ").append(where);
        }

        if (null != orderBy) {
            sb.append(" ORDER BY ").append(orderBy);
        }

        if (null != limit) {
            sb.append(" LIMIT ").append(limit);
        }

        return sb.toString();
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowSequencesOperator();
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_SEQUENCES;
    }

    public static class SqlShowSequencesOperator extends SqlSpecialOperator {

        final private Map<String, SqlTypeName> columnTypeMap = new LinkedHashMap<>();
        private String sql;

        public SqlShowSequencesOperator() {
            super("SHOW_SEQUENCES", SqlKind.SHOW_SEQUENCES);
            init();
        }

        public String getSql() {
            return sql;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            int index = 0;
            for (Entry<String, SqlTypeName> entry : columnTypeMap.entrySet()) {
                columns
                    .add(
                    new RelDataTypeFieldImpl(entry.getKey(), index, typeFactory.createSqlType(entry.getValue())));
                index++;
            }

            return typeFactory.createStructType(columns);
        }

        private String getDisplayNameWithNA(String columnName) {
            StringBuilder sb = new StringBuilder();
            sb.append("IF(").append(columnName).append(" > 0, ").append(columnName);
            sb.append(", '").append(STR_NA).append("') AS ").append(columnName);
            return sb.toString();
        }

        private String buildSelect(String column, String tableName) {
            StringBuilder sb = new StringBuilder("SELECT ");
            sb.append(TStringUtil.isEmpty(column) ? "*" : column);
            sb.append(" FROM ").append(tableName);
            return sb.toString();
        }

        private void init() {
            // Union table 'sequence_opt'
            List<String> columnsOpt = new LinkedList<>();

            String shownColumn = "NAME";
            columnsOpt.add(shownColumn);
            columnTypeMap.put(shownColumn, SqlTypeName.VARCHAR);

            shownColumn = "VALUE";
            columnsOpt.add(getDisplayNameWithNA(shownColumn));
            columnTypeMap.put(shownColumn, SqlTypeName.BIGINT);

            shownColumn = "UNIT_COUNT";
            columnsOpt.add("'N/A ' AS " + shownColumn);
            columnTypeMap.put(shownColumn, SqlTypeName.INTEGER);

            shownColumn = "UNIT_INDEX";
            columnsOpt.add("'N/A  ' AS " + shownColumn);
            columnTypeMap.put(shownColumn, SqlTypeName.INTEGER);

            shownColumn = "STEP";
            columnsOpt.add("'N/A   ' AS " + shownColumn);
            columnTypeMap.put(shownColumn, SqlTypeName.INTEGER);

            shownColumn = "INCREMENT_BY";
            columnsOpt.add(getDisplayNameWithNA(shownColumn));
            columnTypeMap.put(shownColumn, SqlTypeName.BIGINT);

            shownColumn = "START_WITH";
            columnsOpt.add(getDisplayNameWithNA(shownColumn));
            columnTypeMap.put(shownColumn, SqlTypeName.BIGINT);

            shownColumn = "MAX_VALUE";
            columnsOpt.add(getDisplayNameWithNA(shownColumn));
            columnTypeMap.put(shownColumn, SqlTypeName.BIGINT);

            shownColumn = "CYCLE";
            StringBuilder cycleExpr = new StringBuilder();
            cycleExpr.append("IF(").append(shownColumn).append(" & ");
            cycleExpr.append(TIME_BASED).append(" = ").append(TIME_BASED);
            cycleExpr.append(", '").append(STR_NA).append("', IF(");
            cycleExpr.append(shownColumn).append(" & ");
            cycleExpr.append(CYCLE).append(" = ").append(CYCLE);
            cycleExpr.append(", '").append(STR_YES).append("', '");
            cycleExpr.append(STR_NO).append("')) AS ");
            cycleExpr.append(shownColumn);
            columnsOpt.add(cycleExpr.toString());
            columnTypeMap.put(shownColumn, SqlTypeName.VARCHAR);

            // An additional 'TYPE' column to show sequence type
            StringBuilder typeExpr = new StringBuilder();
            typeExpr.append("IF(").append(shownColumn).append(" & ");
            typeExpr.append(TIME_BASED).append(" = ").append(TIME_BASED);
            typeExpr.append(", '").append(Type.TIME).append("', '");
            typeExpr.append(Type.SIMPLE).append("') AS ");

            shownColumn = "TYPE";
            typeExpr.append(shownColumn);
            columnsOpt.add(typeExpr.toString());
            columnTypeMap.put(shownColumn, SqlTypeName.VARCHAR);

            String columnList = TStringUtil.join(columnsOpt, ",");
            final String sequenceOptTN = buildSelect(columnList, GmsSystemTables.SEQUENCE_OPT);

            // Union table 'sequence'
            columnList = "NAME, VALUE, UNIT_COUNT, UNIT_INDEX, INNER_STEP, "
                + "'N/A' AS INCREMENT_BY, 'N/A' AS START_WITH, 'N/A' AS MAX_VALUE, 'N/A' AS CYCLE, "
                + "'GROUP' AS TYPE";
            final String sequenceTN = buildSelect(columnList, GmsSystemTables.SEQUENCE);

            this.sql = sequenceOptTN + " UNION ( " + sequenceTN + " )";
        }
    }
}
