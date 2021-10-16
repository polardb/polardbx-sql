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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_CYCLE_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_GROUP_TABLE_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INCREMENT_BY_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_MAX_VALUE_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_NAME_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_START_WITH_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_TABLE_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_TYPE_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_VALUE_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_INNER_STEP_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_UNIT_COUNT_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.EXT_UNIT_INDEX_COLUMN;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NO;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_YES;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.TIME_BASED;

/**
 * @author chenmo.cm
 */
public class SqlShowSequences extends SqlShow {

    private SqlShowSequencesOperator operator = null;

    private boolean isCustomUnitGroupSeqSupported;

    public SqlShowSequences(SqlParserPos pos,
                            List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                            SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
    }

    public void setCustomUnitGroupSeqSupported(boolean customUnitGroupSeqSupported) {
        isCustomUnitGroupSeqSupported = customUnitGroupSeqSupported;
    }

    public String getSql() {
        String select = ((SqlShowSequencesOperator) getOperator()).getSql();
        if (null == where && null == orderBy && null == limit) {
            return select;
        }

        StringBuilder sb = new StringBuilder("SELECT * FROM (").append(select).append(") t");

        if (null != where) {
            sb.append(" WHERE ").append(where.toString());
        }

        if (null != orderBy) {
            sb.append(" ORDER BY ").append(orderBy.toString());
        }

        if (null != limit) {
            sb.append(" LIMIT ").append(limit.toString());
        }

        return sb.toString();
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowSequencesOperator(isCustomUnitGroupSeqSupported);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_SEQUENCES;
    }

    public static class SqlShowSequencesOperator extends SqlSpecialOperator {

        final private Map<String, SqlTypeName> columnTypeMap = new LinkedHashMap<>();
        private boolean isCustomUnitGroupSeqSupported;
        private String sql;

        public SqlShowSequencesOperator(boolean isCustomUnitGroupSeqSupported) {
            super("SHOW_SEQUENCES", SqlKind.SHOW_SEQUENCES);
            this.isCustomUnitGroupSeqSupported = isCustomUnitGroupSeqSupported;
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
                    .add(new RelDataTypeFieldImpl(entry.getKey(), index, typeFactory.createSqlType(entry.getValue())));
                index++;
            }

            return typeFactory.createStructType(columns);
        }

        private String getDisplayNameWithNA(String origColumnName) {
            // Check if 'N/A' is needed
            StringBuilder sb = new StringBuilder();
            sb.append("IF(").append(origColumnName).append(" > 0, ").append(origColumnName);
            sb.append(", '").append(STR_NA).append("') AS ").append(origColumnName.toUpperCase());
            return sb.toString();
        }

        private String getMockColumnWithNA(String origColumnName) {
            return getMockColumnWithExtNA(origColumnName, null);
        }

        private String getMockColumnWithExtNA(String origColumnName, String ext) {
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(STR_NA).append(ext != null ? ext : "");
            sb.append("' AS ").append(origColumnName.toUpperCase());
            return sb.toString();
        }

        private String buildSelect(String column, String tableName) {
            StringBuilder sb = new StringBuilder("SELECT");
            if (StringUtils.isEmpty(column)) {
                sb.append(" * ");
            } else {
                sb.append(" ");
                sb.append(column);
            }
            sb.append(" FROM ").append(tableName);

            return sb.toString();
        }

        private String buildSelect(List<String> columns, String tableName) {
            return buildSelect(StringUtils.join(columns, ","), tableName);
        }

        private void init() {
            // Union table 'sequence_opt'
            List<String> columnsOpt = new LinkedList<>();

            // The NAME column
            columnsOpt.add(DEFAULT_NAME_COLUMN.toUpperCase());
            columnTypeMap.put(DEFAULT_NAME_COLUMN, SqlTypeName.VARCHAR);

            // The VALUE column
            columnsOpt.add(getDisplayNameWithNA(DEFAULT_VALUE_COLUMN));
            columnTypeMap.put(DEFAULT_VALUE_COLUMN, SqlTypeName.BIGINT);

            if (isCustomUnitGroupSeqSupported) {
                // The UNIT_COUNT column
                columnsOpt.add(getMockColumnWithExtNA(EXT_UNIT_COUNT_COLUMN, " "));
                columnTypeMap.put(EXT_UNIT_COUNT_COLUMN, SqlTypeName.INTEGER);

                // The UNIT_INDEX column
                columnsOpt.add(getMockColumnWithExtNA(EXT_UNIT_INDEX_COLUMN, "  "));
                columnTypeMap.put(EXT_UNIT_INDEX_COLUMN, SqlTypeName.INTEGER);

                // The STEP column
                columnsOpt.add(getMockColumnWithExtNA(EXT_INNER_STEP_COLUMN, "   "));
                columnTypeMap.put(EXT_INNER_STEP_COLUMN, SqlTypeName.INTEGER);
            }

            // The INCREMENT_BY column
            columnsOpt.add(getDisplayNameWithNA(DEFAULT_INCREMENT_BY_COLUMN));
            columnTypeMap.put(DEFAULT_INCREMENT_BY_COLUMN, SqlTypeName.BIGINT);

            // The START_WITH column
            columnsOpt.add(getDisplayNameWithNA(DEFAULT_START_WITH_COLUMN));
            columnTypeMap.put(DEFAULT_START_WITH_COLUMN, SqlTypeName.BIGINT);

            // The MAX_VALUE column
            columnsOpt.add(getDisplayNameWithNA(DEFAULT_MAX_VALUE_COLUMN));
            columnTypeMap.put(DEFAULT_MAX_VALUE_COLUMN, SqlTypeName.BIGINT);

            // The CYCLE column
            StringBuilder cycleExpr = new StringBuilder();
            cycleExpr.append("IF(").append(DEFAULT_CYCLE_COLUMN).append(" & ");
            cycleExpr.append(TIME_BASED).append(" = ").append(TIME_BASED);
            cycleExpr.append(", '").append(STR_NA).append("', IF(");
            cycleExpr.append(DEFAULT_CYCLE_COLUMN).append(" & ");
            cycleExpr.append(CYCLE).append(" = ").append(CYCLE);
            cycleExpr.append(", '").append(STR_YES).append("', '");
            cycleExpr.append(STR_NO).append("')) AS ");
            cycleExpr.append(DEFAULT_CYCLE_COLUMN.toUpperCase());
            columnsOpt.add(cycleExpr.toString());
            columnTypeMap.put(DEFAULT_CYCLE_COLUMN, SqlTypeName.VARCHAR);

            // Add an additional 'TYPE' column to show sequence type
            StringBuilder typeExpr = new StringBuilder();
            typeExpr.append("IF(").append(DEFAULT_CYCLE_COLUMN).append(" & ");
            typeExpr.append(TIME_BASED).append(" = ").append(TIME_BASED);
            typeExpr.append(", '").append(Type.TIME.getAbbreviation()).append("', '");
            typeExpr.append(Type.SIMPLE.getAbbreviation()).append("') AS ");
            typeExpr.append(DEFAULT_TYPE_COLUMN.toUpperCase());
            columnsOpt.add(typeExpr.toString());
            columnTypeMap.put(DEFAULT_TYPE_COLUMN, SqlTypeName.VARCHAR);

            final String sequenceOptTN = buildSelect(columnsOpt, DEFAULT_TABLE_NAME);

            // Union table 'sequence'
            StringBuilder columnsGroup = new StringBuilder();
            columnsGroup.append(DEFAULT_NAME_COLUMN.toUpperCase()).append(", ");
            columnsGroup.append(DEFAULT_VALUE_COLUMN).append(", ");
            if (isCustomUnitGroupSeqSupported) {
                columnsGroup.append(EXT_UNIT_COUNT_COLUMN).append(", ");
                columnsGroup.append(EXT_UNIT_INDEX_COLUMN).append(", ");
                columnsGroup.append(EXT_INNER_STEP_COLUMN).append(", ");
            }
            columnsGroup.append(getMockColumnWithNA(DEFAULT_INCREMENT_BY_COLUMN)).append(", ");
            columnsGroup.append(getMockColumnWithNA(DEFAULT_START_WITH_COLUMN)).append(", ");
            columnsGroup.append(getMockColumnWithNA(DEFAULT_MAX_VALUE_COLUMN)).append(", ");
            columnsGroup.append(getMockColumnWithNA(DEFAULT_CYCLE_COLUMN)).append(", '");
            columnsGroup.append(Type.GROUP.getAbbreviation()).append("' AS ").append(DEFAULT_TYPE_COLUMN);

            final String sequenceTN = buildSelect(columnsGroup.toString(), DEFAULT_GROUP_TABLE_NAME);

            this.sql = sequenceOptTN + " UNION ( " + sequenceTN + " )";
        }
    }
}
