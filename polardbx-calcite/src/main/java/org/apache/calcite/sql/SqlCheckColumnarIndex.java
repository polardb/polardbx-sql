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

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SqlCheckColumnarIndex extends SqlDdl { // Use DDL here to utilize async DDL framework.

    private static final SqlSpecialOperator OPERATOR = new SqlCheckColumnarIndex.SqlCheckColumnarIndexOperator();

    private SqlIdentifier indexName;
    private SqlIdentifier tableName;
    private String extraCmd;

    private List<Long> extras;

    public SqlCheckColumnarIndex(SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier tableName, String extraCmd,
                                 List<Long> extras) {
        super(OPERATOR, pos);
        this.name = indexName;
        this.indexName = indexName;
        this.tableName = tableName;
        this.extraCmd = extraCmd;
        this.extras = extras;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public void setIndexName(SqlIdentifier indexName) {
        this.indexName = indexName;
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public void setTableName(SqlIdentifier tableName) {
        this.tableName = tableName;
    }

    public String getExtraCmd() {
        return extraCmd;
    }

    public void setExtraCmd(String extraCmd) {
        this.extraCmd = extraCmd;
    }

    public CheckCciExtraCmd getExtraCmdEnum() {
        return CheckCciExtraCmd.of(this.extraCmd);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CHECK COLUMNAR INDEX");

        if (indexName != null) {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        if (tableName != null) {
            writer.sep("ON");
            tableName.unparse(writer, leftPrec, rightPrec);
        }

        if (extraCmd != null) {
            writer.sep(extraCmd);
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_COLUMNAR_INDEX;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlCheckColumnarIndexOperator extends SqlSpecialOperator {

        public SqlCheckColumnarIndexOperator() {
            super("CHECK_COLUMNAR_INDEX", SqlKind.CHECK_COLUMNAR_INDEX);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Op", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_type", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckColumnarIndex(this.pos, indexName, tableName, extraCmd, extras);
    }

    public SqlCheckColumnarIndex replaceIndexName(SqlIdentifier newIndexName) {
        return new SqlCheckColumnarIndex(pos, newIndexName, tableName, extraCmd, extras);
    }

    public boolean withTableName() {
        return null != tableName;
    }

    public List<Long> getExtras() {
        return extras;
    }

    public enum CheckCciExtraCmd {
        UNKNOWN, DEFAULT, CHECK, LOCK, CLEAR, SHOW, META, INCREMENT, SNAPSHOT;
        private static final Map<String, CheckCciExtraCmd> VALUE_MAP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        static {
            VALUE_MAP.put("CHECK", CHECK);
            VALUE_MAP.put("LOCK", LOCK);
            VALUE_MAP.put("CLEAR", CLEAR);
            VALUE_MAP.put("SHOW", SHOW);
            VALUE_MAP.put("META", META);
            VALUE_MAP.put("INCREMENT", INCREMENT);
            VALUE_MAP.put("SNAPSHOT", SNAPSHOT);
        }

        public static CheckCciExtraCmd of(String stringVal) {
            if (TStringUtil.isBlank(stringVal)) {
                return DEFAULT;
            }

            return VALUE_MAP.getOrDefault(stringVal, UNKNOWN);
        }
    }

}
