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
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

public class SqlConvertAllSequences extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR =
        new SqlAffectedRowsOperator("CONVERT_ALL_SEQUENCES", SqlKind.CONVERT_ALL_SEQUENCES);

    private final Type fromType;
    private final Type toType;
    private final String schemaName;
    private final boolean allSchemata;

    public SqlConvertAllSequences(SqlParserPos pos, Type fromType, Type toType, String schemaName,
                                  boolean allSchemata) {
        super(OPERATOR, SqlParserPos.ZERO);
        this.fromType = fromType;
        this.toType = toType;
        this.schemaName = schemaName;
        this.allSchemata = allSchemata;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("CONVERT ALL SEQUENCES FROM");
        writer.print(fromType.name());
        writer.keyword(" TO");
        writer.print(toType.name());
        if (!allSchemata) {
            writer.keyword("FOR");
            writer.print(schemaName);
        }
        writer.endList(selectFrame);
    }

    public Type getFromType() {
        return fromType;
    }

    public Type getToType() {
        return toType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isAllSchemata() {
        return allSchemata;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CONVERT_ALL_SEQUENCES;
    }

}
