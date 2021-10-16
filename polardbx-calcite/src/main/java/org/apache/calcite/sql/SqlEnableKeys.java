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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/12/13 8:15 PM
 */
public class SqlEnableKeys extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ENABLE KEYS", SqlKind.ENABLE_KEYS);

    public static enum EnableType {
        ENABLE, DISABLE
    }

    private final SqlLiteral enableType;

    public SqlEnableKeys(SqlParserPos pos, EnableType enableType) {
        super(pos);
        this.enableType = SqlLiteral.createSymbol(enableType, pos);
    }

    public EnableType getEnableType() {
        return this.enableType.getValueAs(EnableType.class);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of((SqlNode) enableType);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "", "");

        enableType.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }
}
