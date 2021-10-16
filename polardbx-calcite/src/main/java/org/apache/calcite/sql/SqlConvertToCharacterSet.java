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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * @version 1.0
 */
public class SqlConvertToCharacterSet extends SqlAlterSpecification {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CONVERT TO CHARACTER SET", SqlKind.CONVERT_TO_CHARACTER_SET);

    final String charset;
    final String collate;

    public SqlConvertToCharacterSet(String charset, String collate, SqlParserPos pos) {
        super(pos);
        this.charset = charset;
        this.collate = collate;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    public String getCharset() {
        return charset;
    }

    public String getCollate() {
        return collate;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT,
            "CONVERT TO CHARACTER SET", "");

        writer.keyword(charset);

        if (StringUtils.isNotEmpty(collate)) {
            writer.sep("collate");
            writer.keyword(collate);
        }

        writer.endList(frame);
    }

}
