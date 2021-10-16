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

public class SqlReplace extends SqlInsert {
	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("REPLACE", SqlKind.REPLACE);

	public SqlReplace(SqlParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source,
			SqlNodeList columnList, int batchSize, SqlNodeList hints) {
		super(pos, keywords, targetTable, source, columnList, batchSize, hints);
	}

	@Override
	public SqlKind getKind() {
		return SqlKind.REPLACE;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.startList(SqlWriter.FrameTypeEnum.SELECT);
		writer.sep("REPLACE");
		for (SqlNode keyword : keywords) {
			keyword.unparse(writer, 0, 0);
		}
		writer.sep("INTO");
		final int opLeft = getOperator().getLeftPrec();
		final int opRight = getOperator().getRightPrec();
		targetTable.unparse(writer, opLeft, opRight);
		if (columnList != null) {
			columnList.unparse(writer, opLeft, opRight);
		}
		writer.newlineAndIndent();
		if (source.getKind() == SqlKind.UNION) {  // if it's a union, don't add brackets.
			writer.getDialect().unparseCall(writer, (SqlCall) source, 0, 0);
		} else {
			source.unparse(writer, 0, 0);
		}
	}
}
