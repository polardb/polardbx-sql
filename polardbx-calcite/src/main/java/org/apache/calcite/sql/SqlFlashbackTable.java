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

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlFlashbackTable extends SqlCall {
	private static final SqlOperator OPERATOR = new SqlSpecialOperator("FLASHBACK TABLE", SqlKind.FLASHBACK_TABLE);

	private SqlIdentifier name;
	private SqlIdentifier renameTo;

	public SqlIdentifier getName() {
		return name;
	}

	public SqlIdentifier getRenameTo() {
		return renameTo;
	}

	public SqlFlashbackTable(SqlParserPos pos, SqlIdentifier name, SqlIdentifier renameTo) {
		super(pos);
		this.name = name;
		this.renameTo = renameTo;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("FLASHBACK");
		writer.keyword("TABLE");
		name.unparse(writer, leftPrec, rightPrec);
		writer.keyword("TO BEFORE DROP");
		if (renameTo != null) {
			writer.keyword("RENAME TO");
			renameTo.unparse(writer, leftPrec, rightPrec);
		}
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.<SqlNode>of(name, renameTo);
	}

	public void validate(SqlValidator validator, SqlValidatorScope scope) {
		// do nothing
	}

}
