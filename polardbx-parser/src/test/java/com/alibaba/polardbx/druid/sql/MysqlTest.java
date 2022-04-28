/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Ignore;

import java.util.List;

@Ignore
public class MysqlTest extends TestCase {
	protected String output(List<SQLStatement> stmtList) {
		StringBuilder out = new StringBuilder();
		MySqlOutputVisitor visitor = new MySqlOutputVisitor(out);

		for (SQLStatement stmt : stmtList) {
			stmt.accept(visitor);
		}

		return out.toString();
	}
	
   protected void print(List<SQLStatement> stmtList) {
		String text = output(stmtList);
		String outputProperty = System.getProperty("fastsql.output");
		if ("false".equals(outputProperty)) {
			return;
		}
		System.out.println(text);
	}


	protected void parseTrue(String sql, String except) {
		SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
		Assert.assertEquals(except, SQLUtils.toMySqlString(statement));
	}

	protected SQLStatement parse(String sql) {
		return SQLUtils.parseSingleMysqlStatement(sql);
	}

	protected List<SQLStatement> parseList(String sql) {
		MySqlStatementParser parser = new MySqlStatementParser(sql);
		return parser.parseStatementList();
	}

	protected void parseFalse(String sql, String exceptError) {
	    Exception ex = null;
		try {
			SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
		} catch (ParserException e) {
		    ex = e;
		}
		Assert.assertNotNull("Excepted fail with exception: " + exceptError, ex);
		Assert.assertEquals(exceptError, ex.getMessage());
	}

}
