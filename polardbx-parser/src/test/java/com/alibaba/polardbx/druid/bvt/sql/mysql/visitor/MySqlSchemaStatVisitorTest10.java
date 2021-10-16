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
package com.alibaba.polardbx.druid.bvt.sql.mysql.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import junit.framework.TestCase;

import java.util.List;

public class MySqlSchemaStatVisitorTest10 extends TestCase {

	public void test_0() throws Exception {
		String sql = "SELECT * \n" +
				"FROM t_plutus_customer_charge c\n" +
				"\tSTRAIGHT JOIN t_plutus_customer_po p ON c.po_number = p.po_number\n" +
				"WHERE c.customer_id = 130431\n" +
				"\tAND c.deleted = false\n" +
				"\tAND c.business_type != 'logistics'\n" +
				"\tAND c.settlement_type IN ('order', 'deduction')\n" +
				"\tAND c.po_number IS NOT NULL\n" +
				"\tAND p.po_business_type IN ('export', 'TOC', 'mixed', 'self_export')\n" +
				"\tAND c.currency = 'CNY'\n" +
				"\tAND c.gmt_create >= '2016-09-01 15:00:00'\n" +
				"\tAND c.invoice_state = 'applying'\n" +
				"\tAND ifnull(c.used_status, 0) != 8\n" +
				"GROUP BY c.po_number\n" +
				"LIMIT 0, 20";

//		sql = "select columnName from table1 where id in (select id from table3 where name = ?)";
		MySqlStatementParser parser = new MySqlStatementParser(sql);
		List<SQLStatement> statementList = parser.parseStatementList();
		SQLStatement stmt = statementList.get(0);

		assertEquals(1, statementList.size());

		MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
		stmt.accept(visitor);

		System.out.println(stmt.toString());

//		System.out.println(sql);
		System.out.println("Tables : " + visitor.getTables());
		System.out.println("fields : " + visitor.getColumns());
		System.out.println(visitor.getConditions());

		assertEquals(2, visitor.getTables().size());
		assertEquals(13, visitor.getColumns().size());
//		assertEquals("[employee.no IN (\"1\", \"2\")]", visitor.getConditions().toString());
		// Column("users", "name")));

	}

}
