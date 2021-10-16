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

public class MySqlSchemaStatVisitorTest11
        extends TestCase {

	public void test_0() throws Exception {
		String sql = "SELECT\n" +
				"    -- 指标\n" +
				"    COUNT(DISTINCT(CONCAT(IFNULL(main.date, '-'),IFNULL(main.data_type, '-'),IFNULL(main.channel_id, '-'),IFNULL(main.channel_name, '-')))) as cnt, \n" +
				"    IF(sum(main.last_hourly_active) <> 0, sum(main.last_hourly_consumption_real) / sum(main.last_hourly_active), 0) as inner_cpa, IF(sum(main.yesterday_last_hourly_active) <> 0, sum(main.yesterday_last_hourly_consumption_real) /sum(main.yesterday_last_hourly_active), 0) as inner_yesterday_cpa, IF(sum(main.last_week_last_hourly_active) <> 0,sum(main.last_week_last_hourly_consumption_real) / sum(main.last_week_last_hourly_active),0) as inner_last_week_cpa, sum(main.material_impression) as material_impression \n" +
				"FROM (\n" +
				"        SELECT aadr.date as date, \n" +
				"if(\n" +
				"    aadr.attribution_status = 2\n" +
				"    , CASE 'HAS_ATTRIBUTION' \n" +
				"        WHEN 'HAS_ATTRIBUTION' THEN '已归因数据'\n" +
				"        WHEN 'CAN_ATTRIBUTION' THEN if(\"can_attribution\" = \"can_attribution\", '已归因数据', '未归因数据')\n" +
				"        WHEN 'CAN_NOT_ATTRIBUTION' THEN if(\"can_attribution\" = \"can_attribution\", '已归因数据', '未归因数据')\n" +
				"        ELSE '-'\n" +
				"      END\n" +
				"    , '已归因数据'\n" +
				") as data_type\n" +
				", c.id as channel_id, c.name as channel_name, aadr.last_hourly_active as last_hourly_active, mdd.last_hour_consumption_real as last_hourly_consumption_real, aadr.yesterday_last_hourly_active as yesterday_last_hourly_active, mdd.yesterday_last_hourly_consumption_real as yesterday_last_hourly_consumption_real, sum(main.last_week_last_hourly_active) as last_week_last_hourly_active, sum(main.last_week_last_hourly_consumption_real) as last_week_last_hourly_consumption_real, mdd.material_impression as material_impression, mdd.consumption_real as consumption_real FROM midujisu_full_att_adid_active_daily_report_v1 AS aadr LEFT JOIN midujisu_agent_account_rebate AS aar ON aar.date = aadr.date and aadr.agent_account_id = aar.agent_account_id LEFT JOIN midujisu_agent AS a ON aar.cur_agent_id = a.id LEFT JOIN midujisu_agent_account AS aa ON aa.id = aadr.agent_account_id LEFT JOIN midujisu_channel AS c ON aa.channel_id = c.id LEFT JOIN midujisu_marketing_daily_data_v2 AS mdd ON aadr.date = mdd.date and aadr.agent_account_id = mdd.agent_account_id and aadr.dtu = mdd.dtu and aadr.campaign_id = mdd.campaign_id and aadr.adgroup_id = mdd.adgroup_id and aadr.ad_id = mdd.ad_id\n" +
				"     WHERE aadr.date = '2019-10-29' AND aa.account_type IN('1') AND aadr.source NOT IN ('device_recall_action') \n" +
				"     HAVING last_hourly_active > 0 OR last_hourly_consumption_real > 0 OR yesterday_last_hourly_active > 0 OR yesterday_last_hourly_consumption_real > 0 OR last_week_last_hourly_active > 0 OR last_week_last_hourly_consumption_real > 0 OR material_impression > 0 OR consumption_real > 0\n" +
				") main";

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

		assertEquals(6, visitor.getTables().size());
		assertEquals(29, visitor.getColumns().size());
//		assertEquals("[employee.no IN (\"1\", \"2\")]", visitor.getConditions().toString());
		// Column("users", "name")));

	}

}
