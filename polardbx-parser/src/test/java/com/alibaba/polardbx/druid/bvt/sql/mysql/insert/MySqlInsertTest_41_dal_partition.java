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
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;

import java.util.List;

public class MySqlInsertTest_41_dal_partition extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert into dla_meta_orc.async_task partition(dt='2018-11-06', region='cn-hangzhou')\n" +
                "select cluster_name, table_schema, id, task_name, status, process_id, creator_id, create_time\n" +
                "       update_time, mpp_query_id, cancellable_task, cloudap_status, uid, complexity, scanned_data_bytes,\n" +
                "       resource_number, async_task, result_file_oss_endpoint, result_file_oss_file, row_colunt, elapse_time,\n" +
                "       client_host, client_port, state, server_host, server_port, connection_id, composition, mpp_query_time,\n" +
                "       server_process_id\n" +
                "from dla_meta.async_task\n" +
                "where `day` in ('2018-11-06') and region in ('cn-hangzhou');\n";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals("INSERT INTO dla_meta_orc.async_task PARTITION (dt='2018-11-06', region='cn-hangzhou')\n" +
                "SELECT cluster_name, table_schema, id, task_name, status\n" +
                "\t, process_id, creator_id, create_time AS update_time, mpp_query_id, cancellable_task\n" +
                "\t, cloudap_status, uid, complexity, scanned_data_bytes, resource_number\n" +
                "\t, async_task, result_file_oss_endpoint, result_file_oss_file, row_colunt, elapse_time\n" +
                "\t, client_host, client_port, state, server_host, server_port\n" +
                "\t, connection_id, composition, mpp_query_time, server_process_id\n" +
                "FROM dla_meta.async_task\n" +
                "WHERE `day` IN ('2018-11-06')\n" +
                "\tAND region IN ('cn-hangzhou');", insertStmt.toString());

    }

}
