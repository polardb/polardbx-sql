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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateTableTest108_drds extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "create table ARCHIVE_ERR_RECORD\n" +
                "(\n" +
                "SUBS_ORDER_ID numeric(18,0) not null comment '订单编号',\n" +
                "ERR_MSG text comment '失败的消息结构',\n" +
                "ERR_REASON varchar(255) comment '失败原因',\n" +
                "PART_ID integer not null comment '分区标识（取订单编号中的月份）'\n" +
                ")\n" +
                "DBPARTITION BY HASH(SUBS_ORDER_ID)\n" +
                "TBPARTITION BY UNI_HASH(PART_ID) TBPARTITIONS 12;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(4, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE ARCHIVE_ERR_RECORD (\n" +
                "\tSUBS_ORDER_ID numeric(18, 0) NOT NULL COMMENT '订单编号',\n" +
                "\tERR_MSG text COMMENT '失败的消息结构',\n" +
                "\tERR_REASON varchar(255) COMMENT '失败原因',\n" +
                "\tPART_ID integer NOT NULL COMMENT '分区标识（取订单编号中的月份）'\n" +
                ")\n" +
                "DBPARTITION BY HASH(SUBS_ORDER_ID)\n" +
                "TBPARTITION BY UNI_HASH(PART_ID) TBPARTITIONS 12;", stmt.toString());
    }
}