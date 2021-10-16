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

package com.alibaba.polardbx.druid.bvt.sql.mysql.alterTable;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author shicai.xsc 2018/9/13 下午3:35
 * @desc
 * @since 5.0.0.0
 */
public class MySqlAlterTableTest46_add_column extends TestCase {
    public void test_0() throws Exception {
        String sql = "ALTER TABLE test_pk ADD COLUMN remark2 varchar(255) DEFAULT NULL , ALGORITHM=inplace,LOCK=NONE";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        assertEquals(1, stmtList.size());
        SQLStatement stmt = stmtList.get(0);
        assertEquals("ALTER TABLE test_pk\n" +
                "\tADD COLUMN remark2 varchar(255) DEFAULT NULL,\n" +
                "\tALGORITHM = inplace,\n" +
                "\tLOCK = NONE", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "ALTER TABLE test_pk modify COLUMN remark2 varchar(32) DEFAULT NULL , ALGORITHM=copy,LOCK=SHARED";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        assertEquals(1, stmtList.size());
        SQLStatement stmt = stmtList.get(0);
        assertEquals("ALTER TABLE test_pk\n" +
            "\tMODIFY COLUMN remark2 varchar(32) DEFAULT NULL,\n" +
            "\tALGORITHM = copy,\n" +
            "\tLOCK = SHARED", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "ALTER TABLE `user_basic` ADD `dtu` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '用户dtu' AFTER `character`;";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        assertEquals(1, stmtList.size());
        SQLStatement stmt = stmtList.get(0);
        assertEquals("ALTER TABLE `user_basic`\n"
            + "\tADD COLUMN `dtu` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '用户dtu' AFTER `character`;", stmt.toString());
    }

}
