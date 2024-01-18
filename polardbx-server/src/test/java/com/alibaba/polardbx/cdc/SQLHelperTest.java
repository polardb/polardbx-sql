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

package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.alibaba.polardbx.cdc.SQLHelper.filterColumns;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * created by ziyang.lb
 **/
public class SQLHelperTest {

    @Test
    public void testFilterColumns() {
        String sqlInput = "CREATE TABLE `omc_index_col_unique_test_gsi1` \n"
            + "( `a` int(11) NOT NULL,\n"
            + " `B` bigint(20) DEFAULT NULL, \n"
            + " `c` bigint(20) DEFAULT NULL, \n"
            + " PRIMARY KEY (`a`), \n"
            + " UNIQUE KEY `b` (`b`), \n"
            + " UNIQUE KEY `c` USING BTREE (`c`), \n"
            + " INDEX `idx_i` using btree(`B`),"
            + " UNIQUE KEY `b_zplr` \n"
            + " USING BTREE (`c`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        String sqlOutput = "CREATE TABLE `omc_index_col_unique_test_gsi1` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`c` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`),\n"
            + "\tUNIQUE KEY `c` USING BTREE (`c`),\n"
            + "\tUNIQUE KEY `b_zplr` USING BTREE (`c`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";

        List<SQLStatement> statementList =
            SQLParserUtils.createSQLStatementParser(sqlInput, DbType.mysql, SQL_PARSE_FEATURES).parseStatementList();
        MySqlCreateTableStatement createStmt = (MySqlCreateTableStatement) statementList.get(0);
        filterColumns(createStmt, Lists.newArrayList("b"));
        String result = createStmt.toString();
        Assert.assertEquals(sqlOutput, result);
    }
}
