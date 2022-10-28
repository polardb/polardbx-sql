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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.druid.sql.parser.SQLUnsupportedException;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UnsupportedSqlTest extends BaseTestCase {

    private List<String> unsupportedSqlList = new ArrayList<>();

    @Before
    public void addUnsupportedSql() {
        unsupportedSqlList.add("ALTER INSTANCE ROTATE INNODB MASTER KEY");
    }

    @Test
    public void testAlterInstanceRotateInnodbMasterKey() throws SQLException {
        Connection connection = getPolardbxConnection();
        for (String sql : unsupportedSqlList) {
            JdbcUtil.executeUpdateFailed(connection, sql, SQLUnsupportedException.UNSUPPORTED_SQL_WARN + sql);
        }
    }

}
