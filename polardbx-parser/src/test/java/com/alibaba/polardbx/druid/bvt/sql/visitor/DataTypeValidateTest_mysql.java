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

package com.alibaba.polardbx.druid.bvt.sql.visitor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SQLDataTypeValidator;
import junit.framework.TestCase;

import java.util.List;

public class DataTypeValidateTest_mysql extends TestCase {
    private DbType dbType = DbType.mysql;
    public void test_odps() throws Exception {
        String sql = "create table t (fid int, fname varchar)";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        SQLDataTypeValidator.check(stmtList);
    }

    public void test_1() throws Exception {
        String sql = "create table t (fid int, fvalue array<int>)";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        Exception error = null;
        try {
            SQLDataTypeValidator.check(stmtList);
        } catch (FastsqlException e) {
            error = e;
        }
        assertNotNull(error);
        assertEquals("illegal dataType : ARRAY, column fvalue", error.getMessage());
    }

    public void test_fail() throws Exception {
        String sql = "create table t (fid int, fname varcharxx)";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        Exception error = null;
        try {
            SQLDataTypeValidator.check(stmtList);
        } catch (FastsqlException e) {
            error = e;
        }
        assertNotNull(error);
        assertEquals("illegal dataType : varcharxx, column fname", error.getMessage());
    }
}
