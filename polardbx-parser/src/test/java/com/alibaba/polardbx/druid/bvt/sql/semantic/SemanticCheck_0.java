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

package com.alibaba.polardbx.druid.bvt.sql.semantic;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.semantic.SemanticException;
import junit.framework.TestCase;

public class SemanticCheck_0 extends TestCase {
    public void test_for_semantic_0() throws Exception {
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleStatement(
                "create table t1 (f0 varchar(10), f0 varchar(10))"
                , DbType.mysql);
        assertTrue(stmt.containsDuplicateColumnNames());
    }

    public void test_for_semantic_1() throws Exception {
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) SQLUtils.parseSingleStatement(
                "create table t1 (f0 varchar(10), f0 varchar(10))"
                , DbType.mysql);

        Exception error = null;
        try {
            stmt.containsDuplicateColumnNames(true);
        } catch (SemanticException ex) {
            error = ex;
        }
        assertNotNull(error);
    }
}
