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

package com.alibaba.polardbx.druid.bvt.sql.repository;

import com.alibaba.polardbx.druid.FastsqlColumnAmbiguousException;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.repository.SchemaResolveVisitor.Option;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

/**
 * Created by wenshao on 03/08/2017.
 */
public class Resolve_ambiguous extends TestCase {
    protected SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

    protected void setUp() throws Exception {
        repository.console("create table t1 (fid bigint, fname varchar(20))");
        repository.console("create table t2 (fid bigint, fname varchar(20))");
    }

    public void test_for_issue() throws Exception {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select fid from t1 join t2 on t1.fid = f2.fid");

        FastsqlColumnAmbiguousException error = null;
        try {
            repository.resolve(stmt, Option.CheckColumnAmbiguous);
        } catch (FastsqlColumnAmbiguousException ex) {
            error = ex;
        }
        assertEquals("Column 'fid' is ambiguous", error.getMessage());
    }
}
