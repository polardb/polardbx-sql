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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TODO 功能描述
 *
 * @author lijun.cailj 2018/3/8
 */
public class MysqlResolveTest {

    @Test
    public void test_1() {

        SchemaRepository repository = new SchemaRepository(DbType.mysql);

        repository.console("create table t_emp(emp_id bigint, name varchar(20));");
        repository.console("create table t_org(org_id bigint, name varchar(20));");

        String sql = "delete from t_emp where name = '12'";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);
        assertEquals(1, stmtList.size());

        SQLDeleteStatement stmt = (SQLDeleteStatement) stmtList.get(0);

        repository.resolve(stmt);
    }
}
