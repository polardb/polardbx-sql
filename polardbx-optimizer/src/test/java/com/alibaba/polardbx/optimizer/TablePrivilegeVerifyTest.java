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

package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.Map;

/**
 * @author arnkore 2016-10-14 18:30
 */
public class TablePrivilegeVerifyTest {

    @Test
    public void testSelect() throws SQLSyntaxErrorException {
        DbPriv dbPriv = null;
        TbPriv tbPriv1 = new TbPriv("db1", "table1");
        tbPriv1.loadPriv(0x1FF);
        TbPriv tbPriv2 = new TbPriv("db1", "table2");
        tbPriv2.loadPriv(0x1FF);
        Map<String, TbPriv> tbPrivMap = Maps.newHashMap();
        tbPrivMap.put("table1", tbPriv1);
        tbPrivMap.put("table2", tbPriv2);
        tbPrivMap.put("table3", tbPriv2);

        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setTablePrivilegeMap(tbPrivMap);

        String sql = "select * from table1 a join table2 b on a.id = b.id where a.id in (select id from table3)";
        checkPriv(sql);
    }

    @Test
    public void testUpdate() throws SQLSyntaxErrorException {
        DbPriv dbPriv = null;
        TbPriv tbPriv2 = new TbPriv("db1", "table2");
        tbPriv2.loadPriv(0x1FF);
        TbPriv tbPriv3 = new TbPriv("db1", "table3");
        tbPriv3.loadPriv(0x1FF);
        TbPriv tbPriv4 = new TbPriv("db1", "table4");
        tbPriv4.loadPriv(0x1FF);
        Map<String, TbPriv> tbPrivMap = Maps.newHashMap();
        tbPrivMap.put("table2", tbPriv2);
        tbPrivMap.put("table3", tbPriv3);
        tbPrivMap.put("table4", tbPriv4);
        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setTablePrivilegeMap(tbPrivMap);
        String sql =
            "update table2 set col1=?,col2=? where id=(select id from (select id from table4 limit 1) as a join table3 on table3.id = a.id)";
        checkPriv(sql);
    }

    @Test
    public void testDelete() throws SQLSyntaxErrorException {
        DbPriv dbPriv = null;
        TbPriv tbPriv3 = new TbPriv("db1", "table3");
        tbPriv3.loadPriv(0x1BB);
        TbPriv tbPriv4 = new TbPriv("db1", "table4");
        tbPriv4.loadPriv(0x1FF);
        Map<String, TbPriv> tbPrivMap = Maps.newHashMap();
        tbPrivMap.put("table3", tbPriv3);
        tbPrivMap.put("table4", tbPriv4);
        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setTablePrivilegeMap(tbPrivMap);
        String sql = "delete from sc.table3 where id=(select id from table4)";
        checkPriv(sql);
    }

    protected void checkPriv(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.IgnoreNameQuotes);
        try {
            List<SQLStatement> stmtList = parser.parseStatementList();

            for (SQLStatement statement : stmtList) {
                ContextParameters context = new ContextParameters(false);
                FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, new ExecutionContext());
                statement.accept(visitor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsert() throws SQLSyntaxErrorException {
        DbPriv dbPriv = null;
        TbPriv tbPriv1 = new TbPriv("db1", "table1");
        tbPriv1.loadPriv(0x000);
        Map<String, TbPriv> tbPrivMap = Maps.newHashMap();
        tbPrivMap.put("table1", tbPriv1);
        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setTablePrivilegeMap(tbPrivMap);

//        String sql = "insert into table1 values(?,?,?,?) on duplicate key update col1 = ?";
//        checkPriv(sql);

        String sql = "insert into sc.table1 values(?,?,?,?)";
        checkPriv(sql);

//        sql = "insert into table1 select * from table2";
//        checkPriv(sql);
        TbPriv tbPriv2 = new TbPriv("db1", "table2");
        tbPriv2.loadPriv(0x1FF);
        tbPrivMap.put("table2", tbPriv2);
        privilegeContext.setTablePrivilegeMap(tbPrivMap);
        checkPriv(sql);
    }
}
