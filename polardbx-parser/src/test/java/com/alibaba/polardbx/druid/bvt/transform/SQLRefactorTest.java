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

package com.alibaba.polardbx.druid.bvt.transform;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.repository.SchemaResolveVisitor;
import com.alibaba.polardbx.druid.sql.transform.SQLRefactorVisitor;
import com.alibaba.polardbx.druid.sql.transform.TableMapping;
import junit.framework.TestCase;

public class SQLRefactorTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "select * from t";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "f2");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT `f2` AS f1\n" +
                "FROM `t2`", stmt.toString());
    }

    public void test_1() throws Exception {
        String sql = "select a.f1 from t a";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "f2");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT a.`f2` AS f1\n" +
                "FROM `t2` a", stmt.toString());
    }

    public void test_alias() throws Exception {
        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint, fid bigint)");

        String sql = "select t.f1 from t t where t.fid > 0";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "f2");
        tableMapping.addColumnMapping("fid", "kid");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT t.`f2` AS f1\n" +
                "FROM `t2` t\n" +
                "WHERE t.`kid` > 0", stmt.toString());
    }

    public void test_alias_2() throws Exception {
        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint, fid bigint)");

        String sql = "select t.f1 from t t where t.fid > 0";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        TableMapping tableMapping = new TableMapping("t", "in");
        tableMapping.addColumnMapping("f1", "f2");
        tableMapping.addColumnMapping("fid", "kid");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT t.`f2` AS f1\n" +
                "FROM `in` t\n" +
                "WHERE t.`kid` > 0", stmt.toString());
    }

    public void test_join_0() throws Exception {
        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        {
            TableMapping tableMapping = new TableMapping("t1", "x1");
            tableMapping.addColumnMapping("f1", "c1");
            tableMapping.addColumnMapping("id", "cid");

            refactor.addMapping(tableMapping);
        }
        {
            TableMapping tableMapping = new TableMapping("t2", "x2");
            tableMapping.addColumnMapping("f1", "k1");
            tableMapping.addColumnMapping("id", "kid");

            refactor.addMapping(tableMapping);
        }

        String sql = "select a.f1 from t1 a inner join t2 b on a.id = b.id";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t1(f1 bigint,id bigint)");
        repo.acceptDDL("create table t2(f1 bigint,id bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        stmt.accept(refactor);

        assertEquals("SELECT a.`c1` AS f1\n" +
                "FROM `x1` a\n" +
                "\tINNER JOIN `x2` b ON a.`cid` = b.`kid`", stmt.toString());
    }

    public void test_join_1() throws Exception {
        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        {
            TableMapping tableMapping = new TableMapping("t1", "x1");
            tableMapping.addColumnMapping("f1", "c1");
            tableMapping.addColumnMapping("id", "cid");

            refactor.addMapping(tableMapping);
        }
        {
            TableMapping tableMapping = new TableMapping("t2", "x2");
            tableMapping.addColumnMapping("f1", "k1");
            tableMapping.addColumnMapping("id", "kid");

            refactor.addMapping(tableMapping);
        }

        String sql = "select a.f1 from (select f1 from t1) a inner join t2 b on a.id = b.id";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t1(f1 bigint,id bigint)");
        repo.acceptDDL("create table t2(f1 bigint,id bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        stmt.accept(refactor);

        assertEquals("SELECT a.f1\n" +
                "FROM (\n" +
                "\tSELECT `c1` AS f1\n" +
                "\tFROM `x1`\n" +
                ") a\n" +
                "\tINNER JOIN `x2` b ON a.id = b.`kid`", stmt.toString());
    }

    public void test_join_2() throws Exception {
        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        {
            TableMapping tableMapping = new TableMapping("t1", "x1");
            tableMapping.addColumnMapping("f1", "c1");
            tableMapping.addColumnMapping("id", "cid");

            refactor.addMapping(tableMapping);
        }
        {
            TableMapping tableMapping = new TableMapping("t2", "x2");
            tableMapping.addColumnMapping("f1", "k1");
            tableMapping.addColumnMapping("id", "kid");

            refactor.addMapping(tableMapping);
        }

        String sql = "select a.f1 from t1 a inner join t2 on a.id = t2.id";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t1(f1 bigint,id bigint)");
        repo.acceptDDL("create table t2(f1 bigint,id bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        stmt.accept(refactor);

        assertEquals("SELECT a.`c1` AS f1\n" +
                "FROM `x1` a\n" +
                "\tINNER JOIN `x2` ON a.`cid` = `x2`.`kid`", stmt.toString());
    }

    public void test_join_3() throws Exception {
        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        {
            TableMapping tableMapping = new TableMapping("t1", "x1");
            tableMapping.addColumnMapping("f1", "c1");
            tableMapping.addColumnMapping("id_x", "cid");

            refactor.addMapping(tableMapping);
        }
        {
            TableMapping tableMapping = new TableMapping("t2", "x2");
            tableMapping.addColumnMapping("f1", "k1");
            tableMapping.addColumnMapping("id_y", "kid");

            refactor.addMapping(tableMapping);
        }

        String sql = "select a.f1 from t1 a inner join t2 on id_x = id_y";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t1(f1 bigint,id_x bigint)");
        repo.acceptDDL("create table t2(f1 bigint,id_y bigint)");

        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);


        stmt.accept(refactor);

        assertEquals("SELECT a.`c1` AS f1\n" +
                "FROM `x1` a\n" +
                "\tINNER JOIN `x2` ON `cid` = `kid`", stmt.toString());
    }


    public void test_groupBy() throws Exception {

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint)");

        String sql = "select f1 as f1 from t group by f1";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "SOME");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT `SOME` AS f1\n" +
                "FROM `t2`\n" +
                "GROUP BY f1", stmt.toString());
    }

    public void test_having() throws Exception {

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint, f2 bigint)");

        String sql = "select f1 as f1, count(distinct f2) f2 from t group by f1 having f2 > 1";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "SOME");
        tableMapping.addColumnMapping("f2", "ANY");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT `SOME` AS f1, count(DISTINCT `ANY`) AS f2\n" +
                "FROM `t2`\n" +
                "GROUP BY f1\n" +
                "HAVING f2 > 1", stmt.toString());
    }

    public void test_having_1() throws Exception {

        SchemaRepository repo = new SchemaRepository(DbType.mysql);
        repo.acceptDDL("create table t(f1 bigint, f2 bigint)");

        String sql = "select f1 as f1, count(distinct f2) f2 from t group by f1 having count(distinct f2) > 1";

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        repo.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        TableMapping tableMapping = new TableMapping("t", "t2");
        tableMapping.addColumnMapping("f1", "SOME");
        tableMapping.addColumnMapping("f2", "ANY");

        SQLRefactorVisitor refactor = new SQLRefactorVisitor(DbType.mysql);
        refactor.addMapping(tableMapping);

        stmt.accept(refactor);

        assertEquals("SELECT `SOME` AS f1, count(DISTINCT `ANY`) AS f2\n" +
                "FROM `t2`\n" +
                "GROUP BY f1\n" +
                "HAVING count(DISTINCT `ANY`) > 1", stmt.toString());
    }
}
