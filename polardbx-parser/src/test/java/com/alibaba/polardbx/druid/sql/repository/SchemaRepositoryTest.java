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

package com.alibaba.polardbx.druid.sql.repository;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * created by ziyang.lb
 **/
public class SchemaRepositoryTest {

    public final static SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };

    SchemaRepository repository;

    @Before
    public void before() {
        repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("d`b1");
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testDropTable() {
        String sql = "create table if not exists `gxw_test``backtick`("
            + "  `col-minus` int,"
            + "  c2 int,"
            + "  _drds_implicit_id_ bigint auto_increment,"
            + "  primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sql, FEATURES);
        SchemaObject tableMeta = findTable(repository, "gxw_test`backtick");
        Assert.assertNotNull(tableMeta);

        sql = "drop table if exists `gxw_test``backtick`";
        repository.console(sql, FEATURES);
        tableMeta = findTable(repository, "gxw_test`backtick");
        Assert.assertNull(tableMeta);
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testCreateIndex() {
        String sql = "create table if not exists `ng` ("
            + "        `2kkxyfni` char(1) not null comment 'kkvy',"
            + "        `i1iavmsfrvs1cpk` char(5),"
            + "        _drds_implicit_id_ bigint auto_increment,"
            + "        primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sql, FEATURES);
        SchemaObject tableMeta = findTable(repository, "ng");
        Assert.assertNotNull(tableMeta);

        sql = "create local index `ng`  on `or0b` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findTable(repository, "ng");
        Assert.assertNotNull(tableMeta);

        tableMeta = findIndex(repository, "ng");
        Assert.assertNotNull(tableMeta);

        sql = "create local index `ab``dc`  on `ng` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ab`dc");
        Assert.assertNotNull(tableMeta);

        sql = "drop index `ab``dc` on `ng`";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ab`dc");
        Assert.assertNull(tableMeta);
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testRename() {

        String sqlCreate = "create table if not exists `gxw_test``backtick`("
            + "  `col-minus` int,"
            + "  c2 int,"
            + "  _drds_implicit_id_ bigint auto_increment,"
            + "  primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        repository.console(sqlCreate, FEATURES);
        SchemaObject table1 = findTable(repository, "gxw_test`backtick");
        Assert.assertNotNull(table1);

        String sqlRename = "rename table `gxw_test``backtick` to `gxw_test``backtick_new`";
        repository.console(sqlRename, FEATURES);
        SchemaObject table2 = findTable(repository, "gxw_test`backtick");
        SchemaObject table3 = findTable(repository, "gxw_test`backtick_new");
        Assert.assertNull(table2);
        Assert.assertNotNull(table3);
    }

    @Test
    public void testAlterTable() {
        testAlterTableInternal("gxw_test``backtick", "gxw_test`backtick");
        testAlterTableInternal("``gxw_test``backtick``", "`gxw_test`backtick`");
        testAlterTableInternal("abc", "abc");
    }

    private void testAlterTableInternal(String tableName1, String tableName2) {
        String sql1 = "create table if not exists `" + tableName1 + "` ("
            + " `col-minus` int, "
            + " c2 int, "
            + " _drds_implicit_id_ bigint auto_increment, "
            + " primary key (_drds_implicit_id_)"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci";
        String sql2 = "alter table `" + tableName1 + "` add c3 int";

        repository.console(sql1, FEATURES);
        repository.console(sql2, FEATURES);

        SchemaObject table = findTable(repository, tableName2);
        SQLColumnDefinition columnDefinition1 = table.findColumn("c2");
        Assert.assertNotNull(columnDefinition1);

        SQLColumnDefinition columnDefinition2 = table.findColumn("c3");
        Assert.assertNotNull(columnDefinition2);
    }

    private SchemaObject findTable(SchemaRepository repository, String tableName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.findTable(tableName);
    }

    private SchemaObject findIndex(SchemaRepository repository, String indexName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.indexes.get(FnvHash.hashCode64(indexName));
    }
}
