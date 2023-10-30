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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLIndex;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * created by ziyang.lb
 **/
public class SchemaRepositoryTest {

    public final static String DEFAULT_SCHEMA = "d`b1";
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
        repository.setDefaultSchema(DEFAULT_SCHEMA);
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

        sql = "create local index `ng`  on `ng` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findTable(repository, "ng");
        Assert.assertNotNull(tableMeta);

        tableMeta = findIndex(repository, "ng", "ng");
        Assert.assertNotNull(tableMeta);

        sql = "create local index `ab``dc`  on `ng` ( `feesesihp3qx`   )";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ng", "ab`dc");
        Assert.assertNotNull(tableMeta);

        sql = "drop index `ab``dc` on `ng`";
        repository.console(sql, FEATURES);
        tableMeta = findIndex(repository, "ng", "ab`dc");
        Assert.assertNull(tableMeta);
    }

    //see Aone issue ,ID:39638018
    @Test
    public void testRenameTable() {
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

    @Test
    public void testAlgorithm() {
        String sql1 =
            "create table `omc_not_null_tbl_test_w3za_00001` ( \ta int primary key, \tb int not null ) dbpartition by hash(a)";
        String sql2 = "alter table `omc_not_null_tbl_test_w3za_00001` \tadd column `b_ehjp` int after `b`, "
            + "\talgorithm = default";
        repository.console(sql1, FEATURES);
        repository.console(sql2, FEATURES);
        SchemaObject table1 = findTable(repository, "omc_not_null_tbl_test_w3za_00001");
        SQLStatement statement = table1.getStatement();
        String sql3 = statement.toString();
        repository.console(sql3, FEATURES);
        checkSql(sql3);
    }

    @Test
    public void testCreateSequence() {
        String sql = "create sequence pxc_seq_64056c9e413d6f79544c4938f86c8d6e start with 1 cache 100000";
        repository.console(sql, FEATURES);
        SchemaObject object = findSequence(repository, "pxc_seq_64056c9e413d6f79544c4938f86c8d6e");
        Assert.assertEquals("CREATE SEQUENCE pxc_seq_64056c9e413d6f79544c4938f86c8d6e START WITH 1 CACHE 100000",
            object.getStatement().toString());
    }

    @Test
    public void testDropIndex() {
        repository.console("create table test_escaping_col_name (\n"
            + "  id int not null primary key,\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  dept int\n"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci");

        repository.console("create index idx_age on test_escaping_col_name(age)");
        Set<String> sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("alter table test_escaping_col_name add index idx_dept(`dept`)");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "idx_dept"), sets);

        repository.console("alter table test_escaping_col_name drop index idx_dept");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("drop index idx_age on test_escaping_col_name");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet(), sets);
    }

    //See aone issue, id : 46539884
    @Test
    public void testConstraintName() {
        String sql1 = " create table `t_test` (\n"
            + "        `id` bigint(20) unsigned not null auto_increment,\n"
            + "        `a` bigint(20) not null comment '',\n"
            + "        `b` bigint(20) not null comment '',\n"
            + "        `c` tinyint(4) not null comment '',\n"
            + "        `d` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `e` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `f` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `g` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `h` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `i` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `j` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `k` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `l` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `m` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `n` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `o` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `p` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `q` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `r` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `s` decimal(14, 4) not null default '0.0000' comment '',\n"
            + "        `t` timestamp not null default current_timestamp,\n"
            + "        `e` timestamp not null default current_timestamp on update current_timestamp,\n"
            + "        primary key (`id`),\n"
            + "        unique key `uk_x` (`a`, `b`, `c`),\n"
            + "        constraint unique `uk_xx` (`a`, `b`, `c`)\n"
            + ")";
        String sql2 = "alter table t_test add constraint `uk_y` unique (e, f, g, h)";
        String sql3 = "alter table t_test drop constraint uk_y";

        String sql4 = "alter table t_test add constraint `uk_h` unique (e, f, g, h)";
        String sql5 = "alter table t_test drop index uk_h";

        String sql6 = "alter table t_test add unique key uk_o(o,p,q)";
        String sql7 = "alter table t_test drop constraint uk_o";

        String sql8 = "alter table t_test add constraint `uk_f` unique (e, f, g, h)";
        String sql9 = "create index idx_z on t_test (g,h ,i ,j ,k ,l,m ,n)";
        String sql10 = "drop index idx_z on t_test";

        String sql11 = "alter table t_test drop primary key";
        String sql12 = "alter table t_test add constraint primary key(id)";

        repository.console(sql1);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql2);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_y"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql3);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql4);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_h"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql5);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql6);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_o"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql7);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        repository.console(sql8);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql9);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "idx_z"), findIndexes(DEFAULT_SCHEMA, "t_test"));
        repository.console(sql10);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"), findIndexes(DEFAULT_SCHEMA, "t_test"));

        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "primary"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
        repository.console(sql11);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
        repository.console(sql12);
        Assert.assertEquals(Sets.newHashSet("uk_x", "uk_xx", "uk_f", "primary"),
            findIndexes(DEFAULT_SCHEMA, "t_test", true));
    }

    @Test
    public void testUniqueKeyInColumn() {
        String sql1 = "CREATE TABLE IF NOT EXISTS `ZXe5GA6` (\n"
            + "  `aiMzgbaKVCIQtle` INT(1) UNSIGNED NULL COMMENT 'treSay',\n"
            + "  `V8R9mZFvUHxQ` MEDIUMINT UNSIGNED ZEROFILL COMMENT 'WenHosxfI3i',\n"
            + "  `RFKRrCAF` TIMESTAMP UNIQUE,\n"
            + "  `ctv` BIGINT(5) ZEROFILL NULL,\n"
            + "  `Vd` TINYINT UNSIGNED ZEROFILL UNIQUE COMMENT 'lysAE',\n"
            + "  `8` MEDIUMINT(4) ZEROFILL COMMENT 'Y',\n"
            + "  `H4rJ5c8d0N1C8Q` BIGINT UNSIGNED ZEROFILL NOT NULL,\n"
            + "  `iE69EIYRLOqXa3` DATE NOT NULL COMMENT 'VAHhex',\n"
            + "  `OsBUdkS` MEDIUMINT ZEROFILL COMMENT 'zgV7ojRAJKgu4XI',\n"
            + "  `LADuM` TIMESTAMP(0) COMMENT 'nkaLg0',\n"
            + "  `kO38Dx6gYUPRtBn` MEDIUMINT UNSIGNED ZEROFILL UNIQUE,\n"
            + "  KEY `Cb` USING HASH (`Vd`),\n"
            + "  INDEX `auto_shard_key_ctv` USING BTREE(`CTV`),\n"
            + "  INDEX `auto_shard_key_ie69eiyrloqxa3` USING BTREE(`IE69EIYRLOQXA3`),\n"
            + "  _drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "  PRIMARY KEY (_drds_implicit_id_)\n"
            + ")\n"
            + "DBPARTITION BY RIGHT_SHIFT(`ctv`, 9)\n"
            + "TBPARTITION BY YYYYMM(`iE69EIYRLOqXa3`) TBPARTITIONS 7";
        String sql2 = "DROP INDEX `ko38dx6gyuprtbn` ON `ZXe5GA6`";
        String sql3 = "alter table `ZXe5GA6` drop index RFKRrCAF";

        repository.console(sql1, FEATURES);
        Assert.assertEquals(Sets.newHashSet("kO38Dx6gYUPRtBn", "auto_shard_key_ie69eiyrloqxa3",
            "RFKRrCAF", "Vd", "auto_shard_key_ctv", "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));

        repository.console(sql2, FEATURES);
        Assert.assertEquals(Sets.newHashSet("auto_shard_key_ie69eiyrloqxa3", "RFKRrCAF",
            "Vd", "auto_shard_key_ctv", "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));

        repository.console(sql3, FEATURES);
        Assert.assertEquals(Sets.newHashSet("auto_shard_key_ie69eiyrloqxa3", "Vd", "auto_shard_key_ctv",
            "Cb"), findIndexes(DEFAULT_SCHEMA, "ZXe5GA6"));
    }

    @Test
    public void testRenameIndex() {
        repository.console("create table test_escaping_col_name (\n"
            + "  id int not null primary key,\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  dept int\n"
            + ") default character set = utf8mb4 default collate = utf8mb4_general_ci");

        repository.console("create index idx_age on test_escaping_col_name(age)");
        Set<String> sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age"), sets);

        repository.console("alter table test_escaping_col_name add index idx_dept(`dept`)");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "idx_dept"), sets);

        repository.console("alter table test_escaping_col_name rename index idx_dept to `bbb`");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("idx_age", "bbb"), sets);

        repository.console("alter table test_escaping_col_name rename index idx_age to `'`");
        sets = findIndexes(DEFAULT_SCHEMA, "test_escaping_col_name");
        Assert.assertEquals(Sets.newHashSet("bbb", "'"), sets);
    }

    @Test
    public void testLocalPartition() {
        String ddl = " CREATE TABLE `t_xxx` (\n"
            + "        `id` bigint(20) NOT NULL DEFAULT '0' COMMENT '',\n"
            + "        `dksl` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `dlsc` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `chw` smallint(6) NOT NULL DEFAULT '0' COMMENT '',\n"
            + "        `co2o` varchar(5000) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `cnx` varchar(200) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',\n"
            + "        `dow` varchar(36) NOT NULL DEFAULT '' COMMENT '',\n"
            + "        PRIMARY KEY USING BTREE (`id`, `create_time`),\n"
            + "        LOCAL KEY `_local_idx_xdfd` USING BTREE (`dksl`) COMMENT '',\n"
            + "        LOCAL KEY `_local_idx_kdfs` USING BTREE (`create_time`) COMMENT ''\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ROW_FORMAT = COMPACT COMMENT ''\n"
            + "LOCAL PARTITION BY RANGE (create_time)\n"
            + "STARTWITH '2022-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 3\n"
            + "PIVOTDATE NOW()\n"
            + "DISABLE SCHEDULE\u0000";
        repository.console(ddl);
    }

    @Test
    public void testCreateViewClone() {
        String sql =
            " create or replace algorithm=undefined definer=`admin`@`%` sql security definer view `v`(`c1`) as select a.pk from select_base_two_one_db_one_tb a join select_base_three_multi_db_one_tb b on a.pk = b.p";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        StringBuffer sb = new StringBuffer();
        stmtList.get(0).clone().output(sb);
        System.out.println(sb);
    }

    @Test
    public void testAlterView() {
        String sql =
            "alter algorithm=undefined definer=`admin`@`%` sql security definer view `v2_unrelated` as select 2 as r1";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql, FEATURES);
        stmtList.get(0);
    }

    @Test
    public void testDropPrimaryKey() {
        String tableName = "t";
        String createTable = " create table t(id bigint primary key , name varchar(20) UNIQUE, content text)";
        repository.console(createTable);
        SchemaObject table = repository.findTable(tableName);
        SQLColumnDefinition idDefine = table.findColumn("id");
        Assert.assertTrue(idDefine.isPrimaryKey());
        String alterSql = "alter table t drop primary key";
        repository.console(alterSql);
        table = repository.findTable(tableName);
        idDefine = table.findColumn("id");
        Assert.assertFalse(idDefine.isPrimaryKey());
    }

    @Test
    public void testDropPrimaryKey2() {
        String tableName = "t";
        String createTable = " create table t(id bigint , name varchar(20) UNIQUE, content text, primary key(id))";
        repository.console(createTable);
        SchemaObject table = repository.findTable(tableName);
        SQLColumnDefinition idDefine = table.findColumn("id");
        Assert.assertTrue(idDefine.isPrimaryKey());
        String alterSql = "alter table t drop primary key";
        repository.console(alterSql);
        table = repository.findTable(tableName);
        idDefine = table.findColumn("id");
        Assert.assertFalse(idDefine.isPrimaryKey());
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

    private static void checkSql(String sql) {
        List<SQLStatement> phyStatementList =
            SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES).parseStatementList();
        phyStatementList.get(0);
    }

    private SchemaObject findTable(SchemaRepository repository, String tableName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.findTable(tableName);
    }

    private SchemaObject findIndex(SchemaRepository repository, String tableName, String indexName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.getStore().getIndex(FnvHash.hashCode64(tableName + "." + indexName));
    }

    public Set<String> findIndexes(String schema, String table) {
        return findIndexes(schema, table, false);
    }

    public Set<String> findIndexes(String schema, String table, boolean includePrimary) {
        Set<String> result = new HashSet<>();

        Schema schemaRep = repository.findSchema(schema);
        if (schemaRep == null) {
            return result;
        }

        SchemaObject data = schemaRep.findTable(table);
        if (data == null) {
            return result;
        }

        SQLStatement statement = data.getStatement();
        if (statement == null) {
            return result;
        }

        if (statement instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement sqlCreateTableStatement = (SQLCreateTableStatement) statement;
            sqlCreateTableStatement.getTableElementList().forEach(e -> {
                if (e instanceof SQLConstraint && e instanceof SQLIndex) {
                    SQLConstraint sqlConstraint = (SQLConstraint) e;
                    if (sqlConstraint.getName() != null) {
                        result.add(SQLUtils.normalize(sqlConstraint.getName().getSimpleName()));
                    }
                }
                if (e instanceof SQLColumnDefinition) {
                    SQLColumnDefinition columnDefinition = (SQLColumnDefinition) e;
                    List<SQLColumnConstraint> constraints = columnDefinition.getConstraints();
                    if (constraints != null) {
                        for (SQLColumnConstraint constraint : constraints) {
                            if (constraint instanceof SQLColumnUniqueKey) {
                                result.add(SQLUtils.normalize(columnDefinition.getName().getSimpleName()));
                            }
                        }
                    }
                }
                if (e instanceof SQLPrimaryKey && includePrimary) {
                    result.add("primary");
                }

            });
        }

        Collection<SchemaObject> objects = schemaRep.getIndexes();
        if (objects != null) {
            objects.forEach(o -> {
                if (o.getStatement() instanceof SQLCreateIndexStatement) {
                    SQLCreateIndexStatement createIndexStatement = (SQLCreateIndexStatement) o.getStatement();
                    String indexTable = SQLUtils.normalize(createIndexStatement.getTableName());
                    if (StringUtils.equalsIgnoreCase(indexTable, table)) {
                        SQLName sqlName = createIndexStatement.getIndexDefinition().getName();
                        if (sqlName != null) {
                            result.add(SQLUtils.normalize(sqlName.getSimpleName()));
                        }
                    }
                }
            });
        }

        return result;
    }

    private SchemaObject findSequence(SchemaRepository repository, String sequenceName) {
        Schema schema = repository.findSchema("d`b1");
        return schema.getStore().getSequence(FnvHash.hashCode64(sequenceName));
    }

    @Test
    public void testSwapColumnName1() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs bigint, a bigint primary key, c int, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c c_dmrs int(11) default null, change column c_dmrs c bigint";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc bigint,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs int(11) DEFAULT NULL,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testSwapColumnName2() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs int, a bigint primary key, c bigint, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c_dmrs c int(11) default null, change column c c_dmrs bigint";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc int(11) DEFAULT NULL,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs bigint,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testSwapColumnName3() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create table t1 (c_dmrs bigint, a bigint primary key, c int, d int, e int)";
        repository.console(createTableSql);

        String alterTableSql =
            "alter table t1 change column c c_dmrs int(11) default null, change column c_dmrs c bigint, algorithm=inplace";
        repository.console(alterTableSql);

        SchemaObject schemaObject = repository.findTable("t1");
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        String newCreateTableSql = "CREATE TABLE t1 (\n"
            + "\tc bigint,\n"
            + "\ta bigint PRIMARY KEY,\n"
            + "\tc_dmrs int(11) DEFAULT NULL,\n"
            + "\td int,\n"
            + "\te int\n"
            + ")";
        Assert.assertEquals(newCreateTableSql, stmt1.toString());
    }

    @Test
    public void testMaterializedView() {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("t1");
        String createTableSql = "create materialized view mview as select id from xxx";
        repository.console(createTableSql);
        // com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement
        // com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement
    }

    @Test
    public void testModifyColumnSequence() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.console(
            "create table `test_compat_yha2_7ajh_00002` (\n"
                + "  a int,\n"
                + "  b double,\n"
                + "  c varchar(10),\n"
                + "  d bigint,\n"
                + "  _drds_implicit_id_ bigint auto_increment,\n"
                + "  primary key (_drds_implicit_id_)\n"
                + ")");
        memoryTableMeta.console("alter table `test_compat_yha2_7ajh_00002`\n"
            + "  modify column a int after b,\n"
            + "  drop column b,\n"
            + "  change column c b int,\n"
            + "  add column c int");

        String expectedDDL = "CREATE TABLE `test_compat_yha2_7ajh_00002` (\n"
            + "\tb int,\n"
            + "\ta int,\n"
            + "\td bigint,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_),\n"
            + "\tc int\n"
            + ")";
        SchemaObject tm1 = memoryTableMeta.findTable("test_compat_yha2_7ajh_00002");
        Assert.assertEquals(expectedDDL, tm1.getStatement().toString());
    }

    @Test
    public void testModifyColumnSequence2() {
        SchemaRepository memoryTableMeta = new SchemaRepository(JdbcConstants.MYSQL);
        memoryTableMeta.console(
            "create table `t25` (\n"
                + "  a int,\n"
                + "  b double,\n"
                + "  c varchar(10),\n"
                + "  _drds_implicit_id_ bigint auto_increment,\n"
                + "  d bigint,\n"
                + "  primary key (_drds_implicit_id_)\n"
                + ")");
        String expectedDDL = "CREATE TABLE `t25` (\n"
            + "\tc varchar(10),\n"
            + "\tb double,\n"
            + "\ta int,\n"
            + "\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n"
            + "\td bigint,\n"
            + "\tPRIMARY KEY (_drds_implicit_id_)\n"
            + ")";
        memoryTableMeta.console("alter table t25 modify column a int after c,modify column c varchar(10) first");
        SchemaObject tm1 = memoryTableMeta.findTable("t25");
        Assert.assertEquals(expectedDDL, tm1.getStatement().toString());
    }
}
