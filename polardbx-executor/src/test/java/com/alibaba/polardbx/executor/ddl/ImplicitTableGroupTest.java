package com.alibaba.polardbx.executor.ddl;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-10-19 18:20
 **/
public class ImplicitTableGroupTest {

    @BeforeClass
    public static void beforeClass() {
        ImplicitTableGroupUtil.setTableGroupConfigProvider(new ImplicitTableGroupUtil.TableGroupConfigProvider() {

            @Override
            public boolean isNewPartitionDb(String schemaName) {
                return true;
            }

            @Override
            public boolean isCheckTgNameValue() {
                return true;
            }

            @Override
            public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, boolean fromDelta) {
                return buildTableGroupConfig(tableName);
            }

            @Override
            public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, String gsiName,
                                                        boolean fromDelta) {
                return buildTableGroupConfig(tableName);
            }

            @Override
            public ImplicitTableGroupUtil.PartitionColumnInfo getPartitionColumnInfo(String schemaName,
                                                                                     String tableName) {
                TableMeta tableMeta = new TableMeta(schemaName, tableName, Lists.newArrayList(), null, null,
                    true, TableStatus.PUBLIC, 1, 1);
                tableMeta.setGsiTableMetaBean(buildGsiTableMetaBean(schemaName, tableName));

                Map<String, Set<String>> gsiPartitionColumns = new HashMap<>();
                if (tableMeta.getGsiPublished() != null) {
                    for (GsiMetaManager.GsiIndexMetaBean i : tableMeta.getGsiPublished().values()) {
                        final String gsiName = StringUtils.substringBeforeLast(i.indexName, "_$");
                        Set<String> gsiColumns = i.indexColumns.stream()
                            .map(c -> c.columnName.toLowerCase())
                            .collect(Collectors.toSet());
                        gsiPartitionColumns.put(gsiName, gsiColumns);
                    }
                }

                return new ImplicitTableGroupUtil.PartitionColumnInfo(schemaName, tableName,
                    Sets.newHashSet("c1"), gsiPartitionColumns);
            }

            private GsiMetaManager.GsiTableMetaBean buildGsiTableMetaBean(String schemaName, String tableName) {
                HashMap<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();
                indexMap.put("gsi_1$abc",
                    buildGsiIndexMetaBean(schemaName, tableName, "gsi_1", Lists.newArrayList("c1")));
                indexMap.put("gsi_2$abc",
                    buildGsiIndexMetaBean(schemaName, tableName, "gsi_2", Lists.newArrayList("c1", "C2")));
                indexMap.put("gsi_3$abc",
                    buildGsiIndexMetaBean(schemaName, tableName, "gsi_3", Lists.newArrayList("c2", "C1")));

                GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = new GsiMetaManager.GsiTableMetaBean(null,
                    schemaName, tableName, GsiMetaManager.TableType.SHARDING, null, null, null,
                    null, null, null, indexMap, null, null);
                return gsiTableMetaBean;
            }

            private GsiMetaManager.GsiIndexMetaBean buildGsiIndexMetaBean(String schemaName, String tableName,
                                                                          String indexName, List<String> columns) {
                List<GsiMetaManager.GsiIndexColumnMetaBean> indexColumns = new ArrayList<>();
                for (String column : columns) {
                    indexColumns.add(buildIndexColumnMetaBean(column));
                }

                GsiMetaManager.GsiIndexMetaBean indexMetaBean =
                    new GsiMetaManager.GsiIndexMetaBean(null, schemaName, tableName, true, schemaName, indexName,
                        indexColumns, null, null, null, null, null, null,
                        IndexStatus.PUBLIC, 1, true, false, IndexVisibility.VISIBLE);
                return indexMetaBean;
            }

            private GsiMetaManager.GsiIndexColumnMetaBean buildIndexColumnMetaBean(String columnName) {
                GsiMetaManager.GsiIndexColumnMetaBean indexColumnMetaBean =
                    new GsiMetaManager.GsiIndexColumnMetaBean(1, columnName, null, 1, null, null, null, true);
                return indexColumnMetaBean;
            }

            private TableGroupConfig buildTableGroupConfig(String tableName) {
                TableGroupRecord tableGroupRecord = new TableGroupRecord();
                tableGroupRecord.tg_name = "tgi2";
                tableGroupRecord.manual_create = 0;
                TableGroupConfig tableGroupConfig = new TableGroupConfig();
                tableGroupConfig.setTableGroupRecord(tableGroupRecord);
                if (StringUtils.equalsIgnoreCase(tableName, "auto_partition_single_test")) {
                    tableGroupRecord.setTg_type(TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG);
                }
                return tableGroupConfig;
            }
        });
    }

    /**
     * 重点测试一些典型sql的toString方法，是否报错
     */
    @Test
    public void testParseToString() {
        String sql = "ALTER TABLE my_r_lc_ntp2\n"
            + "        DROP SUBPARTITION p1sp2 WITH TABLEGROUP=tg1363 IMPLICIT";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
    }

    @Test
    public void testCreateTableWithImplicitTableGroup() {
        // test toString for partition table
        String sql = "create table t1(a int) partition by key(a) partitions 3 with tablegroup='tgi2' implicit";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ")\n"
            + "PARTITION BY KEY (a) PARTITIONS 3\n"
            + "WITH TABLEGROUP = 'tgi2' IMPLICIT ", parseResult.get(0).toString());

        // test toString for single table
        sql = "create table t1(a int) single with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ") SINGLE\n"
            + "WITH TABLEGROUP = 'tgi2' IMPLICIT ", parseResult.get(0).toString());

        // test toString for broadcast table
        sql = "create table t1(a int) broadcast with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ") BROADCAST\n"
            + "WITH TABLEGROUP = 'tgi2' IMPLICIT ", parseResult.get(0).toString());

        // test edit statement with explict partition syntax
        sql = "create table t1(a int) partition by key(a) partitions 3";
        String expectSql = "CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ")\n"
            + "PARTITION BY KEY (a) PARTITIONS 3\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) parseResult.get(0);
        createTableStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        createTableStatement.setWithImplicitTablegroup(true);
        Assert.assertEquals(expectSql, createTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // test edit statement with single table
        sql = "create table t1(a int) single";
        expectSql = "CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ") SINGLE\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createTableStatement = (MySqlCreateTableStatement) parseResult.get(0);
        createTableStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        createTableStatement.setWithImplicitTablegroup(true);
        Assert.assertEquals(expectSql, createTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // test edit statement with broadcast table
        sql = "create table t1(a int) broadcast";
        expectSql = "CREATE TABLE t1 (\n"
            + "\ta int\n"
            + ") BROADCAST\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createTableStatement = (MySqlCreateTableStatement) parseResult.get(0);
        createTableStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        createTableStatement.setWithImplicitTablegroup(true);
        Assert.assertEquals(expectSql, createTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // test edit statement with implicit partition syntax
        sql = "create table t1(a int primary key, b varchar(100))";
        expectSql = "CREATE TABLE t1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb varchar(100)\n"
            + ")\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createTableStatement = (MySqlCreateTableStatement) parseResult.get(0);
        createTableStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        createTableStatement.setWithImplicitTablegroup(true);
        Assert.assertEquals(expectSql, createTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // test single table with auto_partition = 0
        sql = "create table auto_partition_single_test(id int primary key, b varchar(100))";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("d1", "auto_partition_single_test", sql);
        expectSql = "CREATE TABLE auto_partition_single_test (\n"
            + "\tid int PRIMARY KEY,\n"
            + "\tb varchar(100)\n"
            + ") SINGLE\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ";
        Assert.assertEquals(expectSql, sql);
        ImplicitTableGroupUtil.checkSql("d1", "auto_partition_single_test", sql, Sets.newHashSet("tgi2"));
    }

    @Test
    public void testCreateGlobalIndex() {
        // TEST: toString() for explicit GSI (create global index)
        String sql = "create global index g1 on t1(a) partition by key(a) partitions 2 with tablegroup='tgi2' implicit";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT PARTITION BY KEY (a) PARTITIONS 2",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI (create unique global index)
        sql = "create unique global index g1 on t1(a) partition by key(a) partitions 2 with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE UNIQUE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT PARTITION BY KEY (a) PARTITIONS 2",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI (create global unique index)
        sql = "create global unique index g1 on t1(a) partition by key(a) partitions 2 with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE UNIQUE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT PARTITION BY KEY (a) PARTITIONS 2",
            parseResult.get(0).toString());

        // TEST: toString() for implicit GSI (create index)
        sql = "create index g1 on t1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI (create unique index)
        sql = "create unique index g1 on t1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE UNIQUE INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI (create unique key)
        sql = "create unique key g1 on t1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(
            "CREATE UNIQUE INDEX g1 ON t1 (a) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST：edit for explicit GSI( create global index)
        sql = "create global index g1 on t1(a) partition by key(a) partitions 2";
        String expectSql =
            "CREATE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT PARTITION BY KEY (a) PARTITIONS 2";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        SQLCreateIndexStatement createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST：edit for explicit GSI( create global unique index)
        sql = "create global unique index g1 on t1(a) partition by key(a) partitions 2";
        expectSql =
            "CREATE UNIQUE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT PARTITION BY KEY (a) PARTITIONS 2";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST：edit for explicit GSI( create unique global index)
        sql = "create unique global index g1 on t1(a) partition by key(a) partitions 2";
        expectSql =
            "CREATE UNIQUE GLOBAL INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT PARTITION BY KEY (a) PARTITIONS 2";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST：edit for explicit GSI( create unique global index)
        sql = "create index g1 on t1(a)";
        expectSql = "CREATE INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST：edit for explicit GSI( create unique global index)
        sql = "create unique index g1 on t1(a)";
        expectSql = "CREATE UNIQUE INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST：edit for explicit GSI( create unique global index)
        sql = "create unique key g1 on t1(a)";
        expectSql = "CREATE UNIQUE INDEX g1 ON t1 (a) WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        createIndexStatement = (SQLCreateIndexStatement) parseResult.get(0);
        createIndexStatement.setWithImplicitTablegroup(true);
        createIndexStatement.setTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, createIndexStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);
    }

    @Test
    public void testAlterTableAddGsi() {

        // TEST: toString() for explicit GSI( add global index)
        String sql = "alter table t1 add global index g1(a) partition by key(a) partitions 2 "
            + "with tablegroup='tgi2' implicit";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddIndex.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI( add global unique index)
        sql = "alter table t1 add global unique index g1(a) partition by key(a) partitions 2 "
            + "with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddIndex.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD UNIQUE GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for explicit GSI( add unique global index)
        sql = "alter table t1 add unique global index g1(a) partition by key(a) partitions 2 "
            + "with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddConstraint.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD UNIQUE GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for implicit GSI(add index)
        sql = "alter table t1 add index idx1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddIndex.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD INDEX idx1 (a) WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for implicit GSI(add unique index)
        sql = "alter table t1 add unique index idx1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddConstraint.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD UNIQUE INDEX idx1 (a) WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for implicit GSI(add key)
        sql = "alter table t1 add key idx1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddIndex.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD KEY idx1 (a) WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: toString() for implicit GSI(add unique key)
        sql = "alter table t1 add unique key idx1(a) with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals(SQLAlterTableAddConstraint.class,
            ((SQLAlterTableStatement) parseResult.get(0)).getItems().get(0).getClass());
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tADD UNIQUE KEY idx1 (a) WITH TABLEGROUP= 'tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST: edit for explicit GSI( add global index)
        sql = "alter table t1 add global index g1(a) partition by key(a) partitions 2";
        String expectSql = "ALTER TABLE t1\n"
            + "\tADD GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for explicit GSI( add global unique index)
        sql = "alter table t1 add global unique index g1(a) partition by key(a) partitions 2";
        expectSql = "ALTER TABLE t1\n"
            + "\tADD UNIQUE GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for explicit GSI( add global unique index)
        sql = "alter table t1 add unique global index g1(a) partition by key(a) partitions 2";
        expectSql = "ALTER TABLE t1\n"
            + "\tADD UNIQUE GLOBAL INDEX g1 (a) PARTITION BY KEY (a) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for implicit GSI( add key)
        sql = "alter table t1 add key k1(a)";
        expectSql = "ALTER TABLE t1\n" + "\tADD KEY k1 (a) WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for implicit GSI( add unique key)
        sql = "alter table t1 add unique key k1(a)";
        expectSql = "ALTER TABLE t1\n" + "\tADD UNIQUE KEY k1 (a) WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for implicit GSI( add index)
        sql = "alter table t1 add index idx1(a)";
        expectSql = "ALTER TABLE t1\n" + "\tADD INDEX idx1 (a) WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST: edit for implicit GSI( add unique index)
        sql = "alter table t1 add unique index idx2(a)";
        expectSql = "ALTER TABLE t1\n" + "\tADD UNIQUE INDEX idx2 (a) WITH TABLEGROUP= tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        processImplicitTableGroup4Index((SQLAlterTableStatement) parseResult.get(0), "tgi2");
        Assert.assertEquals(expectSql, parseResult.get(0).toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);
    }

    @Test
    public void createTableWithGsi() {
        // TEST toString() for explict GSI definition in create table
        String sql = "create table t1("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10),"
            + " global index (b) partition by key(b) partitions 2 with tablegroup='tgi2' implicit, "
            + " unique global index g2(c) partition by key(c) partitions 2 with tablegroup='tgi2' implicit, "
            + " unique global key g3(c) partition by key(c) partitions 2 with tablegroup='tgi2' implicit,"
            + " global unique index g4(c) partition by key(c) partitions 2 with tablegroup='tgi2' implicit,"
            + " global unique key g5(c) partition by key(c) partitions 2 with tablegroup='tgi2' implicit)"
            + " partition by key(a) partitions 3 with tablegroup='tgi1' implicit";

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int,\n"
            + "\tc int,\n"
            + "\td varchar(10),\n"
            + "\tGLOBAL INDEX b(b) PARTITION BY KEY (b) PARTITIONS 2 WITH TABLEGROUP='tgi2' IMPLICIT,\n"
            + "\tUNIQUE GLOBAL INDEX g2 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT,\n"
            + "\tUNIQUE GLOBAL KEY g3 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT,\n"
            + "\tUNIQUE GLOBAL INDEX g4 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT,\n"
            + "\tUNIQUE GLOBAL KEY g5 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= 'tgi2' IMPLICIT\n"
            + ")\n"
            + "PARTITION BY KEY (a) PARTITIONS 3\n"
            + "WITH TABLEGROUP = 'tgi1' IMPLICIT ", parseResult.get(0).toString());

        // TEST toString() for implicit GSI definition in create table
        sql = "create table t1("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10) unique,"
            + " index g1(b) with tablegroup='tgi2' implicit,"
            + " key g2(b) with tablegroup='tgi2' implicit, "
            + " unique key g3(b) with tablegroup='tgi2' implicit, "
            + " unique index g4(b) with tablegroup='tgi2' implicit) with tablegroup='tgi1' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int,\n"
            + "\tc int,\n"
            + "\td varchar(10) UNIQUE,\n"
            + "\tINDEX g1(b) WITH TABLEGROUP='tgi2' IMPLICIT,\n"
            + "\tKEY g2 (b) WITH TABLEGROUP='tgi2' IMPLICIT,\n"
            + "\tUNIQUE KEY g3 (b) WITH TABLEGROUP= 'tgi2' IMPLICIT,\n"
            + "\tUNIQUE INDEX g4 (b) WITH TABLEGROUP= 'tgi2' IMPLICIT\n"
            + ")\n"
            + "WITH TABLEGROUP = 'tgi1' IMPLICIT ", parseResult.get(0).toString());

        // TEST edit for explicit GSI definition in create table
        sql = "create table t1("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10),"
            + " global index (b) partition by key(b) partitions 2, "
            + " unique global index g2(c) partition by key(c) partitions 2, "
            + " unique global key g3(c) partition by key(c) partitions 2,"
            + " global unique index g4(c) partition by key(c) partitions 2,"
            + " global unique key g5(c) partition by key(c) partitions 2)"
            + " partition by key(a) partitions 3";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int,\n"
            + "\tc int,\n"
            + "\td varchar(10),\n"
            + "\tGLOBAL INDEX b(b) PARTITION BY KEY (b) PARTITIONS 2 WITH TABLEGROUP=tgi2 IMPLICIT,\n"
            + "\tUNIQUE GLOBAL INDEX g2 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT,\n"
            + "\tUNIQUE GLOBAL KEY g3 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT,\n"
            + "\tUNIQUE GLOBAL INDEX g4 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT,\n"
            + "\tUNIQUE GLOBAL KEY g5 (c) PARTITION BY KEY (c) PARTITIONS 2 WITH TABLEGROUP= tgi2 IMPLICIT\n"
            + ")\n"
            + "PARTITION BY KEY (a) PARTITIONS 3\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ", sql);

        // TEST edit for implicit GSI definition in create table
        sql = "create table t1("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10) unique,"
            + " index g1(b),"
            + " key g2(b), "
            + " unique key g3(b), "
            + " unique index g4(b))";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("CREATE TABLE t1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int,\n"
            + "\tc int,\n"
            + "\td varchar(10) UNIQUE,\n"
            + "\tINDEX g1(b) WITH TABLEGROUP=tgi2 IMPLICIT,\n"
            + "\tKEY g2 (b) WITH TABLEGROUP=tgi2 IMPLICIT,\n"
            + "\tUNIQUE KEY g3 (b) WITH TABLEGROUP= tgi2 IMPLICIT,\n"
            + "\tUNIQUE INDEX g4 (b) WITH TABLEGROUP= tgi2 IMPLICIT\n"
            + ")\n"
            + "WITH TABLEGROUP = tgi2 IMPLICIT ", sql);
    }

    @Test
    public void testAlterTableWithPartition() {
        // TEST toString() for reorg partition
        String sql = "/* comments */alter table t1 split partition p1 with tablegroup='tgi2' implicit";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER TABLE t1\n"
            + "\tSPLIT PARTITION p1  WITH TABLEGROUP='tgi2' IMPLICIT", parseResult.get(0).toString());

        // TEST toString for single
        sql = "alter table t1 single with tablegroup=tgi2 implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER TABLE t1\n"
            + "\tSINGLE WITH TABLEGROUP=tgi2 IMPLICIT", parseResult.get(0).toString());

        // Test toString for broadcast
        sql = "alter table t1 broadcast with tablegroup=tgi2 implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER TABLE t1\n"
            + "\tBROADCAST WITH TABLEGROUP=tgi2 IMPLICIT", parseResult.get(0).toString());

        // TEST edit for reorg partition
        sql = "alter table t1 split partition p1";
        String expectSql = "ALTER TABLE t1\n"
            + "\tSPLIT PARTITION p1  WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) parseResult.get(0);
        alterTableStatement.setTargetImplicitTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, alterTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST edit for single
        sql = "alter table t1 single";
        expectSql = "ALTER TABLE t1\n"
            + "\tSINGLE WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        alterTableStatement = (SQLAlterTableStatement) parseResult.get(0);
        alterTableStatement.setTargetImplicitTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, alterTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);

        // TEST edit for broadcast
        sql = "alter table t1 broadcast";
        expectSql = "ALTER TABLE t1\n"
            + "\tBROADCAST WITH TABLEGROUP=tgi2 IMPLICIT";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        alterTableStatement = (SQLAlterTableStatement) parseResult.get(0);
        alterTableStatement.setTargetImplicitTableGroup(new SQLIdentifierExpr("tgi2"));
        Assert.assertEquals(expectSql, alterTableStatement.toString());

        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals(expectSql, sql);
    }

    @Test
    public void testAlterIndex() {
        // TEST toString for alter index
        String sql = "/*# add **/alter index gsi_lc on table lc_rc_tp1"
            + " add partition ( partition p2 values in ((11,11),(10,10)) ) with tablegroup='tgi2' implicit";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc ON TABLE lc_rc_tp1\n"
                + "\tADD PARTITION (PARTITION p2 VALUES IN ((11, 11), (10, 10))) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST toString for alter index
        sql = "alter index gsi_lc_rc on table lc_rc_tp1 modify partition p1 add values ( (15,15) )"
            + " with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tMODIFY PARTITION p1 ADD VALUES ((15, 15)) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST toString for alter index
        sql = "/*# split **/alter index gsi_lc_rc on table lc_rc_tp1 "
            + "split subpartition sp1 into ( "
            + "subpartition sp1 values less than ('2022-01-01','a'), "
            + "subpartition sp2 values less than (maxvalue, maxvalue) ) "
            + "with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tSPLIT SUBPARTITION sp1 INTO (SUBPARTITION sp1 VALUES LESS THAN ('2022-01-01', 'a'), SUBPARTITION sp2 VALUES LESS THAN (maxvalue, maxvalue)) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST toString for alter index
        sql = "alter index gsi_lc_rc on table lc_rc_tp1 merge subpartitions sp1,sp2 to sp1"
            + " with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
            + "\tMERGE SUBPARTITIONS sp1, sp2 TO sp1 WITH TABLEGROUP='tgi2' IMPLICIT", parseResult.get(0).toString());

        // TEST toString for alter index
        sql = "/*# reorg*/alter index gsi_lc_rc on table lc_rc_tp1 "
            + "reorganize subpartition sp0,sp1 into ("
            + "subpartition sp4 values less than ('2021-01-01','a'), "
            + "subpartition sp5 values less than ('2028-01-01','a'), "
            + "subpartition sp3 values less than (maxvalue, maxvalue) ) "
            + "with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tREORGANIZE SUBPARTITION sp0, sp1 INTO (SUBPARTITION sp4 VALUES LESS THAN ('2021-01-01', 'a'), SUBPARTITION sp5 VALUES LESS THAN ('2028-01-01', 'a'), SUBPARTITION sp3 VALUES LESS THAN (maxvalue, maxvalue)) WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST toString for alter index
        sql = "/*# rename*/alter index gsi_lc_rc on table lc_rc_tp1 rename subpartition sp4 to sp0, sp3 to sp1"
            + " with tablegroup='tgi2' implicit";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        parseResult = parser.parseStatementList();
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tRENAME SUBPARTITION sp4 TO sp0, sp3 TO sp1 WITH TABLEGROUP='tgi2' IMPLICIT",
            parseResult.get(0).toString());

        // TEST edit for alter index
        sql = "/*# add **/alter index gsi_lc on table lc_rc_tp1 "
            + "add partition ( partition p2 values in ((11,11),(10,10)) )";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc ON TABLE lc_rc_tp1\n"
            + "\tADD PARTITION (PARTITION p2 VALUES IN ((11, 11), (10, 10))) WITH TABLEGROUP=tgi2 IMPLICIT", sql);

        // TEST edit for alter index
        sql = "alter index gsi_lc_rc on table lc_rc_tp1 modify partition p1 add values ( (15,15) )";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
            + "\tMODIFY PARTITION p1 ADD VALUES ((15, 15)) WITH TABLEGROUP=tgi2 IMPLICIT", sql);

        // TEST edit for alter index
        sql = "/*# split **/alter index gsi_lc_rc on table lc_rc_tp1 split subpartition sp1 into ( "
            + "     subpartition sp1 values less than ('2022-01-01','a'), "
            + "     subpartition sp2 values less than (maxvalue, maxvalue) "
            + ")";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tSPLIT SUBPARTITION sp1 INTO (SUBPARTITION sp1 VALUES LESS THAN ('2022-01-01', 'a'), SUBPARTITION sp2 VALUES LESS THAN (maxvalue, maxvalue)) WITH TABLEGROUP=tgi2 IMPLICIT",
            sql);

        // TEST edit for alter index
        sql = "alter index gsi_lc_rc on table lc_rc_tp1 merge subpartitions sp1,sp2 to sp1";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
            + "\tMERGE SUBPARTITIONS sp1, sp2 TO sp1 WITH TABLEGROUP=tgi2 IMPLICIT", sql);

        // TEST edit for alter index
        sql = "/*# reorg*/alter index gsi_lc_rc on table lc_rc_tp1 reorganize subpartition sp0,sp1 into ( "
            + "     subpartition sp4 values less than ('2021-01-01','a'), "
            + "     subpartition sp5 values less than ('2028-01-01','a'), "
            + "     subpartition sp3 values less than (maxvalue, maxvalue) "
            + ")";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                + "\tREORGANIZE SUBPARTITION sp0, sp1 INTO (SUBPARTITION sp4 VALUES LESS THAN ('2021-01-01', 'a'), SUBPARTITION sp5 VALUES LESS THAN ('2028-01-01', 'a'), SUBPARTITION sp3 VALUES LESS THAN (maxvalue, maxvalue)) WITH TABLEGROUP=tgi2 IMPLICIT",
            sql);

        // TEST edit for alter index
        sql = "/*# rename*/alter index gsi_lc_rc on table lc_rc_tp1 "
            + "rename subpartition sp4 to sp0, sp3 to sp1";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "xx", sql);
        Assert.assertEquals("ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
            + "\tRENAME SUBPARTITION sp4 TO sp0, sp3 TO sp1 WITH TABLEGROUP=tgi2 IMPLICIT", sql);
    }

    @Test
    public void testAlterTableModifyColumn() {
        String sql = "alter table t1 modify column c1 bigint not null";
        sql = ImplicitTableGroupUtil.tryAttachImplicitTableGroup("xx", "t1", sql);
        Assert.assertEquals("ALTER TABLE t1\n"
                + "\tMODIFY COLUMN c1 bigint NOT NULL WITH TABLEGROUP=tgi2 IMPLICIT, INDEX gsi_3 WITH TABLEGROUP=tgi2 IMPLICIT, INDEX gsi_2 WITH TABLEGROUP=tgi2 IMPLICIT, INDEX gsi_1 WITH TABLEGROUP=tgi2 IMPLICIT",
            sql);
    }

    private void processImplicitTableGroup4Index(SQLAlterTableStatement alterTableStatement, String tableGroupName) {
        for (SQLAlterTableItem item : alterTableStatement.getItems()) {
            if (item instanceof SQLAlterTableAddIndex) {
                SQLAlterTableAddIndex alterTableAddIndex = (SQLAlterTableAddIndex) item;
                alterTableAddIndex.setTableGroup(new SQLIdentifierExpr(tableGroupName));
                alterTableAddIndex.setWithImplicitTablegroup(true);
            } else if (item instanceof SQLAlterTableAddConstraint) {
                SQLAlterTableAddConstraint alterTableAddConstraint = (SQLAlterTableAddConstraint) item;
                SQLConstraint sqlConstraint = alterTableAddConstraint.getConstraint();
                if (sqlConstraint instanceof MySqlUnique) {
                    MySqlUnique mySqlUnique = (MySqlUnique) sqlConstraint;
                    mySqlUnique.setTableGroup(new SQLIdentifierExpr(tableGroupName));
                    mySqlUnique.setWithImplicitTablegroup(true);
                }
            }
        }
    }
}
