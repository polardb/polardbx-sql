package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Test;

import java.util.List;

public class MySqlCreateTableTest164_local_partition extends MysqlTest {

    @Test
    public void test1(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '202101'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12\n"
            + "PIVOTDATE NOW();";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test2(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 3 YEAR\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test3(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 3 YEAR\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test4(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '202101'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12\n"
            + "PIVOTDATE DATA_ADD(NOW(), INTERVAL 1 DAY);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test5(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '202101'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12\n"
            + "PIVOTDATE DATA_SUB(NOW(), INTERVAL 1 DAY)\n"
            + "DISABLE SCHEDULE";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test6(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }

    @Test
    public void test7(){
        String sql = "CREATE TABLE t_order (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME NOT NULL comment \"aaa\"\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS(id) (\n"
            + "    PARTITION p0 VALUES LESS THAN (100000000),\n"
            + "    PARTITION p1 VALUES LESS THAN (200000000),\n"
            + "    PARTITION p2 VALUES LESS THAN (300000000),\n"
            + "    PARTITION p3 VALUES LESS THAN (400000000),\n"
            + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "DISABLE SCHEDULE";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement)statementList.get(0);

        System.out.println(stmt);
    }
}