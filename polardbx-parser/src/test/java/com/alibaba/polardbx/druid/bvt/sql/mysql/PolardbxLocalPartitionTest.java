package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @version 1.0
 * @ClassName PolardbxSchedulerTest
 * @description
 * @Author guxu
 */
public class PolardbxLocalPartitionTest extends MysqlTest {

    @Test
    public void test_create(){
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
    public void test_allocate_local_partition(){
        String sql = "ALTER TABLE t1 ALLOCATE LOCAL PARTITION";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        System.out.println(statementList.get(0));
    }

    @Test
    public void test_expire_local_partition(){
        String sql = "ALTER TABLE t1 EXPIRE LOCAL PARTITION";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        System.out.println(statementList.get(0));
    }

    @Test
    public void test_expire_local_partition_p1(){
        String sql = "ALTER TABLE t1 EXPIRE LOCAL PARTITION p1";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        System.out.println(statementList.get(0));
    }

    @Test
    public void test_alter_to_local_partition1(){
        String sql = "ALTER TABLE t1\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        System.out.println(statementList.get(0));
    }

    @Test
    public void test_alter_to_local_partition2(){
        String sql = "ALTER TABLE t1\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '202101'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 12\n"
            + "PIVOTDATE DATA_SUB(NOW(), INTERVAL 1 DAY);";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        System.out.println(statementList.get(0));
    }

}
