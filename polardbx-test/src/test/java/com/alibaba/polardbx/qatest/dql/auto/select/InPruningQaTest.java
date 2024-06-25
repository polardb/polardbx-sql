package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;

/**
 * @author fangwu
 */
public class InPruningQaTest extends BaseTestCase {
    private static final String AUTO_DB_NAME = InPruningQaTest.class.getSimpleName() + "_auto";
    private static final String SHARD_DB_NAME = InPruningQaTest.class.getSimpleName() + "_shard";

    private static final String PRUNING_INFO = "pruningInfo";
    private static final String PRUNING_INFO_PRE = "pruning detail:";
    private static int testInSize = 300;
    private static final Random r = new Random();

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + AUTO_DB_NAME);
            c.createStatement().execute("drop database if exists " + SHARD_DB_NAME);
            c.createStatement().execute("create database if not exists " + AUTO_DB_NAME + " mode=auto");
            c.createStatement().execute("create database if not exists " + SHARD_DB_NAME);
        }
        // set seed
        long seed = System.currentTimeMillis();
        r.setSeed(seed);
        System.out.println("random seed:" + seed);
    }

    @AfterClass
    public static void clean() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + AUTO_DB_NAME);
            c.createStatement().execute("drop database if exists " + SHARD_DB_NAME);
        }
    }

    @Test
    public void testShardSingleBroadcastWontPruning() throws Exception {
        try (Connection c = getPolardbxConnection(SHARD_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("CREATE TABLE single_tbl(\n"
                + " id bigint not null auto_increment, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ");");

            String sql = "select * from single_tbl where id in (1,2,3)";
            String explain = getExplainResult(c, sql);

            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);

            sql = "update single_tbl set name='a' where id in (1,2,3)";
            explain = getExplainResult(c, sql);

            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);

            sql = "delete single_tbl where id in (1,2,3)";
            explain = getExplainResult(c, sql);

            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);

            stmt.execute("CREATE TABLE brd_tbl(\n"
                + "  id bigint not null auto_increment, \n"
                + "  name varchar(30), \n"
                + "  primary key(id)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 BROADCAST;");

            explain = getExplainResult(c, "select * from brd_tbl where id in (1,2,3)");
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);

            explain = getExplainResult(c, "update brd_tbl set name='a' where id in (1,2,3)");
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);

            explain = getExplainResult(c, "delete from brd_tbl where id in (1,2,3)");
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO);
        }
    }

    @Test
    public void testShardPruning() throws Exception {
        try (Connection c = getPolardbxConnection(SHARD_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");

            // multi db by hash
            stmt.execute("CREATE TABLE multi_db_single_tbl(\n"
                + "  id bigint not null auto_increment, \n"
                + "  name varchar(30), \n"
                + "  bid bigint not null, \n"
                + "  primary key(id)\n"
                + ") dbpartition by hash(id);");

            String sql = "select * from multi_db_single_tbl where id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "update multi_db_single_tbl set name='a' where id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "delete from multi_db_single_tbl where id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "update multi_db_single_tbl set id=1234 where id %s and (id%%7)=1";
            testPruningCorrectness(c, sql, stmt);

            sql = "delete from multi_db_single_tbl where id %s and (id%%7)=1";
            testPruningCorrectness(c, sql, stmt);

            sql = "select * from multi_db_single_tbl where name in ('a', 'b', 'name') and id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "update multi_db_single_tbl set name='a' where name in ('a', 'b', 'name') and id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "delete from multi_db_single_tbl where name in ('a', 'b', 'name') and id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "select * from multi_db_single_tbl where (name in ('a', 'b', 'name') and bid>1) and id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "update multi_db_single_tbl set name='a' where (name in ('a', 'b', 'name') and bid>1) and id %s";
            testPruningCorrectness(c, sql, stmt);

            sql = "delete from multi_db_single_tbl where (name in ('a', 'b', 'name') and bid>1) and id %s";
            testPruningCorrectness(c, sql, stmt);

            // multi db multi tbl by hash
            stmt.execute("CREATE TABLE multi_db_multi_tbl(\n"
                + " id bigint not null auto_increment, \n"
                + " bid int, \n"
                + " name varchar(30), \n"
                + " primary key(id)\n"
                + ") dbpartition by hash(id) tbpartition by hash(bid) tbpartitions 3;");

            sql = "select * from multi_db_multi_tbl where id %s and bid %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});

            sql = "update multi_db_multi_tbl set name='a' where id %s and bid %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});

            sql = "delete from multi_db_multi_tbl where id %s and bid %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});

            // hash + week
            stmt.execute("CREATE TABLE user_log(\n"
                + " userId int, \n"
                + " name varchar(30), \n"
                + " operation varchar(30), \n"
                + " actionDate DATE\n"
                + ") dbpartition by hash(userId) tbpartition by WEEK(actionDate) tbpartitions 7;");

            sql = "select * from user_log where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "update user_log set name='a' where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "delete from user_log where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            // hash + mm
            stmt.execute("CREATE TABLE user_log2(\n"
                + " userId int, \n"
                + " name varchar(30), \n"
                + " operation varchar(30), \n"
                + " actionDate DATE\n"
                + ") dbpartition by hash(userId) tbpartition by MM(actionDate) tbpartitions 12; ");

            sql = "select * from user_log2 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "update user_log2 set name='a' where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "delete from user_log2 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            // hash + dd
            stmt.execute("CREATE TABLE user_log3(\n"
                + " userId int, \n"
                + " name varchar(30), \n"
                + " operation varchar(30), \n"
                + " actionDate DATE\n"
                + ") dbpartition by hash(userId) tbpartition by DD(actionDate) tbpartitions 31;");

            sql = "select * from user_log3 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "update user_log3 set name='a' where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "delete from user_log3 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            // hash + mmdd
            stmt.execute("CREATE TABLE user_log4(\n"
                + " userId int, \n"
                + " name varchar(30), \n"
                + " operation varchar(30), \n"
                + " actionDate DATE\n"
                + ") dbpartition by hash(userId) tbpartition by MMDD(actionDate) tbpartitions 64;");

            sql = "select * from user_log4 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "update user_log4 set name='a' where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "delete from user_log4 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            stmt.execute("CREATE TABLE user_log5(\n"
                + " userId int, \n"
                + " name varchar(30), \n"
                + " operation varchar(30), \n"
                + " actionDate DATE\n"
                + ") dbpartition by hash(userId) tbpartition by MMDD(actionDate) tbpartitions 10");

            sql = "select * from user_log5 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "update user_log5 set name='a' where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});

            sql = "delete from user_log5 where userId %s and actionDate %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "date"});
        }
    }

    @Test
    public void testAutoPruning() throws Exception {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("set PARTITION_PRUNING_STEP_COUNT_LIMIT=10000");

            stmt.execute("CREATE TABLE key_tbl_AutoPruning(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(name, id)\n"
                + "PARTITIONS 8;");

            String sql = "select * from key_tbl_AutoPruning where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            sql = "update key_tbl_AutoPruning set name='a' where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            sql = "delete from key_tbl_AutoPruning where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            stmt.execute("CREATE TABLE hash_tbl_AutoPruning(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "partition by hash(id)\n"
                + "partitions 8;");

            sql = "select * from hash_tbl_AutoPruning where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            sql = "update hash_tbl_AutoPruning set name='a' where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            sql = "delete from hash_tbl_AutoPruning where name %s and id %s";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"varchar", "int"});

            stmt.execute("CREATE TABLE hash_tbl_todays_AutoPruning(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY HASH(TO_DAYS(birthday))\n"
                + "PARTITIONS 8;");

            sql = "select * from hash_tbl_todays_AutoPruning where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "update hash_tbl_todays_AutoPruning set name='a' where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "delete from hash_tbl_todays_AutoPruning where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            stmt.execute("CREATE TABLE hash_tbl_todays1_AutoPruning(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY HASH(TO_DAYS(birthday))\n"
                + "PARTITIONS 8;");

            sql = "select * from hash_tbl_todays1_AutoPruning where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "update hash_tbl_todays1_AutoPruning set name='a' where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "delete from hash_tbl_todays1_AutoPruning where birthday %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            stmt.execute("CREATE TABLE orders_todays_AutoPruning(\n"
                + " id int,\n"
                + " order_time datetime not null)\n"
                + "PARTITION BY RANGE(to_days(order_time))\n"
                + "(\n"
                + "  PARTITION p1 VALUES LESS THAN (to_days('2021-01-01')),\n"
                + "  PARTITION p2 VALUES LESS THAN (to_days('2021-04-01')),\n"
                + "  PARTITION p3 VALUES LESS THAN (to_days('2021-07-01')),\n"
                + "  PARTITION p4 VALUES LESS THAN (to_days('2021-10-01')),\n"
                + "  PARTITION p5 VALUES LESS THAN (to_days('2022-01-01')),\n"
                + "  PARTITION p6 VALUES LESS THAN (MAXVALUE)\n"
                + ");");

            sql = "select * from orders_todays_AutoPruning where order_time %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "update orders_todays_AutoPruning set id=1 where order_time %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            sql = "delete from orders_todays_AutoPruning where order_time %s and id >100";
            testPruningCorrectness(c, sql, stmt, 1, new String[] {"date"});

            stmt.execute("CREATE TABLE t_orders_AutoPruning(\n"
                + " id bigint not null auto_increment, \n"
                + " order_id bigint, \n"
                + " buyer_id bigint,\n"
                + " order_time datetime not null,\n"
                + " primary key(id)\n"
                + ") \n"
                + "PARTITION BY CO_HASH(\n"
                + " \tRIGHT(`order_id`,6) /*取c1列的后6位字符*/,\n"
                + " \tRIGHT(`buyer_id`,6) /*取c2列的后6位字符*/\n"
                + ") \n"
                + "PARTITIONS 8;");

            sql = "select * from t_orders_AutoPruning where order_id %s and buyer_id %s and id >100";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});

            sql = "update t_orders_AutoPruning set buyer_id=11 where order_id %s and buyer_id %s and id >100";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});

            sql = "delete from t_orders_AutoPruning where order_id %s and buyer_id %s and id >100";
            testPruningCorrectness(c, sql, stmt, 2, new String[] {"int", "int"});
        }
    }

    @Test
    public void testInOr() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("CREATE TABLE IF NOT EXISTS key_tbl(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(name, id)\n"
                + "PARTITIONS 8;");
            String sql = "select * from key_tbl where name in ('a', 'b', 'c') or id in (1,2,3)";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl where name in ('a', 'b', 'c') and id in (1,2,3)";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            assert explain.contains("PruneRaw('a')");
            assert explain.contains("PruneRaw('b')");
            assert explain.contains("PruneRaw('c')");
            assert explain.contains("NonPruneRaw(1,2,3)");
        }
    }

    @Test
    public void testInNot() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("CREATE TABLE key_tbl_InNot(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(name, id)\n"
                + "PARTITIONS 8;");
            String sql = "select * from key_tbl_InNot where !name in ('a', 'b', 'c')";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl_InNot where !(name in ('a', 'b', 'c') or id in (1,2,3))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl_InNot where !(name in ('a', 'b', 'c') and id in (1,2,3))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);
        }
    }

    @Test
    public void testInFunction() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("CREATE TABLE IF NOT EXISTS key_tbl_InFunction(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY KEY(id)\n"
                + "PARTITIONS 8;");
            String sql = "select * from key_tbl_InFunction where id in (1,2,3,4,6,7)";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl_InFunction where (id+1) in (1,2,3,4,6,7)";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);
        }
    }

    @Test
    public void testInMultiCols() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("CREATE TABLE IF NOT EXISTS key_tbl_MultiCols(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY hash(id, name)\n"
                + "PARTITIONS 8;");
            String sql = "select * from key_tbl_MultiCols where id in (1,2,3,4,6,7) and name in ('b', 'c', 'd')";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl_MultiCols where (id, name) in ((1, 'c'),(2,'3'),(4,'d'))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            int endIndex = explain.indexOf("HitCache") - 3;
            int startIndex = explain.indexOf(PRUNING_INFO_PRE);
            Map<String, List<String[]>> check1 = decodePruningInfo(explain.substring(startIndex, endIndex));

            sql = "select * from key_tbl_MultiCols where (id,bid,name) in ((1, 5, 'c'),(2,2,'3'),(4,99,'d'))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            endIndex = explain.indexOf("HitCache") - 3;
            startIndex = explain.indexOf(PRUNING_INFO_PRE);
            Map<String, List<String[]>> check2 = decodePruningInfo(explain.substring(startIndex, endIndex));

            for (Map.Entry<String, List<String[]>> entry : check1.entrySet()) {
                List<String[]> valuesList = entry.getValue();
                List<String[]> valuesList1 = check2.get(entry.getKey());
                for (int i = 0; i < valuesList.size(); i++) {
                    String[] arr = valuesList.get(i);
                    String[] arr1 = valuesList1.get(i);
                    assert arr.length == 2;
                    assert arr1.length == 3;
                    assert arr[0].equals(arr1[0]);
                    assert arr[1].equals(arr1[2]);
                }
            }

            sql = "select * from key_tbl_MultiCols where (id,bid,name) in ((1, 5, 'c'),(2,2,'3'),(4,99,'d')) and"
                + " (name,birthday) in (('a', '2023-01-01'), ('b', '2023-01-02'))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);

            sql = "select * from key_tbl_MultiCols where (id,bid,name) in ((1, 5, 'c'),(2,2,'3'),(4,99,'d')) and"
                + " (name,birthday) in (('c', '2023-01-01'), ('3', '2023-01-02'))";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
        }
    }

    @Test
    public void testInMultiTbls() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");
            stmt.execute("CREATE TABLE IF NOT EXISTS key_tbl_MultiTbls1(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY key(id)\n"
                + "PARTITIONS 8;");

            stmt.execute("CREATE TABLE IF NOT EXISTS key_tbl_MultiTbls2(\n"
                + " id bigint not null auto_increment,\n"
                + " bid int,\n"
                + " name varchar(30),\n"
                + " birthday datetime not null,\n"
                + " primary key(id)\n"
                + ")\n"
                + "PARTITION BY key(id)\n"
                + "PARTITIONS 8;");

            String sql = "select * from key_tbl_MultiTbls1 a join key_tbl_MultiTbls2 b on a.id = b.id"
                + " where a.id in (1,2,3,4,6,7,8,9,10,11,12)";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            assert explain.contains("pruning size:77");

            sql = "select * from key_tbl_MultiTbls1 a join key_tbl_MultiTbls2 b on a.id = b.id"
                + " where b.id in (1,2,3,4,6,7,8,9,10,11,12)";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            assert explain.contains("pruning size:77");

            sql = "select * from key_tbl_MultiTbls1 a join key_tbl_MultiTbls2 b on a.id = b.id"
                + " where a.id in (1,2,3,4,6,7,10,12) and b.id in (7,8,9,10,11,12) ";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            assert explain.contains("pruning size:22");

            sql = "select * from key_tbl_MultiTbls1 a join key_tbl_MultiTbls2 b on a.id = b.id"
                + " where a.id in (1,2,3,4,6,7,10,12) and (b.id = 7 or b.id=10) ";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
            assert explain.contains("pruning size:14");
        }
    }

    @Test
    public void testInGsi() throws SQLException {
        try (Connection c = getPolardbxConnection(AUTO_DB_NAME)) {
            Statement stmt = c.createStatement();
            stmt.execute("set EXPLAIN_PRUNING_DETAIL=true");
            stmt.execute("set IN_SUB_QUERY_THRESHOLD=10000");

            stmt.execute("CREATE PARTITION TABLE `auto_part_tbl_gsi` (\n"
                + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "    `bid` int(11) DEFAULT NULL,\n"
                + "    `name` varchar(30) DEFAULT NULL,\n"
                + "    PRIMARY KEY (`id`),\n"
                + "    GLOBAL INDEX /* idx_name_$a870 */ `idx_name` (bid, `name`) PARTITION BY KEY (bid) PARTITIONS 16,\n"
                + "    LOCAL KEY `_local_idx_name` (`name`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
                + "PARTITION BY KEY(`id`)\n"
                + "PARTITIONS 16");

            String sql = "select * from auto_part_tbl_gsi force index(primary) where bid in (1,2,3,4,6,7,8,9,10,11,12)";
            String explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert !explain.contains(PRUNING_INFO_PRE);

            sql = "select * from auto_part_tbl_gsi force index(idx_name) where bid in (1,2,3,4,6,7,8,9,10,11,12)";
            explain = getExplainResult(c, sql);
            System.out.println(explain);
            assert explain.contains(PRUNING_INFO_PRE);
        }
    }

    @Test
    public void testDecode() {
        String pruningInfo =
            "pruning detail:(TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00003])->(PruneRaw(4));"
                + "(TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00005])->(PruneRaw(5,8));"
                + "(TEST_AUTO_P00000_GROUP, [hash_tbl_tQlx_00004])->(PruneRaw(6));"
                + "(TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00001])->(PruneRaw(7))";
        Map<String, List<String[]>> map = decodePruningInfo(pruningInfo);
        String[] arr = map.get("test_auto_p00001_group.hash_tbl_tqlx_00003").get(0);
        assert arr.length == 1 && arr[0].equals("4");

        arr = map.get("test_auto_p00001_group.hash_tbl_tqlx_00005").get(0);
        assert arr.length == 2 && arr[0].equals("5") && arr[1].equals("8");

        arr = map.get("test_auto_p00000_group.hash_tbl_tqlx_00004").get(0);
        assert arr.length == 1 && arr[0].equals("6");

        arr = map.get("test_auto_p00001_group.hash_tbl_tqlx_00001").get(0);
        assert arr.length == 1 && arr[0].equals("7");
    }

    private void testPruningCorrectness(Connection c, String sql, Statement stmt) throws SQLException {
        testPruningCorrectness(c, sql, stmt, 1, new String[] {"int", "int"});
    }

    /**
     * build sql params by type and set them into to sql.
     * decode explain result and get pruning info
     * check pruning info by explain sharding each in value and check the sharding group and physical table
     *
     * @param c test connection
     * @param sql test sql
     * @param stmt test statement
     * @param inSize in size
     * @param types in params types
     */
    private void testPruningCorrectness(Connection c, String sql, Statement stmt, int inSize, String[] types)
        throws SQLException {
        String[] inParams = new String[inSize];
        for (int i = 0; i < inSize; i++) {
            if (types[i].equals("int")) {
                List<Integer> inList = randomIntList(testInSize);
                StringJoiner sj = new StringJoiner(", ");
                inList.forEach(j -> sj.add(String.valueOf(j)));
                inParams[i] = "in (" + sj + ")";
            } else if (types[i].equals("date")) {
                List<LocalDate> dates = Lists.newArrayList();
                for (int j = 0; j < testInSize; j++) {
                    dates.add(randomDate());
                }
                StringJoiner sj = new StringJoiner("','");
                dates.forEach(j -> sj.add(j.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))));
                inParams[i] = "in ('" + sj + "')";
            } else if (types[i].equals("varchar")) {
                List<String> strings = Lists.newArrayList();
                for (int j = 0; j < testInSize; j++) {
                    strings.add(randomString(10));
                }
                StringJoiner sj = new StringJoiner("','");
                strings.forEach(j -> sj.add(j));
                inParams[i] = "in ('" + sj + "')";
            } else {
                throw new RuntimeException("unknown type: " + types[i]);
            }
        }

        String explain = getExplainResult(c, String.format(sql, inParams));
        int endIndex = explain.indexOf("HitCache") - 3;
        int startIndex = explain.indexOf(PRUNING_INFO_PRE);
        Map<String, List<String[]>> explainMap = decodePruningInfo(explain.substring(startIndex, endIndex));

        for (Map.Entry<String, List<String[]>> entry : explainMap.entrySet()) {
            // check each shard
            String groupTbl = entry.getKey();
            List<String[]> valuesList = entry.getValue();
            int checkSize = 1;
            for (String[] arr : valuesList) {
                checkSize *= arr.length;
            }

            int finalCheckSize = checkSize;
            int finalCheckSize1 = checkSize;
            Iterator<int[]> it = new Iterator<int[]>() {
                int cur = 0;

                @Override
                public boolean hasNext() {
                    return cur >= finalCheckSize;
                }

                @Override
                public int[] next() {
                    if (!hasNext()) {
                        throw new IllegalStateException("No more elements in the iterator.");
                    }
                    int[] rs = new int[valuesList.size()];
                    int totalSize = finalCheckSize1;
                    int currentIndex = cur;
                    for (int dim = 0; dim < valuesList.size(); dim++) {
                        String[] args = valuesList.get(dim);
                        int rawStringSize = args.length;
                        int otherSize = totalSize / rawStringSize;
                        int curIndex = currentIndex / otherSize;
                        currentIndex = currentIndex % otherSize;
                        totalSize = otherSize;
                        rs[dim] = curIndex;
                    }
                    cur++;
                    return rs;
                }
            };
            while (it.hasNext()) {
                int[] next = it.next();
                String[] vals = new String[next.length];
                for (int i = 0; i < next.length; i++) {
                    vals[i] = "=" + valuesList.get(i)[next[i]];
                }
                ResultSet resultSet = stmt.executeQuery("explain sharding " + String.format(sql, vals));
                resultSet.next();
                assert resultSet.getString("SHARDING").equalsIgnoreCase(groupTbl);
                resultSet.close();
            }

        }
    }

    private LocalDate randomDate() {
        LocalDate startDate = LocalDate.of(2000, 1, 1);
        LocalDate endDate = LocalDate.of(2022, 12, 31);
        long days = startDate.until(endDate, ChronoUnit.DAYS);
        long randomDays = r.nextInt((int) days) + 1;
        LocalDate randomDate = startDate.plusDays(randomDays);
        return randomDate;
    }

    private List<Integer> randomIntList(int inSize) {
        List<Integer> integers = Lists.newArrayList();
        for (int i = 0; i < inSize; i++) {
            integers.add(r.nextInt(10000));
        }
        return integers;
    }

    /**
     * pruning detail:
     * (TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00003])->(PruneRaw(4));
     * (TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00005])->(PruneRaw(5,8));
     * (TEST_AUTO_P00000_GROUP, [hash_tbl_tQlx_00004])->(PruneRaw(6));
     * (TEST_AUTO_P00001_GROUP, [hash_tbl_tQlx_00001])->(PruneRaw(7))
     */
    private static Map<String, List<String[]>> decodePruningInfo(String pruningInfo) {
        Map<String, List<String[]>> map = Maps.newHashMap();
        String detail =
            pruningInfo.substring(pruningInfo.indexOf(PRUNING_INFO_PRE) + PRUNING_INFO_PRE.length()).toLowerCase();
        for (String line : detail.split(";")) {
            String groupAndTbl = line.split("->")[0];
            String pruning = line.split("->")[1];

            groupAndTbl =
                groupAndTbl.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(" ", "").replaceAll("\\[", "")
                    .replaceAll("\\]", "").replaceAll(",", ".");
            pruning = pruning.substring(1, pruning.length() - 1);
            String[] pruningArr = pruning.split("pruneraw");
            List<String[]> pruningList = Lists.newArrayList();
            for (String p : pruningArr) {
                if (StringUtils.isEmpty(p)) {
                    continue;
                }
                p = p.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(" ", "").replaceAll("pruneraw", "");
                String[] arr = p.split(",");
                pruningList.add(arr);
            }

            map.put(groupAndTbl, pruningList);
        }
        return map;
    }

    private String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char ch = (char) (r.nextInt('x' - 'a') + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }
}
