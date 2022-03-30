package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.util.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author chenhui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionTablePartitioningEqualsTest extends BasePlannerTest {

    static final String dbName = "optest";

    protected PartitioningEqualsTestParameter testParameter;

    public PartitionTablePartitioningEqualsTest(PartitioningEqualsTestParameter testParameter) {
        super(dbName);
        this.testParameter = testParameter;
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    public static class PartitioningEqualsTestParameter {
        public Pair<Boolean, Pair<String, String>> createTablePair;
        public PartitioningEqualsTestParameter(Pair<Boolean, Pair<String, String>> createTablePair) {
            this.createTablePair = createTablePair;
        }

        @Override
        public String toString() {
            return String.format("t1:[%s],t2:[%s]" , createTablePair.getValue().getKey(), createTablePair.getValue().getValue());
        }
    }

    @Test
    public void testPartitionInfoEqualsByParams() throws SQLException {
        ec.setParams(new Parameters());
        ec.setSchemaName(appName);
        ec.setServerVariables(new HashMap<>());

        boolean expectResult = this.testParameter.createTablePair.getKey();
        String sql1 = this.testParameter.createTablePair.getValue().getKey();
        String sql2 = this.testParameter.createTablePair.getValue().getValue();

        final MySqlCreateTableStatement stat1 =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(sql1).get(0);
        final MySqlCreateTableStatement stat2 =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(sql2).get(0);
        final SqlCreateTable sqlCreateTable1 = (SqlCreateTable) FastsqlParser
            .convertStatementToSqlNode(stat1, null, ec);
        final TableMeta tm1 = new TableMetaParser().parse(sqlCreateTable1);
        buildLogicalCreateTable(dbName, tm1, sqlCreateTable1, stat1.getTableName(),
            PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));

        final SqlCreateTable sqlCreateTable2 = (SqlCreateTable) FastsqlParser
            .convertStatementToSqlNode(stat2, null, ec);
        final TableMeta tm2 = new TableMetaParser().parse(sqlCreateTable2);

        buildLogicalCreateTable(dbName, tm2, sqlCreateTable2, stat2.getTableName(),
            PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));
        PartitionInfo tm1PartInfo = tm1.getPartitionInfo();
        PartitionInfo tm2PartInfo = tm2.getPartitionInfo();
        boolean compRs = PartitionInfoUtil.partitionEquals(tm1PartInfo, tm2PartInfo);
        Assert.assertTrue(expectResult == compRs,
            "unexpected tableGroup for " + tm1.getTableName() + " <->" + tm2.getTableName());
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    @Parameterized.Parameters(name = "{index}: params {0}")
    public static List<PartitioningEqualsTestParameter> parameters() {
        List<PartitioningEqualsTestParameter> parameters = new ArrayList<>();
        for (int i = 0; i < createTablePairs.size(); i++) {
            PartitioningEqualsTestParameter p = new PartitioningEqualsTestParameter(createTablePairs.get(i));
            parameters.add(p);
        }
        return parameters;
    }

    static List<Pair<Boolean, Pair<String, String>>> createTablePairs = Arrays.asList(
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b varchar(32) CHARACTER SET latin1 collate latin1_bin ) partition by range columns(a,b) ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))",
                "create table t2(a int, b varchar(32) CHARACTER SET latin1 collate latin1_general_ci ) partition by range columns(a,b)  ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b varchar(32) CHARACTER SET latin1 ) partition by range columns(a,b) ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))",
                "create table t2(a int, b varchar(32) CHARACTER SET utf8 ) partition by range columns(a,b)  ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b varchar(32) ) partition by range columns(a,b) ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))",
                "create table t2(a int, b varchar(64) ) partition by range columns(a,b)  ( partition p1 values less than (1,'1'), partition p2 values less than (2,'2'),partition p3 values less than (3,'3'))"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,1), partition p2 values less than (2,2),partition p3 values less than (3,3))",
                "create table t2(a int, b int ) partition by range columns(a,b)  ( partition p1 values less than (1,2), partition p2 values less than (2,3),partition p3 values less than (3,3))"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,maxvalue), partition p2 values less than (2,maxvalue),partition p3 values less than (3,maxvalue))",
                "create table t2(a int,b datetime, c int , d varchar) partition by range columns(c,b,d)  ( partition p1 values less than (1,'2012-12-12', maxvalue), partition p2 values less than (2,'2012-12-13',maxvalue),partition p3 values less than (3,maxvalue,maxvalue))"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,maxvalue), partition p2 values less than (2,maxvalue),partition p3 values less than (3,maxvalue))",
                "create table t2(a int,b datetime, c int ) partition by range columns(c,b)  ( partition p1 values less than (1,maxvalue), partition p2 values less than (2,maxvalue),partition p3 values less than (3,maxvalue))"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,maxvalue), partition p2 values less than (2,maxvalue),partition p3 values less than (3,maxvalue))",
                "create table t2(a int,b datetime, c int ) partition by range columns(c)  ( partition p1 values less than (1), partition p2 values less than (2),partition p3 values less than (3))"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(b varchar(32), a int, d char) partition by key(b,a,d) partitions 3",
                "create table t2(a datetime, c varchar(64) ) partition by key(c,a) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b varchar, d char) partition by key(b,a,d) partitions 3",
                "create table t2(a datetime, c varchar ) partition by key(c,a) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b varchar) partition by key(a,b) partitions 3",
                "create table t2(a datetime, c int ) partition by key(c,a) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b int) partition by key(a) partitions 3",
                "create table t2(a int, c int) partition by key(c,a) partitions 3"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a int, b int) partition by hash(a) partitions 3",
                "create table t2(a int, c int) partition by hash(c,a) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a int, b int) partition by hash(a) partitions 3",
                "create table t2(a int, c int) partition by hash(c) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3",
                "create table t2(c datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3",
                "create table t2(c datetime, b varchar(20)) charset=utf8 partition by key(b) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
                "create table t2(c datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
                "create table t2(c datetime, b varchar(20)) charset=gbk collate=gbk_chinese_ci partition by key(b) partitions 3"))
        ,
        new Pair<>(Boolean.FALSE,
            new Pair<>(
                "create table t1(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
                "create table t2(c datetime, b varchar(24)) charset=gbk collate=gbk_bin partition by key(b) partitions 3"))
        ,
        new Pair<>(Boolean.TRUE,
            new Pair<>(
                "create table t1(a timestamp(6), b varchar(20)) charset=gbk collate=gbk_bin partition by key(a) partitions 3",
                "create table t2(c timestamp(6), b varchar(20)) charset=gbk collate=gbk_bin partition by key(c) partitions 3"))
    );

//    static List<Pair<Boolean, Pair<String, String>>> createTablePairs = Arrays.asList(
//        new Pair<>(Boolean.TRUE, new Pair<>("create table t1(a int, b int) partition by hash(a) partitions 3",
//            "create table t2(a int, c int) partition by hash(c) partitions 3")),
//        new Pair<>(Boolean.FALSE, new Pair<>("create table t3(a int, b int) partition by hash(a) partitions 3",
//            "create table t4(a int, c bigint) partition by hash(c) partitions 3")),
//        new Pair<>(Boolean.TRUE, new Pair<>("create table t5(a int, b int) partition by key(a) partitions 3",
//            "create table t6(a int, c int) partition by key(c) partitions 3")),
//        new Pair<>(Boolean.FALSE, new Pair<>("create table t7(a int, b int) partition by key(a) partitions 3",
//            "create table t8(a int, c varchar(2)) partition by key(c) partitions 3")),
//        new Pair<>(Boolean.TRUE, new Pair<>(
//            "create table t9(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
//            "create table t10(c datetime, b int) partition by range(year(c))(partition p1 values less than (10))")),
//        new Pair<>(Boolean.FALSE, new Pair<>(
//            "create table t11(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
//            "create table t12(c datetime, b int) partition by range(month(c))(partition p1 values less than (10))")),
//        new Pair<>(Boolean.FALSE, new Pair<>(
//            "create table t13(a datetime, b int) partition by range(year(a))(partition p1 values less than (10))",
//            "create table t14(a int, b int) partition by range(a)(partition p1 values less than (10))")),
//        new Pair<>(Boolean.FALSE, new Pair<>(
//            "create table t15(a datetime, b varchar(20)) charset=utf8 partition by key(b) partitions 3",
//            "create table t16(c datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3")),
//        new Pair<>(Boolean.FALSE, new Pair<>(
//            "create table t17(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
//            "create table t18(c datetime, b varchar(20)) charset=gbk collate=gbk_chinese_ci partition by key(b) partitions 3")),
//        new Pair<>(Boolean.TRUE, new Pair<>(
//            "create table t19(a datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3",
//            "create table t20(c datetime, b varchar(20)) charset=gbk collate=gbk_bin partition by key(b) partitions 3")),
//        new Pair<>(Boolean.TRUE, new Pair<>(
//            "create table t21(a datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3",
//            "create table t22(c datetime, b varchar(20)) charset=gbk partition by key(b) partitions 3")));
}

