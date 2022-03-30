package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
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
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenhui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionTablePartitioningClassifyTest extends BasePlannerTest {

    static final String dbName = "optest";

    public static class PartitioningClassifyTestParameter {
        public Pair<String[][], String[]> createTablePair;
        public PartitioningClassifyTestParameter(Pair<String[][], String[]> createTablePair) {
            this.createTablePair = createTablePair;
        }

        @Override
        public String toString() {
            return String.format("%s", String.join(",", Arrays.asList(createTablePair.getValue())) );
        }
    }

    protected PartitioningClassifyTestParameter testParameter;
    public PartitionTablePartitioningClassifyTest(PartitioningClassifyTestParameter testParameter) {
        super(dbName);
        this.testParameter = testParameter;
    }

    @Parameterized.Parameters(name = "{index}: params {0}")
    public static List<PartitioningClassifyTestParameter> parameters() {
        List<PartitioningClassifyTestParameter> parameters = new ArrayList<>();
        for (int i = 0; i < createTablePairs.size(); i++) {
            PartitioningClassifyTestParameter p = new PartitioningClassifyTestParameter(createTablePairs.get(i));
            parameters.add(p);
        }
        return parameters;
    }


    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }


    @Test
    public void testPartitionInfoEqualsByParams() throws SQLException {
        ec.setParams(new Parameters());
        ec.setSchemaName(appName);
        ec.setServerVariables(new HashMap<>());

        String[][] expectGroupingRs = this.testParameter.createTablePair.getKey();
        String[] ddlCreateTblList = this.testParameter.createTablePair.getValue();

        List<PartitionInfo> partInfoList = new ArrayList<>();
        for (int i = 0; i < ddlCreateTblList.length; i++) {
            String ddlCreateTb = ddlCreateTblList[i];
            final MySqlCreateTableStatement stmt =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(ddlCreateTb).get(0);
            final SqlCreateTable sqlCreateTable = (SqlCreateTable) FastsqlParser
                .convertStatementToSqlNode(stmt, null, ec);
            final TableMeta tm = new TableMetaParser().parse(sqlCreateTable);
            buildLogicalCreateTable(dbName, tm, sqlCreateTable, stmt.getTableName(),
                PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));
            PartitionInfo partInfo = tm.getPartitionInfo();
            partInfoList.add(partInfo);
        }

        int nPrefixPartColCnt = 1;

        Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new TreeMap<>();
        Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, nPrefixPartColCnt, outputPartInfoGrouping, outputTblNameToGrpIdMap);

        Assert.assertTrue(outputPartInfoGrouping.size() == expectGroupingRs.length);
        for ( Map.Entry<Long, List<PartitionInfo>> grpInfoItem : outputPartInfoGrouping.entrySet() ) {
            Long grpId = grpInfoItem.getKey();
            List<PartitionInfo> partInfos = grpInfoItem.getValue();

            Assert.assertTrue(partInfos.size() == expectGroupingRs[grpId.intValue() - 1].length);
            for (int j = 0; j < partInfos.size(); j++) {
                Assert.assertTrue(partInfos.get(j).getTableName().equalsIgnoreCase(expectGroupingRs[grpId.intValue() - 1][j]));
            }
        }
    }

    //@Test
    public void testPartitionInfoEquals() {
        for (Pair<String[][], String[]> createTablePair : createTablePairs) {
            ec.setParams(new Parameters());
            ec.setSchemaName(appName);
            ec.setServerVariables(new HashMap<>());

            String[][] expectGroupingRs = createTablePair.getKey();
            String[] ddlCreateTblList = createTablePair.getValue();

            List<PartitionInfo> partInfoList = new ArrayList<>();
            for (int i = 0; i < ddlCreateTblList.length; i++) {
                String ddlCreateTb = ddlCreateTblList[i];
                final MySqlCreateTableStatement stmt =
                    (MySqlCreateTableStatement) FastsqlUtils.parseSql(ddlCreateTb).get(0);
                final SqlCreateTable sqlCreateTable = (SqlCreateTable) FastsqlParser
                    .convertStatementToSqlNode(stmt, null, ec);
                final TableMeta tm = new TableMetaParser().parse(stmt, new ExecutionContext());
                buildLogicalCreateTable(dbName, tm, sqlCreateTable, stmt.getTableName(),
                    PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));
                PartitionInfo partInfo = tm.getPartitionInfo();
                partInfoList.add(partInfo);
            }

            int nPrefixPartColCnt = 1;
            Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new TreeMap<>();
            Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, nPrefixPartColCnt, outputPartInfoGrouping, outputTblNameToGrpIdMap);

            Assert.assertTrue(outputPartInfoGrouping.size() == expectGroupingRs.length);
            for ( Map.Entry<Long, List<PartitionInfo>> grpInfoItem : outputPartInfoGrouping.entrySet() ) {
                Long grpId = grpInfoItem.getKey();
                List<PartitionInfo> partInfos = grpInfoItem.getValue();

                Assert.assertTrue(partInfos.size() == expectGroupingRs[grpId.intValue() - 1].length);
                for (int j = 0; j < partInfos.size(); j++) {
                    Assert.assertTrue(partInfos.get(j).getTableName().equalsIgnoreCase(expectGroupingRs[grpId.intValue() - 1][j]));
                }
            }
        }
    }

    static List<Pair<String[][], String[]>> createTablePairs = Arrays.asList(
        new Pair<>(new String[][]
            {
                new String[] {"t4","t5","t6"}
            },
            new String[] {
                "create table t4(a DATETIME(3), b int ) partition by range columns(a,b) ( partition p1 values less than ('2021-11-11 00:00:00',maxvalue), partition p2 values less than ('2021-11-12 00:00:00',maxvalue),partition p3 values less than ('2021-11-13 00:00:00',maxvalue))",
                "create table t5(a DATETIME(3), b int ) partition by range columns(a) ( partition p1 values less than ('2021-11-11 00:00:00'), partition p2 values less than ('2021-11-12 00:00:00'),partition p3 values less than ('2021-11-13 00:00:00'))",
                "create table t6(a DATETIME(3), b int ) partition by range columns(a,b) ( partition p1 values less than ('2021-11-11 00:00:00',maxvalue), partition p2 values less than ('2021-11-12 00:00:00',maxvalue),partition p3 values less than ('2021-11-13 00:00:00',maxvalue))",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t4"},
                new String[] {"t5"},
                new String[] {"t6"},
            },
            new String[] {
                "create table t4(a DATETIME(6), b int ) partition by range columns(a,b) ( partition p1 values less than ('2021-11-11 00:00:00',maxvalue), partition p2 values less than ('2021-11-12 00:00:00',maxvalue),partition p3 values less than ('2021-11-13 00:00:00',maxvalue))",
                "create table t5(a DATETIME(3), b int ) partition by range columns(a) ( partition p1 values less than ('2021-11-11 00:00:00'), partition p2 values less than ('2021-11-12 00:00:00'),partition p3 values less than ('2021-11-13 00:00:00'))",
                "create table t6(a DATETIME, b int ) partition by range columns(a,b) ( partition p1 values less than ('2021-11-11 00:00:00',maxvalue), partition p2 values less than ('2021-11-12 00:00:00',maxvalue),partition p3 values less than ('2021-11-13 00:00:00',maxvalue))",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t4"},
                new String[] {"t5"},
                new String[] {"t6"},
            },
            new String[] {
                "create table t4(a varchar(12), b int ) partition by range columns(a,b) ( partition p1 values less than ('a',maxvalue), partition p2 values less than ('b',maxvalue),partition p3 values less than ('c',maxvalue))",
                "create table t5(a varchar(15), b int ) partition by range columns(a) ( partition p1 values less than ('a'), partition p2 values less than ('b'),partition p3 values less than ('c'))",
                "create table t6(a varchar(16), b int ) partition by range columns(a,b) ( partition p1 values less than ('a',maxvalue), partition p2 values less than ('b',maxvalue),partition p3 values less than ('c',maxvalue))",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t1", "t3"},
                new String[] {"t2"},
            },
            new String[] {
                "create table t1(a int, b int, c int, d int ) partition by range columns(a,b,c) ( partition p1 values less than (1,10,maxvalue), partition p2 values less than (2,20,maxvalue),partition p3 values less than (3,30,maxvalue))",
                "create table t2(a int, b int, c varchar, d int ) partition by range columns(a,b,c,d) ( partition p1 values less than (1,10,maxvalue,maxvalue), partition p2 values less than (2,20,maxvalue,maxvalue),partition p3 values less than (3,31,maxvalue,maxvalue))",
                "create table t3(a int, b int, c datetime, d int ) partition by range columns(a,b,d) ( partition p1 values less than (1,10,maxvalue), partition p2 values less than (2,20,maxvalue),partition p3 values less than (3,30,maxvalue))",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t4", "t5"},
                new String[] {"t6"},
            },
            new String[] {
                "create table t4(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,maxvalue), partition p2 values less than (2,maxvalue),partition p3 values less than (3,maxvalue))",
                "create table t5(a int, b int ) partition by range columns(a) ( partition p1 values less than (1), partition p2 values less than (2),partition p3 values less than (3))",
                "create table t6(a int, b int ) partition by range columns(a,b) ( partition p1 values less than (1,2), partition p2 values less than (2,5),partition p3 values less than (3,maxvalue))",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t7", "t9"},
                new String[] {"t8"},
            },
            new String[] {
                "create table t7(a int, b int) partition by key(a,b) partitions 4",
                "create table t8(a int, c varchar) partition by key(a) partitions 5",
                "create table t9(a int, b datetime) partition by key(a,b) partitions 4",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t1","t2","t3"},
            },
            new String[] {
                "create table t1(a int, b int) partition by key(a) partitions 4",
                "create table t2(a int, c varchar) partition by key(a) partitions 4",
                "create table t3(a int, b datetime) partition by key(a) partitions 4",
            })
        ,
        new Pair<>(new String[][]
            {
                new String[] {"t1", "t2", "t3", "t4"},
                new String[] {"t5"},
            },
            new String[] {
                "create table t1(a int, b int ) partition by key(a,b) partitions 4",
                "create table t2(c int, d int ) partition by key(c,d) partitions 4",
                "create table t3(c int, d int ) partition by key(c) partitions 4",
                "create table t4(c int, d datetime ) partition by key(c) partitions 4",
                "create table t5(c varchar, d datetime ) partition by key(c) partitions 4",
            })
    );

}

