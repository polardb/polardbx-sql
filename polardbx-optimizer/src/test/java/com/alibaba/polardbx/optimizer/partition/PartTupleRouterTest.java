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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ColumnarShardProcessor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;
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
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class PartTupleRouterTest extends BasePlannerTest {

    static final String dbName = "optest";

    protected PartTupleRouterTestParam testParameter;

    public PartTupleRouterTest(PartTupleRouterTestParam testParameter) {
        super(dbName);
        this.testParameter = testParameter;
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    public static class PartTupleRouterTestParam {

        private String caseName;
        private String createTblDdl = " ";

        /**
         * <pre>
         * each entry of the list rowValAndExceptRsInfo are defined as followed:
         *     key: rowVals of shard columns, a list
         *     val:
         *          v1: exceptedTupleValeAfterCalcPartFunc: String[][]
         *          v2: exceptedPartName: String
         * </pre>
         */
        public List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo;

        public PartTupleRouterTestParam(String caseName,
                                        String ddl,
                                        boolean useHash,
                                        List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo) {
            this.caseName = caseName;
            this.createTblDdl = ddl;

            this.rowValAndExceptRsInfo = rowValAndExceptRsInfo;
        }

        @Override
        public String toString() {
            return String.format("case:[%s]", caseName);
        }
    }

    @Test
    public void testPartTupleRouterByParams() throws SQLException {
        ec.setParams(new Parameters());
        ec.setSchemaName(appName);
        ec.setServerVariables(new HashMap<>());

        String ddl = this.testParameter.createTblDdl;
        List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo = this.testParameter.rowValAndExceptRsInfo;
        //String sql2 = this.testParameter.createTablePair.getValue().getValue();

        final MySqlCreateTableStatement stmt =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(ddl).get(0);

        final TableMeta tm = new TableMetaParser().parse(stmt);

        final SqlCreateTable sqlCreateTable = (SqlCreateTable) FastsqlParser
            .convertStatementToSqlNode(stmt, null, ec);

        buildLogicalCreateTable(dbName, tm, sqlCreateTable, stmt.getTableName(),
            PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));

        PartitionInfo tmPartInfo = tm.getPartitionInfo();

        boolean isHash = false;
        if (ddl.toLowerCase().contains("partition by key") || ddl.toLowerCase().contains("partition by hash")) {
            isHash = true;
        }
        if (isHash) {
            testPartTupleRouterCalcHash(tmPartInfo, ec, rowValAndExceptRsInfo);
        }
        testPartTupleRouterCalcSearchDatum(tmPartInfo, ec, rowValAndExceptRsInfo);
        testPartTupleRouterRouteTuple(tmPartInfo, ec, rowValAndExceptRsInfo);
        testColumnarShardRouteTuple(tmPartInfo, ec, rowValAndExceptRsInfo);

    }

    protected void testPartTupleRouterCalcHash(PartitionInfo tmPartInfo,
                                               ExecutionContext ec,
                                               List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo) {

        PartTupleRouter tupleRouter = new PartTupleRouter(tmPartInfo, ec);
        tupleRouter.init();

        for (int i = 0; i < rowValAndExceptRsInfo.size(); i++) {
            Pair<List<Object>, List<Object>> rowValAndRsItem = rowValAndExceptRsInfo.get(i);
            List<Object> rowVal = rowValAndRsItem.getKey();
            String[][] tarSearchDatumStr = (String[][]) rowValAndRsItem.getValue().get(0);

            /**
             * Calc the hash code for the row value
             */
            List<Long[]> hashVals = tupleRouter.calcHashCode(Arrays.asList(rowVal));

            for (int k = 0; k < hashVals.size(); k++) {
                Long[] hashVal = hashVals.get(k);
                for (int j = 0; j < hashVal.length; j++) {
                    String str1 = String.valueOf(hashVal[j]);
                    String str2 = tarSearchDatumStr[k][j];
                    Assert.assertTrue(str1.equals(str2));
                }
            }

        }
    }

    protected void testPartTupleRouterCalcSearchDatum(PartitionInfo tmPartInfo,
                                                      ExecutionContext ec,
                                                      List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo) {

        PartTupleRouter tupleRouter = new PartTupleRouter(tmPartInfo, ec);
        tupleRouter.init();

        for (int i = 0; i < rowValAndExceptRsInfo.size(); i++) {
            Pair<List<Object>, List<Object>> rowValAndRsItem = rowValAndExceptRsInfo.get(i);
            List<Object> rowVal = rowValAndRsItem.getKey();
            String[][] tarSearchDatumStr = (String[][]) rowValAndRsItem.getValue().get(0);

            /**
             * route tuple to partition for the row value
             */
            List<SearchDatumInfo> tarSearchDatums = tupleRouter.calcSearchDatum(Arrays.asList(rowVal));

            for (int k = 0; k < tarSearchDatums.size(); k++) {
                SearchDatumInfo tarSearchDatum = tarSearchDatums.get(k);
                for (int j = 0; j < tarSearchDatum.getDatumInfo().length; j++) {
                    String str1 = tarSearchDatumStr[k][j];
                    String str2 = tarSearchDatum.getDatumInfo()[j].toString();
                    Assert.assertTrue(str1.equals(str2));
                }
            }
        }
    }

    protected void testPartTupleRouterRouteTuple(PartitionInfo tmPartInfo,
                                                 ExecutionContext ec,
                                                 List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo) {

        PartTupleRouter tupleRouter = new PartTupleRouter(tmPartInfo, ec);
        tupleRouter.init();

        for (int i = 0; i < rowValAndExceptRsInfo.size(); i++) {
            Pair<List<Object>, List<Object>> rowValAndRsItem = rowValAndExceptRsInfo.get(i);
            List<Object> rowVal = rowValAndRsItem.getKey();
            String tarPart = (String) rowValAndRsItem.getValue().get(1);

            /**
             * route tuple to partition for the row value
             */
            PhysicalPartitionInfo phyInfo = tupleRouter.routeTuple(Arrays.asList(rowVal));
            Assert.assertTrue(phyInfo.getPartName().equalsIgnoreCase(tarPart));
        }
    }

    protected void testColumnarShardRouteTuple(PartitionInfo tmPartInfo,
                                               ExecutionContext ec,
                                               List<Pair<List<Object>, List<Object>>> rowValAndExceptRsInfo) {

        PartTupleRouter tupleRouter = new PartTupleRouter(tmPartInfo, ec);
        tupleRouter.init();

        for (int i = 0; i < rowValAndExceptRsInfo.size(); i++) {
            Pair<List<Object>, List<Object>> rowValAndRsItem = rowValAndExceptRsInfo.get(i);
            List<Object> rowVal = rowValAndRsItem.getKey();
            String tarPart = (String) rowValAndRsItem.getValue().get(1);

            String calPart = ColumnarShardProcessor.shard(tupleRouter, rowVal, null, true);
            String calPart2 = ColumnarShardProcessor.shard(tupleRouter, rowVal, null, false);

            Assert.assertTrue(calPart.equalsIgnoreCase(tarPart));
            Assert.assertTrue(calPart2.equalsIgnoreCase(tarPart));
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    @Parameterized.Parameters(name = "{index}: params {0}")
    public static List<PartTupleRouterTestParam> parameters() {
        List<PartTupleRouterTestParam> parameters = new ArrayList<>();
        for (int i = 0; i < caseList.size(); i++) {
            parameters.add(caseList.get(i));
        }
        return parameters;
    }

    static List<PartTupleRouterTestParam> caseList = Arrays.asList(

        new PartTupleRouterTestParam(
            "case-1",
            "create table t9(a datetime, b int) partition by range(year(a)) (partition p1 values less than (2000),partition p2 values less than (2020),partition p3 values less than (maxvalue))",
            false,
            Arrays.asList(
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2009-01-01 00:00:00"),
                    Arrays.asList(
                        new String[][] {
                            {"2009"}
                        },
                        "p2"
                    )
                )
            )
        ),

        new PartTupleRouterTestParam(
            "case-2",
            "create table tbl(a datetime, b int) partition by key(a) partitions 4",
            true,
            Arrays.asList(
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2009-01-01 00:00:00"),
                    Arrays.asList(
                        new String[][] {
                            {"749964047422459224"}
                        },
                        "p3"
                    )
                ),
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2000-01-01 00:00:00"),
                    Arrays.asList(
                        new String[][] {
                            {"4796388105427186539"}
                        },
                        "p4"
                    )
                )
            )
        ),
        new PartTupleRouterTestParam(
            "case-3",
            "create table tbl(a datetime, b int) partition by key(a,b) partitions 4",
            true,
            Arrays.asList(
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2009-01-01 00:00:00", "10"),
                    Arrays.asList(
                        new String[][] {
                            {"749964047422459224", "8033048159982359313"}
                        },
                        "p3"
                    )
                ),
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2000-01-01 00:00:00", "10"),
                    Arrays.asList(
                        new String[][] {
                            {"4796388105427186539", "8033048159982359313"}
                        },
                        "p4"
                    )
                )
            )
        ),

        new PartTupleRouterTestParam(
            "case-4",
            "create table tbl(a datetime, b int) partition by hash(a,b) partitions 4",
            true,
            Arrays.asList(
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2009-01-01 00:00:00", "10"),
                    Arrays.asList(
                        new String[][] {
                            {"-2533267580759484321"}
                        },
                        "p2"
                    )
                ),
                new Pair<List<Object>, List<Object>>(
                    Arrays.asList("2000-01-01 00:00:00", "10"),
                    Arrays.asList(
                        new String[][] {
                            {"-4861912150126808042"}
                        },
                        "p1"
                    )
                )
            )
        )
    );
}

