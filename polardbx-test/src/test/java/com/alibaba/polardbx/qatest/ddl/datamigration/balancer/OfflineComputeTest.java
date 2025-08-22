package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.solver.MixedModel;
import com.alibaba.polardbx.executor.balancer.solver.Solution;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author moyi
 * @since 2021/04
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@NotThreadSafe
public class OfflineComputeTest extends BalancerTestBase {

    private int tableNum = 2;

    private List<String> tableNames = new ArrayList<>();
    private int tableRows = 5000000;

    public static int maxWaitTime = 5000 * 200;

    public static int waitTime = 2000;

    @Before
    public void before() throws FileNotFoundException, SQLException, InterruptedException {
        String createTableStmt = "create table if not exists table_detail( "
            + "        table_schema varchar(128), "
            + "            table_group_name varchar(128), "
            + "            table_name varchar(128), "
            + "            index_name varchar(128), "
            + "            physical_table varchar(128), "
            + "            partition_seq int, "
            + "            partition_name varchar(128), "
            + "            subpartition_name varchar(128), "
            + "            subpartition_template_name varchar(128), "
            + "            table_rows bigint, "
            + "            data_length bigint, "
            + "            index_length bigint, "
            + "            data_free bigint, "
            + "            bound_value text, "
            + "            sub_bound_value text, "
            + "            percent varchar(128), "
            + "            storage_inst_id varchar(128), "
            + "            group_name varchar(128), "
            + "            rows_read bigint, "
            + "            rows_inserted bigint, "
            + "            rows_updated bigint, "
            + "            rows_deleted bigint "
            + ") single;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists balance_table_detail_info");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use balance_table_detail_info");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
    }

    @After
    public void after() {
    }

    protected static Pair<String, List<String>> loadInsertSqls(String fileName) throws IOException {
        String resourceDir = "partition/env/SolverTest/" + fileName;
        String fileDir = OfflineComputeTest.class.getClassLoader().getResource(resourceDir).getPath();
        BufferedReader reader = new BufferedReader(new FileReader(fileDir));
        String line = null;
        Boolean firstLine = true;
        String insertStmtTemp = " insert into table_detail(%s) values ";
        List<String> insertValues = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            String[] values = line.split("\t");
            if (firstLine) {
                String valueSpecs = StringUtils.join(values, ",").toString();
                insertStmtTemp = String.format(insertStmtTemp, valueSpecs);
                firstLine = false;
            } else {
                List<String> valueList = Arrays.stream(values).map(o -> "'" + o + "'").collect(Collectors.toList());
                String insertValue = "(" + StringUtils.join(valueList, ",").toString() + ")";
                insertValues.add(insertValue);
            }
        }
        return Pair.of(insertStmtTemp, insertValues);

    }

    public static void insertIntoTableDetail(Connection tddlConnection, String fileName) throws IOException {
        Pair<String, List<String>> insertSqlAndValue = loadInsertSqls(fileName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "truncate table " + "table_detail");
        List<String> insertValues = insertSqlAndValue.getValue();
        String insertSql = insertSqlAndValue.getKey();
        for (int i = 0; i < insertValues.size(); ) {
            List<String> insertValueTemp = new ArrayList<>();
            int j = i;
            for (; j < insertValues.size() && j < i + 10240; j++) {
                insertValueTemp.add(insertValues.get(j));
            }
            String sql = insertSql + StringUtils.join(insertValueTemp, ",");
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            i = j;
        }
        System.out.println("we have finished insert " + insertValues.size() + " values into table_detail");
    }

    public static String queryTableDetailSql =
        " select table_schema, table_group_name, table_name, physical_table, partition_name, subpartition_name,"
            + " table_rows, (data_length+index_length) data_size, storage_inst_id from table_detail where table_schema = '%s'";

    public static Map<String, TableGroupStats> getTableGroupStats(Connection tddlConnection, String tableSchema)
        throws SQLException {
        String querySql = String.format(queryTableDetailSql, tableSchema);
        ResultSet resultSet = JdbcUtil.executeQuery(querySql, tddlConnection);
        TreeMap<String, TableGroupStats> tableGroupStatsMap = new TreeMap<>();
        while (resultSet.next()) {
            String schemaName = resultSet.getString("table_schema");
            String tableGroupName = resultSet.getString("table_group_name");
            String tableName = resultSet.getString("table_name");
            String physicalTable = resultSet.getString("physical_table");
            String subPartitionGroupName = resultSet.getString("subpartition_name");
            String partitionGroupName = resultSet.getString("partition_name");
            Long tableRows = resultSet.getLong("table_rows");
            Long dataSize = resultSet.getLong("data_size");
            String storageInstId = resultSet.getString("storage_inst_id");

            TableGroupStats tableGroupStats =
                tableGroupStatsMap.computeIfAbsent(tableGroupName, k -> new TableGroupStats(tableGroupName));
            Boolean isSubPartition = !StringUtils.isEmpty(subPartitionGroupName);
            String physicalPartitionName = isSubPartition ? subPartitionGroupName : partitionGroupName;
            PartitionGroupStats partitionGroupStats =
                tableGroupStats.partitionGroupStatsMap.computeIfAbsent(physicalPartitionName,
                    k -> new PartitionGroupStats(tableGroupName, partitionGroupName, subPartitionGroupName,
                        isSubPartition));
            partitionGroupStats.appendPhysicalTableStat(tableName, physicalTable, tableRows, dataSize, storageInstId);
        }
        return tableGroupStatsMap;
    }

    public static Map<String, String> solveMovePartitionProblemAccumulate(String tableGroupName,
                                                                          Map<String, TableGroupStats> tableGroupStatsMap,
                                                                          TreeSet<String> solvedTableGroups,
                                                                          TreeMap<String, Integer> storageInstMap) {
        Map<String, String> solution = new TreeMap<>();
        Map<String, Integer> partitionNums = new TreeMap<>();
        Map<String, Long> partitionSizes = new TreeMap<>();
        initialOriginalState(solvedTableGroups, tableGroupStatsMap, partitionNums, partitionSizes, storageInstMap);
        TableGroupStats tableGroupStats = tableGroupStatsMap.get(tableGroupName);
        solution = solveMovePartitionProblem(tableGroupStats, partitionNums, partitionSizes, storageInstMap);
        return solution;
    }

    public static void initialOriginalState(TreeSet<String> solvedTableGroups,
                                            Map<String, TableGroupStats> tableGroupStatsMap,
                                            Map<String, Integer> partitionNums, Map<String, Long> partitionSizes,
                                            TreeMap<String, Integer> storageInstMap) {
        for (String storageInst : storageInstMap.keySet()) {
            partitionNums.put(storageInst, 0);
            partitionSizes.put(storageInst, 0L);
        }
        TreeSet<String> partitionNames = new TreeSet<>();
        for (String solvedTableGroup : solvedTableGroups) {
            TableGroupStats tableGroupStats = tableGroupStatsMap.get(solvedTableGroup);
            TreeMap<String, PartitionGroupStats> partitionGroupStatsMap = tableGroupStats.partitionGroupStatsMap;
            for (String partitionGroupName : partitionGroupStatsMap.keySet()) {
                String instId = partitionGroupStatsMap.get(partitionGroupName).storageInstId;
                long dataSize = partitionGroupStatsMap.get(partitionGroupName).dataLength;
                partitionSizes.put(instId, dataSize + partitionSizes.get(instId));
                String partitionName = partitionGroupStatsMap.get(partitionGroupName).partitionGroupName;
                if (!partitionNames.contains(partitionName)) {
                    partitionNums.put(instId, 1 + partitionNums.get(instId));
                    partitionNames.add(partitionName);
                }
            }
        }
    }

    public static Map<String, String> solveMovePartitionProblem(TableGroupStats tableGroupStats,
                                                                Map<String, Integer> partitionNums,
                                                                Map<String, Long> partitionSizes,
                                                                Map<String, Integer> storageInstMap) {
        TreeMap<String, String> finalSolution = new TreeMap<>();
        TreeMap<String, PartitionGroupStats> partitionGroupStatsMap = tableGroupStats.partitionGroupStatsMap;
        int physicalPartitionNum = partitionGroupStatsMap.size();

        int storageInstNum = storageInstMap.size();
        Map<Integer, String> indexToStorageInstMap = new TreeMap<>();
        for (String storageInst : storageInstMap.keySet()) {
            indexToStorageInstMap.put(storageInstMap.get(storageInst), storageInst);
        }
        int[] originalPlace = new int[physicalPartitionNum];
        double[] weights = new double[physicalPartitionNum];
        int[] storedPartitionNum = new int[storageInstNum];
        double[] storedWeight = new double[storageInstNum];
        for (String storageInst : partitionNums.keySet()) {
            int storageIndex = storageInstMap.get(storageInst);
            storedPartitionNum[storageIndex] = partitionNums.get(storageInst);
            storedWeight[storageIndex] = partitionSizes.get(storageInst);
        }
        int i = 0;
        TreeMap<String, Set<Integer>> logicalPartitionNameMap = new TreeMap<>();
        TreeMap<String, Set<String>> logicalPartitionNameToPhysicalPartitionNameMap = new TreeMap<>();
        for (String physicalPartitionName : partitionGroupStatsMap.keySet()) {
            PartitionGroupStats partitionGroupStats = partitionGroupStatsMap.get(physicalPartitionName);
            originalPlace[i] = storageInstMap.get(partitionGroupStats.storageInstId);
            weights[i] = partitionGroupStats.dataLength;
            logicalPartitionNameMap.computeIfAbsent(
                partitionGroupStatsMap.get(physicalPartitionName).partitionGroupName, k -> new HashSet<>()).add(i);
            logicalPartitionNameToPhysicalPartitionNameMap.computeIfAbsent(
                    partitionGroupStatsMap.get(physicalPartitionName).partitionGroupName, k -> new HashSet<>())
                .add(physicalPartitionName);
            i++;
        }
        double mu = 0.9;
        double lambda1 = 1 - mu;
        double lambda2 = 1 + 0.2 / (1 - mu);
        Solution
            solution =
            MixedModel.solveMovePartitionForceCompute(storageInstNum, physicalPartitionNum, originalPlace, weights,
                logicalPartitionNameMap,
                storedPartitionNum, storedWeight, lambda1, lambda2);
        Solution lastSolution = solution;
        while (solution.withValidSolve && mu > 0.1) {
            lastSolution = solution;
            mu = mu * 0.75;
            lambda1 = 1 - mu;
            lambda2 = 1 + 0.2 / (1 - mu);
            solution =
                MixedModel.solveMovePartitionForceCompute(storageInstNum, physicalPartitionNum, originalPlace, weights,
                    logicalPartitionNameMap,
                    storedPartitionNum, storedWeight, lambda1, lambda2);
        }

        int[] targetPlace = lastSolution.targetPlace;
        if (targetPlace == null) {
            targetPlace = originalPlace;
        }
        i = 0;
        for (String logicalPartitionName : logicalPartitionNameMap.keySet()) {
            for (String physicalPartitionName : logicalPartitionNameToPhysicalPartitionNameMap.get(
                logicalPartitionName)) {
                finalSolution.put(physicalPartitionName, indexToStorageInstMap.get(targetPlace[i]));
            }
            i++;
        }
        return finalSolution;
    }

    public static void applyTableGroupStats(String tableGroupName, Map<String, TableGroupStats> tableGroupStatsMap,
                                            Map<String, String> solution) {
        TableGroupStats tableGroupStats = tableGroupStatsMap.get(tableGroupName);
        for (String physicalPartitionName : tableGroupStats.partitionGroupStatsMap.keySet()) {
            tableGroupStats.partitionGroupStatsMap.get(physicalPartitionName).storageInstId =
                solution.get(physicalPartitionName);
        }
    }

    public static Map<String, Map<String, String>> testRebalanceProblem(Connection tddlConnection, String schemaName,
                                                                        String fileName)
        throws IOException, SQLException {
        insertIntoTableDetail(tddlConnection, fileName);
        Map<String, TableGroupStats> tableGroupStatsMap = getTableGroupStats(tddlConnection, schemaName);
        String tableGroupStatsMapStr = JSON.toJSONString(tableGroupStatsMap);
        Map<String, TableGroupStats> newTableGroupStatsMap =
            JSON.parseObject(tableGroupStatsMapStr, new TypeReference<Map<String, TableGroupStats>>() {
            });
        newTableGroupStatsMap = tableGroupStatsMap;
        TreeSet<String> solvedTableGroupName = new TreeSet<>();
        TreeMap<String, Integer> storageInstMap = initialStorageInstMap(newTableGroupStatsMap);
        Map<String, Map<String, String>> schemaSolutions = new HashMap<>();
        List<String> tableGroupNameList = new ArrayList<>(newTableGroupStatsMap.keySet());
//        tableGroupNameList.remove("tg11");
//        tableGroupNameList.remove("tg59");
        Collections.shuffle(tableGroupNameList);
//        tableGroupNameList.add(0, "tg11");
//        tableGroupNameList.add(1, "tg59");
        for (int i = 0; i < tableGroupNameList.size(); i++) {
            String tableGroupName = tableGroupNameList.get(i);
            Map<String, String> solution =
                solveMovePartitionProblemAccumulate(tableGroupName, newTableGroupStatsMap, solvedTableGroupName,
                    storageInstMap);
            solvedTableGroupName.add(tableGroupName);
            applyTableGroupStats(tableGroupName, newTableGroupStatsMap, solution);
            schemaSolutions.put(tableGroupName, solution);
        }
        return schemaSolutions;
    }

    private static TreeMap<String, Integer> initialStorageInstMap(Map<String, TableGroupStats> newTableGroupStatsMap) {
        AtomicInteger i = new AtomicInteger();
        TreeMap<String, Integer> storageInstMap = new TreeMap<>();
        for (TableGroupStats tableGroupStats : newTableGroupStatsMap.values()) {
            for (PartitionGroupStats partitionGroupStats : tableGroupStats.partitionGroupStatsMap.values()) {
                storageInstMap.computeIfAbsent(partitionGroupStats.storageInstId, k -> (i.getAndIncrement()));
            }
        }
        return storageInstMap;
    }

    @Test
    @Ignore
    public void testRandomData1() throws SQLException, IOException {
        Map<String, Map<String, String>> schemaSolution =
            testRebalanceProblem(tddlConnection, "random_data1", "random_data1.txt");

    }

    @Test
    @Ignore
    public void testRandomData2() throws SQLException, IOException {
        Map<String, Map<String, String>> schemaSolution =
            testRebalanceProblem(tddlConnection, "random_data2", "random_data2.txt");

    }

    @Ignore
    @Test
    public void testOnlyForValidate() throws SQLException, IOException {
        {
            int N = 4;
            int M = 4;
            int[] originalPlace = {0, 1, 2, 3};
            double[] weight = {
                10, 10, 10, 3
            };
            Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, weight);
            Assert.assertTrue(solution.withValidSolve);
        }

        {
            int N = 32;
            int M = 4;
            int[] originalPlace = {
                2, 3, 2, 1, 2, 2, 2, 0, 3, 2, 1, 3, 1, 0, 3, 1, 2, 3, 1, 1, 2, 0,
                1, 1, 2, 0, 2, 0, 3, 0, 2, 2};
            double[] weight = {
                38.97159876, 245.83359855, 61.16195536, 235.69867329,
                228.87400895, 75.36737441, 170.95714039, 142.49200599,
                21.86900384, 131.95151408, 507.93869079, 15.79410773,
                346.41577768, 166.27455326, 109.28667149, 123.7734882,
                130.73867517, 243.10617929, 290.82485184, 609.94877029,
                281.36828914, 265.80116796, 249.91876762, 46.81863377,
                62.05097136, 39.32968497, 394.5090844, 213.20195816,
                25.74168461, 16.06044984, 120.03769936, 250.8436211
            };
            Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, weight);
            Assert.assertTrue(solution.withValidSolve);
        }
    }

}
