package com.alibaba.polardbx.executor.balancer;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.balancer.solver.GreedyModel;
import com.alibaba.polardbx.executor.balancer.solver.SequentialPlaceModel;
import com.alibaba.polardbx.executor.balancer.solver.Solution;
import com.alibaba.polardbx.executor.balancer.solver.MixedModel;

import com.google.common.collect.Lists;
import groovy.lang.IntRange;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jinkun.taojinkun
 * @since 2022/08
 */
public class TestBalanceSolver {
    public static Logger logger =  LoggerFactory.getLogger(TestBalanceSolver.class);

    public static void testMixedSolver(int M, int N, int[] originalPlace, double[] weight, double moveRate,
                                       double overRate) {
        Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, weight);
        Assert.assertTrue(solution.withValidSolve);
        int[] targetPlace = solution.targetPlace;
        double[] sumWeight = new double[M];
        double fullWeight = 0;
        double avgWeight = 0;
        double moveCost = 0;
        int[] count = new int[M];
        for (int i = 0; i < N; i++) {
            fullWeight += weight[i];
            int j = targetPlace[i];
            sumWeight[j] += weight[i];
            count[j]++;
            if (originalPlace[i] != j) {
                moveCost += weight[i];
            }
        }
        avgWeight = fullWeight / M;
        double maxOverload = 0;
        for (int j = 0; j < M; j++) {
            double overload = Math.abs(sumWeight[j] - avgWeight);
            maxOverload = (maxOverload > overload) ? maxOverload : overload;
        }
        int maxN = (int) Math.ceil(N / (float) M);
        int minN = (int) Math.floor(N / (float) M);
        for (int i = 0; i < M; i++) {
            Assert.assertTrue(count[i] >= minN);
            Assert.assertTrue(count[i] <= maxN);
        }
        double actualMoveRate = moveCost / fullWeight;
        double actualOverRate = maxOverload / avgWeight;
        System.out.println("actualMoveRate = " + String.valueOf(actualMoveRate));
        System.out.println("actualOverRate = " + actualOverRate);
        System.out.println(visualizeCase(M, N, originalPlace, weight, targetPlace));
        Assert.assertTrue(actualMoveRate < moveRate);
        Assert.assertTrue(actualOverRate < overRate);
    }

    public static DataDistInfo.DnInfo mockDnInfo(int i){
        return new DataDistInfo.DnInfo("dn" + i, "group_" + i);
    }

    public static DataDistInfo.TgDataDistInfo mockTgDataDistInfo(int N, int[] originalPlace, double[] weight, int[] targetPlace) {
        String tgName = "mock_tg";
        String tableName = "mock_table";
        List<DataDistInfo.PgDataDistInfo> pgDataDistInfos = Lists.newArrayList();
        for (int i = 0; i < N; i++) {
            String pgName = "pg" + i;
            pgDataDistInfos.add(new DataDistInfo.PgDataDistInfo(tgName, pgName, (long) weight[i], (long)weight[i], originalPlace[i], targetPlace[i], ""));
        }
        DataDistInfo.TgDataDistInfo tgDataDistInfo = new DataDistInfo.TgDataDistInfo(tgName, pgDataDistInfos, Lists.newArrayList(tableName));
        return tgDataDistInfo;
    }
    public static String visualizeCase(int M, int N, int[] originalPlace, double[] weight, int[] targetPlace){
        String schemaName = "mock_db";
        List<DataDistInfo.DnInfo> dnInfoList = new IntRange(0, M - 1).stream().map(o->mockDnInfo(o)).collect(Collectors.toList());
        DataDistInfo.TgDataDistInfo tgDataDistInfo = mockTgDataDistInfo(N, originalPlace, weight, targetPlace);
        DataDistInfo dataDistInfo = new DataDistInfo(schemaName, dnInfoList, Lists.newArrayList(tgDataDistInfo));
        String result = JSON.toJSONString(dataDistInfo);
        return result;
    }

    public void testSequentialModelSolver(int M, int N, int[] originalPlace, double[] weight) {
        int[] targetPlace = SequentialPlaceModel.solve(M, N, originalPlace, weight);
        int avgPartNum = N / M;
        int index = 0;
        for (int i = 0; i < M; i++) {
            int ref = index;
            for (int j = 0; j < avgPartNum; j++) {
                Assert.assertTrue(targetPlace[ref] == targetPlace[index]);
                index++;
            }
        }
    }

    public void testSequentialModelSolverDrainNode(int M, int N, int[] originalPlace, double[] weight,
                                                   int[] drainIndex) {
        int[] targetPlace = SequentialPlaceModel.solve(M, N, originalPlace, weight, drainIndex);
        int avgPartNum = N / (M - drainIndex.length);
        int index = 0;
        Set<Integer> drainIndexSet = Arrays.stream(drainIndex).boxed().collect(Collectors.toSet());
        for (int i = 0; i < M - drainIndex.length; i++) {
            int ref = index;
            Assert.assertTrue(!drainIndexSet.contains(targetPlace[ref]));
            for (int j = 0; j < avgPartNum; j++) {
                Assert.assertTrue(targetPlace[ref] == targetPlace[index]);
                index++;
            }
        }
    }

    public static void testGreedySolver(int M, int N, int[] originalPlace, double[] weight, double moveRate,
                                        double overRate) {
        int[] targetPlace = GreedyModel.solve(M, N, originalPlace, weight);
        double[] sumWeight = new double[M];
        double fullWeight = 0;
        double avgWeight = 0;
        double moveCost = 0;
        int[] count = new int[M];
        for (int i = 0; i < N; i++) {
            fullWeight += weight[i];
            int j = targetPlace[i];
            sumWeight[j] += weight[i];
            count[j]++;
            if (originalPlace[i] != j) {
                moveCost += weight[i];
            }
        }
        avgWeight = fullWeight / M;
        double maxOverload = 0;
        for (int j = 0; j < M; j++) {
            double overload = Math.abs(sumWeight[j] - avgWeight);
            maxOverload = (maxOverload > overload) ? maxOverload : overload;
        }
        int maxN = (int) Math.ceil(N / (float) M);
        int minN = (int) Math.floor(N / (float) M);
        for (int i = 0; i < M; i++) {
            Assert.assertTrue(count[i] >= minN);
            Assert.assertTrue(count[i] <= maxN);
        }

        Assert.assertTrue(moveCost / fullWeight < moveRate);
        Assert.assertTrue(maxOverload / avgWeight < overRate);
    }

    public static void testMixedSolverDrainNode(int M, int N, int[] originalPlace, double[] weight, double moveRate,
                                                double overRate, int[] drainIndexes) {
        Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, weight, drainIndexes);
        int resM = M - drainIndexes.length;
        Assert.assertTrue(solution.withValidSolve);
        int[] targetPlace = solution.targetPlace;
        double[] sumWeight = new double[M];
        double fullWeight = 0;
        double avgWeight = 0;
        double moveCost = 0;
        int[] count = new int[M];
        for (int i = 0; i < N; i++) {
            fullWeight += weight[i];
            int j = targetPlace[i];
            sumWeight[j] += weight[i];
            count[j]++;
            if (originalPlace[i] != j) {
                moveCost += weight[i];
            }
        }
        avgWeight = fullWeight / resM;
        double maxOverload = 0;
        Set<Integer> drainIndexSet = Arrays.stream(drainIndexes).boxed().collect(Collectors.toSet());
        for (int j = 0; j < M; j++) {
            if (!drainIndexSet.contains(j)) {
                double overload = Math.abs(sumWeight[j] - avgWeight);
                maxOverload = (maxOverload > overload) ? maxOverload : overload;
            }
        }
        int maxN = (int) Math.ceil(N / (float) resM);
        int minN = (int) Math.floor(N / (float) resM);
        for (int i = 0; i < M; i++) {
            if (!drainIndexSet.contains(i)) {
                Assert.assertTrue(count[i] >= minN);
                Assert.assertTrue(count[i] <= maxN);
            } else {
                Assert.assertTrue(count[i] == 0);
            }
        }
        double actualMoveRate = moveCost / fullWeight;
        double actualOverRate = maxOverload / avgWeight;
        Assert.assertTrue(actualMoveRate < moveRate);
        Assert.assertTrue(actualOverRate < overRate);
    }

    public static void testGreedySolverDrainNode(int M, int N, int[] originalPlace, double[] weight, double moveRate,
                                                 double overRate, int[] drainIndexes) {
        int[] targetPlace = GreedyModel.solve(M, N, originalPlace, weight, drainIndexes);
        int resM = M - drainIndexes.length;
        double[] sumWeight = new double[M];
        double fullWeight = 0;
        double avgWeight = 0;
        double moveCost = 0;
        int[] count = new int[M];
        for (int i = 0; i < N; i++) {
            fullWeight += weight[i];
            int j = targetPlace[i];
            sumWeight[j] += weight[i];
            count[j]++;
            if (originalPlace[i] != j) {
                moveCost += weight[i];
            }
        }
        avgWeight = fullWeight / resM;
        double maxOverload = 0;
        Set<Integer> drainIndexSet = Arrays.stream(drainIndexes).boxed().collect(Collectors.toSet());
        for (int j = 0; j < M; j++) {
            if (!drainIndexSet.contains(j)) {
                double overload = Math.abs(sumWeight[j] - avgWeight);
                maxOverload = (maxOverload > overload) ? maxOverload : overload;
            }
        }
        int maxN = (int) Math.ceil(N / (float) resM);
        int minN = (int) Math.floor(N / (float) resM);
        for (int i = 0; i < M; i++) {
            if (!drainIndexSet.contains(i)) {
                Assert.assertTrue(count[i] >= minN);
                Assert.assertTrue(count[i] <= maxN);
            } else {
                Assert.assertTrue(count[i] == 0);
            }
        }

        Assert.assertTrue(moveCost / fullWeight < moveRate);
        Assert.assertTrue(maxOverload / avgWeight < overRate);
    }

    public static void testSolverFromFile(String sampleName, String solverType) throws FileNotFoundException {
        String resourceDir = String.format("com/alibaba/polardbx/executor/balancer/%s.sample.txt", sampleName);
        String fileDir = TestBalanceSolver.class.getClassLoader().getResource(resourceDir).getPath();
        File file = new File(fileDir);
        try (Scanner scanner = new Scanner(file)) {
            int N = scanner.nextInt();
            int M = scanner.nextInt();
            int[] originalPlace = new int[N];
            double[] weight = new double[N];
            for (int i = 0; i < N; i++) {
                originalPlace[i] = scanner.nextInt();
            }
            for (int i = 0; i < N; i++) {
                weight[i] = scanner.nextDouble();
            }
            double moveRate = scanner.nextDouble();
            double overRate = scanner.nextDouble();
            if (solverType.equals("greedy")) {
                testGreedySolver(M, N, originalPlace, weight, moveRate, overRate);
            } else if (solverType.equals("mixed")) {
                testMixedSolver(M, N, originalPlace, weight, moveRate, overRate);
            } else if (solverType.equals("greedyDrainNode")) {
                int K = scanner.nextInt();
                int[] drainNodeIndexes = new int[K];
                for (int i = 0; i < K; i++) {
                    drainNodeIndexes[i] = scanner.nextInt();
                }
                testGreedySolverDrainNode(M, N, originalPlace, weight, moveRate, overRate, drainNodeIndexes);
            } else if (solverType.equals("mixedDrainNode")) {
                int K = scanner.nextInt();
                int[] drainNodeIndexes = new int[K];
                for (int i = 0; i < K; i++) {
                    drainNodeIndexes[i] = scanner.nextInt();
                }
                testMixedSolverDrainNode(M, N, originalPlace, weight, moveRate, overRate, drainNodeIndexes);

            }
        }
    }


    @Test
    public void testSequentialSolver() {
        int N = 16;
        int M = 4;
        int[] originalPlace = {0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3};
        double[] weight = {
            100, 10, 10, 10, 20, 50, 20, 50, 10, 10, 20, 10, 20, 40, 50, 40
        };
        testSequentialModelSolver(M, N, originalPlace, weight);
    }

    @Test
    public void testSequentialSolverDrainNode() {
        int N = 16;
        int M = 4;
        int[] originalPlace = {0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3};
        int[] drainNode = {0, 1};
        double[] weight = {
            100, 10, 10, 10, 20, 50, 20, 50, 10, 10, 20, 10, 20, 40, 50, 40
        };
        testSequentialModelSolverDrainNode(M, N, originalPlace, weight, drainNode);
    }

    @Test
    public void testLargeSequentialSolver() {
        int N = 16;
        int M = 2;
        int[] originalPlace = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
        double[] weight = {
            100, 10, 10, 10, 20, 50, 20, 50, 10, 10, 20, 10, 20, 40, 50, 40
        };
        testSequentialModelSolver(M, N, originalPlace, weight);
    }

    @Test
    public void testSolverInfeasible() {
        int N = 4;
        int M = 4;
        int[] originalPlace = {0, 1, 2, 3};
        double[] weight = {
            10, 10, 10, 3
        };
        Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, weight);
        Assert.assertTrue(solution.withValidSolve);
    }

    @Test
    public void testMixedSolver32_4() {
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
        double moveRate = 0.28;
        double overRate = 0.12;
        testMixedSolver(M, N, originalPlace, weight, moveRate, overRate);
    }

    @Test
    public void testGreedySolver8_2() {
        int N = 8;
        int M = 2;
        int[] originalPlace = {0, 0, 0, 0, 1, 1, 1, 1};
        double[] weight = {
            1, 2, 3, 4,
            10, 20, 30, 40
        };
        double moveRate = 0.51;
        double overRate = 0.01;
        testGreedySolver(M, N, originalPlace, weight, moveRate, overRate);
    }

    @Test
    public void testGreedySolverEmpty8_2() {
        int N = 8;
        int M = 2;
        int[] originalPlace = {0, 0, 0, 0, 1, 1, 1, 1};
        double[] weight = {
            0, 0, 0, 0,
            0, 0, 0, 0
        };
        int[] targetPlace = GreedyModel.solve(M, N, originalPlace, weight);
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(originalPlace[i], targetPlace[i]);
        }
    }

    @Test
    public void testMixedSolverEmpty8_2() {
        int N = 8;
        int M = 2;
        int[] originalPlace = {0, 0, 0, 0, 1, 1, 1, 1};
        double[] weight = {
            0, 0, 0, 0,
            0, 0, 0, 0
        };
        int[] targetPlace = MixedModel.solveMovePartition(M, N, originalPlace, weight).targetPlace;
        for (int i = 0; i < N; i++) {
            Assert.assertEquals(originalPlace[i], targetPlace[i]);
        }
    }

    @Test
    public void testGreedySolver7_3() {
        int N = 7;
        int M = 3;
        int[] originalPlace = {0, 0, 0, 1, 1, 2, 2};
        double[] weight = {
            20, 15, 10, 4, 4, 1, 1
        };
        double moveRate = 0.51;
        double overRate = 0.15;
        testGreedySolver(M, N, originalPlace, weight, moveRate, overRate);
    }

    @Test
    public void testGreedySolver32_4() {
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
        double moveRate = 0.34;
        double overRate = 0.05;
        testGreedySolver(M, N, originalPlace, weight, moveRate, overRate);
    }

    @Test
    public void testGreedySolver64_8() throws FileNotFoundException {
        String sampleName = "rebalance_64_8_greedy";
        testSolverFromFile(sampleName, "greedy");
    }

    @Test
    public void testGreedySolverDrainNode64_8() throws FileNotFoundException {
        String sampleName = "rebalance_64_8_greedyDrainNode";
        testSolverFromFile(sampleName, "greedyDrainNode");
    }

    @Test
    public void testMixedSolver64_8() throws FileNotFoundException {
        String sampleName = "rebalance_64_8_mixed";
        testSolverFromFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolverDrainNode64_8() throws FileNotFoundException {
        String sampleName = "rebalance_64_8_mixedDrainNode";
        testSolverFromFile(sampleName, "mixedDrainNode");
    }

    @Test
    public void testGreedySolver256_16() throws FileNotFoundException {
        String sampleName = "rebalance_256_16_greedy";
        testSolverFromFile(sampleName, "greedy");
    }

    @Test
    public void testGreedySolverDrainNode256_16() throws FileNotFoundException {
        String sampleName = "rebalance_256_16_greedyDrainNode";
        testSolverFromFile(sampleName, "greedyDrainNode");
    }

    @Test
    public void testMixedSolver256_16() throws FileNotFoundException {
        String sampleName = "rebalance_256_16_mixed";
        testSolverFromFile(sampleName, "mixed");
    }




    @Test
    public void testMixedSolverDrainNode256_16() throws FileNotFoundException {
        String sampleName = "rebalance_256_16_mixedDrainNode";
        testSolverFromFile(sampleName, "mixedDrainNode");
    }

    @Test
    public void testGreedySolver1024_32() throws FileNotFoundException {
        String sampleName = "rebalance_1024_32_greedy";
        testSolverFromFile(sampleName, "greedy");
    }

    @Test
    public void testGreedySolverDrainNode1024_32() throws FileNotFoundException {
        String sampleName = "rebalance_1024_32_greedyDrainNode";
        testSolverFromFile(sampleName, "greedyDrainNode");
    }

    @Test
    public void testMixedSolver1024_32() throws FileNotFoundException {
        String sampleName = "rebalance_1024_32_mixed";
        testSolverFromFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolverDrainNode1024_32() throws FileNotFoundException {
        String sampleName = "rebalance_1024_32_mixedDrainNode";
        testSolverFromFile(sampleName, "mixedDrainNode");
    }

    @Test
    public void testMixedSolver1024_16() throws FileNotFoundException {
        String sampleName = "rebalance_1024_16_mixed";
        testSolverFromFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolverDrainNode1024_16() throws FileNotFoundException {
        String sampleName = "rebalance_1024_16_mixedDrainNode";
        testSolverFromFile(sampleName, "mixedDrainNode");
    }

    @Test
    public void testMixedSolver2048_16() throws FileNotFoundException {
        String sampleName = "rebalance_2048_16_mixed";
        testSolverFromFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolver4096_16() throws FileNotFoundException {
        String sampleName = "rebalance_4096_16_mixed";
        testSolverFromFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolver8192_16() throws FileNotFoundException {
        String sampleName = "rebalance_8192_16_mixed";
        testSolverFromFile(sampleName, "mixed");
    }
}