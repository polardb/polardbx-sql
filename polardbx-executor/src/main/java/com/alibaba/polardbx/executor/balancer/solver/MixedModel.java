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

package com.alibaba.polardbx.executor.balancer.solver;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.sun.jna.Platform;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MixedModel {
    public LpModel lpModel;

    public GreedyModel greedyModel = new GreedyModel();

    public SplitModel splitModel = new SplitModel();

    private double maxBalanceFactor = 0.5;

    private double splitBalanceFactor = 0.2;

    private double minBalanceFactor = 0.08;

    private double middleBalanceFactor = 0.15;

    public int maxNodeNum = 32;

    public int maxPartitionNum = 2048;

    public enum SolveLevel {
        BALANCE_DEFAULT,
        NON_HOT_SPLIT,
        MIN_COST,
        HOT_SPLIT
    }

    public static Boolean checkNativeOptimizationSupport() {
        if ((Platform.isARM() && Platform.isMac()) || (Platform.isIntel() && Platform.isLinux())) {
            return true;
        }
        return false;
    }

    public void buildLpModel(int M, int N, int[] X, double Weight[], double mu) {
        this.lpModel = new LpModel();
        this.lpModel.setVariable(M, N, X, Weight, 1 - mu, 1 + mu);
    }

    public void buildLpModel(int M, int N, int[] X, double Weight[], double mu, int[] drainNodeIndexes) {
        this.lpModel = new LpModel();
        this.lpModel.setVariable(M, N, X, Weight, 1 - mu, 1 + mu, drainNodeIndexes);
    }

    public void buildSplitModel(int M, int N, int[] X, double Weight[], PartitionSplitInfo partitionSplitInfo,
                                double mu) {
        this.splitModel.fromGreedyModel(this.greedyModel);
        this.splitModel.setPartitionSplitPlan(partitionSplitInfo);
    }

    public void buildGreedyModel(int M, int N, int[] X, double Weight[]) {
        this.greedyModel.setVariable(M, N, X, Weight);
        this.greedyModel.preSolve();
    }

    public void buildGreedyModel(int M, int N, int[] X, double Weight[], int[] drainNodeIndexes) {
        this.greedyModel.setVariable(M, N, X, Weight, drainNodeIndexes);
        this.greedyModel.preSolve();
    }

    public double getBalanaceFactor() {
        return greedyModel.mu;
    }

    public Solution lpSolve() {
        return lpModel.lpSolveWithRetry();
    }

    public Solution greedySolve() {
        return greedyModel.greedySolveWithRetry();
    }

    public Solution splitSolve() {
        return splitModel.splitSolve();
    }

    public void closeLpModel() {
        lpModel.close();
    }

    public static Solution solveSplitPartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                               PartitionSplitInfo partitionSplitInfo) {
        MixedModel model = new MixedModel();
        model.buildGreedyModel(m, n, originalPlace, partitionSize);
        double mu = model.getBalanaceFactor();
        Solution solution = new Solution(true);
        if (mu >= model.splitBalanceFactor) {
            model.buildSplitModel(m, n, originalPlace, partitionSize, partitionSplitInfo, mu);
            solution = model.splitSolve();
        }
        return solution;
    }

    public static Solution solveMovePartitionByGreedy(int m, int n, int[] originalPlace, int[] selectedIndex,
                                                      double[] partitionSize) {
        Map<Integer, Integer> reindexedMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> i, i -> selectedIndex[i]));
        Map<Integer, Integer> reindexedReverseMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> selectedIndex[i], i -> i));
        int[] reindexedOriginalPlace = Arrays.stream(originalPlace).map(i -> reindexedReverseMap.get(i)).toArray();
        Solution solution = solveMovePartitionByGreedy(m, n, reindexedOriginalPlace, partitionSize);
        if (solution.withValidSolve) {
            solution.targetPlace = Arrays.stream(solution.targetPlace).map(i -> reindexedMap.get(i)).toArray();
        }
        return solution;
    }

    public static Solution solveMovePartitionByGreedy(int m, int n, int[] originalPlace, double[] partitionSize) {
        MixedModel model = new MixedModel();
        model.buildGreedyModel(m, n, originalPlace, partitionSize);
        double mu = model.getBalanaceFactor();
        Pair<Boolean, Double> result = caculateBalanceFactor(m, n, originalPlace, partitionSize);
        Boolean isNumBalanced = result.getKey();
        double originalMu = result.getValue();
        if (isNumBalanced && originalMu - mu <= model.minBalanceFactor * 1.5) {
            return new Solution(true, originalPlace, originalMu, "IdempotentCompare");
        } else {
            return model.greedySolve();
        }
    }

    public static Solution solveMovePartitionByGreedy(int m, int n, int[] originalPlace, double[] partitionSize,
                                                      int[] selectedIndexes,
                                                      int[] drainNodeIndexes) {
        Map<Integer, Integer> reindexedMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> i, i -> selectedIndexes[i]));
        Map<Integer, Integer> reindexedReverseMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> selectedIndexes[i], i -> i));
        int[] reindexedOriginalPlace = Arrays.stream(originalPlace).map(i -> reindexedReverseMap.get(i)).toArray();
        int[] reindexedDrainNodeIndexes =
            Arrays.stream(drainNodeIndexes).map(i -> reindexedReverseMap.get(i)).toArray();
        Solution solution =
            solveMovePartitionByGreedy(m, n, reindexedOriginalPlace, partitionSize, reindexedDrainNodeIndexes);
        if (solution.withValidSolve) {
            solution.targetPlace = Arrays.stream(solution.targetPlace).map(i -> reindexedMap.get(i)).toArray();
        }
        return solution;
    }

    public static Solution solveMoveSequentialPartition(int m, int n, int[] originalPlace, double[] partitionSize) {
        SequentialPlaceModel sequentialPlaceModel = new SequentialPlaceModel();
        sequentialPlaceModel.setVariable(m, n, originalPlace, partitionSize);
        return sequentialPlaceModel.lpSolveWithRetry();
    }

    public static Solution solveMoveSequentialPartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                                        int[] drainNodeIndex) {
        SequentialPlaceModel sequentialPlaceModel = new SequentialPlaceModel();
        sequentialPlaceModel.setVariable(m, n, originalPlace, partitionSize, drainNodeIndex);
        return sequentialPlaceModel.lpSolveWithRetry();
    }

    public static Solution solveMovePartitionByGreedy(int m, int n, int[] originalPlace, double[] partitionSize,
                                                      int[] drainNodeIndexes) {
        MixedModel model = new MixedModel();
        model.buildGreedyModel(m, n, originalPlace, partitionSize, drainNodeIndexes);
        double mu = model.getBalanaceFactor();
        Pair<Boolean, Double> result = caculateBalanceFactor(m, n, originalPlace, partitionSize, drainNodeIndexes);
        Boolean isNumBalanced = result.getKey();
        double originalMu = result.getValue();
        if (isNumBalanced && originalMu - mu <= model.minBalanceFactor * 1.5) {
            return new Solution(true, originalPlace, originalMu, "IdempotentCompare");
        } else {
            return model.greedySolve();
        }
    }

    public static Pair<Boolean, Double> caculateBalanceFactor(int M, int N, int place[], double partitionSize[]) {
        double[] dataSizes = new double[M];
        int[] dataNums = new int[M];
        int minNum = Math.floorDiv(N, M);
        int maxNum = -Math.floorDiv(-N, M);
        for (int i = 0; i < N; i++) {
            int index = place[i];
            dataSizes[index] += partitionSize[i];
            dataNums[index] += 1;
        }
        double avgSize = Arrays.stream(dataSizes).sum() / M;
        double maxSize = Arrays.stream(dataSizes).max().getAsDouble();
        double minSize = Arrays.stream(dataSizes).min().getAsDouble();
        double factor = (avgSize < 1e-5) ? 1 : Math.max(maxSize - avgSize, avgSize - minSize) / avgSize;
        boolean numBalanaced = Arrays.stream(dataNums).noneMatch(o -> {
            return o < minNum || o > maxNum;
        });
        return Pair.of(numBalanaced, factor);
    }

    public static Pair<Boolean, Double> caculateBalanceFactor(int M, int N, int place[], int selectedIndexes[],
                                                              double partitionSize[]) {
        Map<Integer, Integer> reindexedReverseMap = IntStream.range(0, M)
            .boxed()
            .collect(Collectors.toMap(i -> selectedIndexes[i], i -> i));
        int[] reindexedPlace = Arrays.stream(place).map(i -> reindexedReverseMap.get(i)).toArray();
        return caculateBalanceFactor(M, N, reindexedPlace, partitionSize);

    }

    public static Pair<Boolean, Double> caculateBalanceFactor(int M, int N, int place[], double partitionSize[],
                                                              int[] drainNodeIndexes) {
        int resM = M - drainNodeIndexes.length;
        double[] dataSizes = new double[M];
        int[] dataNums = new int[M];
        int minNum = Math.floorDiv(N, resM);
        int maxNum = -Math.floorDiv(-N, resM);
        for (int i = 0; i < N; i++) {
            int index = place[i];
            dataSizes[index] += partitionSize[i];
            dataNums[index] += 1;
        }
        double avgSize = Arrays.stream(dataSizes).sum() / resM;
        double maxSize = Arrays.stream(dataSizes).max().getAsDouble();
        double minSize = Arrays.stream(dataSizes).min().getAsDouble();
        double factor = (avgSize < 1e-5) ? 1 : Math.max(maxSize - avgSize, avgSize - minSize) / avgSize;
        boolean numBalanaced = true;
        Set<Integer> drainNodeIndexSet = Arrays.stream(drainNodeIndexes).boxed().collect(Collectors.toSet());
        for (int i = 0; i < M; i++) {
            if (drainNodeIndexSet.contains(i) && dataNums[i] != 0) {
                numBalanaced = false;
            } else if (dataNums[i] < minNum || dataNums[i] > maxNum) {
                numBalanaced = false;
            }
        }
        return Pair.of(numBalanaced, factor);
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize) {
        return solveMovePartition(m, n, originalPlace, partitionSize, SolveLevel.BALANCE_DEFAULT);
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                              int[] drainIndex) {
        return solveMovePartition(m, n, originalPlace, partitionSize, SolveLevel.BALANCE_DEFAULT, drainIndex);
    }

    public static Solution solveShufflePartition(int m, int n, int[] originalPlace, double[] partitionSize) {
        int[] targetPlace = new int[n];
        Random random = new Random((System.nanoTime()));
        for (int i = 0; i < n; i++) {
            targetPlace[i] = random.nextInt(m);
        }
        double mu = caculateBalanceFactor(m, n, targetPlace, partitionSize).getValue();
        return new Solution(true, targetPlace, mu, "ShufflePartition");
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                              int[] selectedIndexes, SolveLevel solveLevel, int[] drainNodeIndexes) {
        Map<Integer, Integer> reindexedMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> i, i -> selectedIndexes[i]));
        Map<Integer, Integer> reindexedReverseMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> selectedIndexes[i], i -> i));
        int[] reindexedOriginalPlace = Arrays.stream(originalPlace).map(i -> reindexedReverseMap.get(i)).toArray();
        int[] reindexedDrainNodeIndexes =
            Arrays.stream(drainNodeIndexes).map(i -> reindexedReverseMap.get(i)).toArray();
        Solution solution = solveMovePartition(m, n, reindexedOriginalPlace, partitionSize, reindexedDrainNodeIndexes);
        if (solution.withValidSolve) {
            solution.targetPlace = Arrays.stream(solution.targetPlace).map(i -> reindexedMap.get(i)).toArray();
        }
        return solution;
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                              int[] selectedIndex, SolveLevel solveLevel) {
        Map<Integer, Integer> reindexedMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> i, i -> selectedIndex[i]));
        Map<Integer, Integer> reindexedReverseMap = IntStream.range(0, m)
            .boxed()
            .collect(Collectors.toMap(i -> selectedIndex[i], i -> i));
        int[] reindexedOriginalPlace = Arrays.stream(originalPlace).map(i -> reindexedReverseMap.get(i)).toArray();
        Solution solution = solveMovePartition(m, n, reindexedOriginalPlace, partitionSize, solveLevel);
        if (solution.withValidSolve) {
            solution.targetPlace = Arrays.stream(solution.targetPlace).map(i -> reindexedMap.get(i)).toArray();
        }
        return solution;
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                              SolveLevel solveLevel) {
        MixedModel model = new MixedModel();
        model.buildGreedyModel(m, n, originalPlace, partitionSize);
        double mu = model.getBalanaceFactor();
        if (solveLevel == SolveLevel.MIN_COST) {
            Pair<Boolean, Double> result = caculateBalanceFactor(m, n, originalPlace, partitionSize);
            Boolean isNumBalanced = result.getKey();
            double originalMu = result.getValue();
            if (isNumBalanced && originalMu - mu <= model.minBalanceFactor * 1.5) {
                return new Solution(true, originalPlace, originalMu, "IdempotentCompare");
            }
        }
        if (!checkNativeOptimizationSupport()) {
            return model.greedySolve();
        }
        if (m > model.maxNodeNum || n > model.maxPartitionNum) {
            return model.greedySolve();
        } else if (mu > model.maxBalanceFactor) {
            return model.greedySolve();
        } else if (mu < model.minBalanceFactor) {
            mu = Math.max(mu, model.minBalanceFactor);
        }
        Solution solution = new Solution(false);
        double stepGap;
        for (double modifiedMu = mu; modifiedMu >= model.middleBalanceFactor; modifiedMu -= stepGap) {
            model.buildLpModel(m, n, originalPlace, partitionSize, modifiedMu);
            Solution tempSolution = model.lpSolve();
            model.closeLpModel();
            if (!tempSolution.withValidSolve) {
                break;
            } else {
                solution = tempSolution;
            }
            stepGap = (modifiedMu > 0.5) ? 0.1 : 0.05;
        }
        if (!solution.withValidSolve) {
            if (!StringUtils.equals("LinearProgram", solution.strategy)) {
                model.buildLpModel(m, n, originalPlace, partitionSize, mu);
                solution = model.lpSolve();
            } else {
                solution = model.greedySolve();
            }
        }
        return solution;
    }

    public static Solution solveMovePartition(int m, int n, int[] originalPlace, double[] partitionSize,
                                              SolveLevel solveLevel, int[] drainNodeIndexes) {
        MixedModel model = new MixedModel();
        model.buildGreedyModel(m, n, originalPlace, partitionSize, drainNodeIndexes);
        double mu = model.getBalanaceFactor();
        if (solveLevel == SolveLevel.MIN_COST) {
            Pair<Boolean, Double> result = caculateBalanceFactor(m, n, originalPlace, partitionSize, drainNodeIndexes);
            Boolean isNumBalanced = result.getKey();
            double originalMu = result.getValue();
            if (isNumBalanced && originalMu - mu <= model.minBalanceFactor * 1.5) {
                return new Solution(true, originalPlace, originalMu, "IdempotentCompare");
            }
        }
        if (!checkNativeOptimizationSupport()) {
            return model.greedySolve();
        }
        if (m > model.maxNodeNum || n > model.maxPartitionNum) {
            return model.greedySolve();
        } else if (mu > model.maxBalanceFactor) {
            return model.greedySolve();
        } else if (mu < model.minBalanceFactor) {
            mu = Math.max(mu, model.minBalanceFactor);
        }
        Solution solution = new Solution(false);
        double stepGap;
        for (double modifiedMu = mu; modifiedMu >= model.middleBalanceFactor; modifiedMu -= stepGap) {
            model.buildLpModel(m, n, originalPlace, partitionSize, modifiedMu, drainNodeIndexes);
            Solution tempSolution = model.lpSolve();
            model.closeLpModel();
            if (!tempSolution.withValidSolve) {
                break;
            } else {
                solution = tempSolution;
            }
            stepGap = (modifiedMu > 0.5) ? 0.1 : 0.05;
        }
        if (!solution.withValidSolve) {
            if (!StringUtils.equals("LinearProgram", solution.strategy)) {
                model.buildLpModel(m, n, originalPlace, partitionSize, mu, drainNodeIndexes);
                solution = model.lpSolve();
            } else {
                solution = model.greedySolve();
            }
        }
        return solution;
    }
}
