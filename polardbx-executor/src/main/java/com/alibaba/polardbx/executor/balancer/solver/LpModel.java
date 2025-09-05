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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LpModel {
    int M;

    int N;

    int solverTime = 60;

    double minGapPercent = 20;

    int maxRetryTime = 3;

    double mu;

    public static final int BEFORE_BRANCH_AND_CUT = -1;
    public static final int OPTIMAL = 0;
    public static final int INFEASIBLE = 1;
    public static final int STOP_ON_GAP = 2;
    public static final int STOP_ON_NODE = 3;
    public static final int STOP_ON_TIME = 4;
    public static final int STOP_ON_EVENTS = 5;
    public static final int STOP_ON_SOLUTIONS = 6;
    public static final int RELAX_UNBOUND = 7;
    public static final int STOP_ON_ITERATION_LIMIT = 8;

    LpSolver lpSolver = new LpSolver();

    public LpModel() {

    }

    public void setVariable(int M, int N, int[] X, double Weight[], double lamda1, double lamda2) {
        // create the variables (and set up the objective coefficients)
        // X: N * M matrix,
        // Weight: N * 1 vector
        this.mu = 1 - lamda1;
        this.N = N;
        this.M = M;
        LpVariable[][] vars = new LpVariable[N][M];
        double avgWeight = Arrays.stream(Weight).sum() / M;
        int minN = (int) Math.floor(N / (M * 1.0));
        int maxN = (int) Math.ceil(N / (M * 1.0));
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                int moveScale = (X[i] == j) ? 0 : 1;
                vars[i][j] = new LpVariable()
                    .lb(0.0)
                    .ub(1.0)
                    .isInteger((char) 1)
                    .name("x_" + i + "_" + j)
                    .objective((Weight[i] + 1) * moveScale);
                lpSolver.addVariable(vars[i][j]);
            }
        }
        // constraint: every subject must be assigned exactly once
        for (int j = 0; j < M; j++) {
            LpExpression wMin = new LpExpression();
            LpExpression wMax = new LpExpression();
            LpExpression nMin = new LpExpression();
            LpExpression nMax = new LpExpression();
            for (int i = 0; i < N; i++) {
                wMin.add(Weight[i], vars[i][j]);
                wMax.add(Weight[i], vars[i][j]);
                nMin.add(vars[i][j]);
                nMax.add(vars[i][j]);
            }
            wMin.geq(lamda1 * avgWeight).name(String.format("wMin_%d", j));
            wMax.leq(lamda2 * avgWeight).name(String.format("wMax_%d", j));
            nMin.geq(minN).name(String.format("nMin_%d", j));
            nMax.leq(maxN).name(String.format("nMax_%d", j));
            lpSolver.addExpression(wMin);
            lpSolver.addExpression(wMax);
            lpSolver.addExpression(nMin);
            lpSolver.addExpression(nMax);
        }
        // constraint: every slot must be filled exactly once
        for (int i = 0; i < N; i++) {
            LpExpression expr = new LpExpression();
            for (int j = 0; j < M; j++) {
                expr.add(vars[i][j]);
            }
            expr.eq(1.0).name("SumNum" + i);
            lpSolver.addExpression(expr);
        }
        if (N <= 256 && M <= 16) {
            this.solverTime = 30;
        } else if (N <= 2048 && N >= 1024 && M <= 32) {
            this.solverTime = 120;
        } else {
            this.solverTime = 60;
        }
    }

//    public void setVariableForMergeMap(int M, int N, int[] X, double Weight[], double lamda1, double lamda2,
//                                       TreeMap<String, Set<Integer>> mergeMap, int[] storedPartitionNum,
//                                       double[] storedPartitionSize) {
//        this.mu = 1 - lamda1;
//        this.M = M;
//        List<String> keys = mergeMap.keySet().stream().collect(Collectors.toList());
//        Map<Integer, String> indexKeyMap = new HashMap<>();
//        for (int i = 0; i < keys.size(); i++) {
//            String key = keys.get(i);
//            for (Integer index : mergeMap.get(key)) {
//                indexKeyMap.put(index, key);
//            }
//        }
//        this.N = keys.size();
//        int keyNum = keys.size();
//        LpVariable[][] vars = new LpVariable[keyNum][M];
//        double avgWeight = Arrays.stream(Weight).sum() / M;
//        avgWeight += Arrays.stream(storedPartitionSize).sum() / M;
//        int storedN = Arrays.stream(storedPartitionNum).sum();
//        int minN = (int) Math.floor((keys.size() + storedN) / (M * 1.0));
//        int maxN = (int) Math.ceil((keys.size() + storedN) / (M * 1.0));
//
//        for (int i = 0; i < keyNum; i++) {
//            for (int j = 0; j < M; j++) {
//                String key = keys.get(i);
//                double scale = 0;
//                for (Integer index : mergeMap.get(key)) {
//                    int moveScale = (X[index] == j) ? 0 : 1;
//                    scale += (Weight[index] + 1) * moveScale;
//                }
//                vars[i][j] = new LpVariable()
//                    .lb(0.0)
//                    .ub(1.0)
//                    .isInteger((char) 1)
//                    .name("x_" + i + "_" + j)
//                    .objective(scale);
//                lpSolver.addVariable(vars[i][j]);
//            }
//        }
//
//        // constraint: every subject must be assigned exactly once
//        for (int j = 0; j < M; j++) {
//            LpExpression wMin = new LpExpression();
//            LpExpression wMax = new LpExpression();
//            LpExpression nMin = new LpExpression();
//            LpExpression nMax = new LpExpression();
//            for (int k = 0; k < keys.size(); k++) {
//                String key = keys.get(k);
//                nMin.add(vars[k][j]);
//                nMax.add(vars[k][j]);
//                double weight = 0;
//                for (Integer index : mergeMap.get(key)) {
//                    weight += Weight[index];
//                }
//                wMax.add(weight, vars[k][j]);
//                wMin.add(weight, vars[k][j]);
//            }
//            wMin.add(storedPartitionSize[j]);
//            wMax.add(storedPartitionSize[j]);
//            nMin.add(storedPartitionNum[j]);
//            nMax.add(storedPartitionNum[j]);
//            wMin.geq(lamda1 * avgWeight).name(String.format("wMin_%d", j));
//            wMax.leq(lamda2 * avgWeight).name(String.format("wMax_%d", j));
//            nMin.geq(minN).name(String.format("nMin_%d", j));
//            nMax.leq(maxN).name(String.format("nMax_%d", j));
//            lpSolver.addExpression(wMin);
//            lpSolver.addExpression(wMax);
//            lpSolver.addExpression(nMin);
//            lpSolver.addExpression(nMax);
//        }
//        // constraint: every slot must be filled exactly once
//        for (int i = 0; i < keyNum; i++) {
//            LpExpression expr = new LpExpression();
//            for (int j = 0; j < M; j++) {
//                expr.add(vars[i][j]);
//            }
//            expr.eq(1.0).name("SumNum" + i);
//            lpSolver.addExpression(expr);
//        }
//        if (N <= 256 && M <= 16) {
//            this.solverTime = 30;
//        } else if (N <= 2048 && N >= 1024 && M <= 32) {
//            this.solverTime = 120;
//        } else {
//            this.solverTime = 180;
//        }
//
//    }

//    public void setVariable(int M, int N, int[] X, double Weight[], double lamda1, double lamda2,
//                            TreeMap<String, Set<Integer>> mergeMap, int[] storedPartitionNum,
//                            double[] storedPartitionSize) {
//        // create the variables (and set up the objective coefficients)
//        // X: N * M matrix,
//        // Weight: N * 1 vector
//        this.mu = 1 - lamda1;
//        this.N = N;
//        this.M = M;
//        List<String> keys = mergeMap.keySet().stream().collect(Collectors.toList());
//        Map<Integer, String> indexKeyMap = new HashMap<>();
//        for (int i = 0; i < keys.size(); i++) {
//            String key = keys.get(i);
//            for (Integer index : mergeMap.get(key)) {
//                indexKeyMap.put(index, key);
//            }
//        }
//        if (this.N != keys.size()) {
//            setVariableForMergeMap(M, N, X, Weight, lamda1, lamda2, mergeMap, storedPartitionNum, storedPartitionSize);
//            return;
//        }
//        LpVariable[][] vars = new LpVariable[N][M];
//        double avgWeight = Arrays.stream(Weight).sum() / M;
//        avgWeight += Arrays.stream(storedPartitionSize).sum() / M;
//        int storedN = Arrays.stream(storedPartitionNum).sum();
//        int minN = (int) Math.floor((keys.size() + storedN) / (M * 1.0));
//        int maxN = (int) Math.ceil((keys.size() + storedN) / (M * 1.0));
//        for (int i = 0; i < N; i++) {
//            for (int j = 0; j < M; j++) {
//                int moveScale = (X[i] == j) ? 0 : 1;
//                vars[i][j] = new LpVariable()
//                    .lb(0.0)
//                    .ub(1.0)
//                    .isInteger((char) 1)
//                    .name("x_" + i + "_" + j)
//                    .objective((Weight[i] + 1) * moveScale);
//                lpSolver.addVariable(vars[i][j]);
//            }
//        }
//        // constraint: every subject must be assigned exactly once
//        for (int j = 0; j < M; j++) {
//            LpExpression wMin = new LpExpression();
//            LpExpression wMax = new LpExpression();
//            LpExpression nMin = new LpExpression();
//            LpExpression nMax = new LpExpression();
//            for (int i = 0; i < N; i++) {
//                wMin.add(Weight[i], vars[i][j]);
//                wMax.add(Weight[i], vars[i][j]);
//                nMin.add(vars[i][j]);
//                nMax.add(vars[i][j]);
//            }
//            wMin.add(storedPartitionSize[j]);
//            wMax.add(storedPartitionSize[j]);
//            wMin.geq(lamda1 * avgWeight).name(String.format("wMin_%d", j));
//            wMax.leq(lamda2 * avgWeight).name(String.format("wMax_%d", j));
//            nMin.add(storedPartitionNum[j]);
//            nMax.add(storedPartitionNum[j]);
//            nMin.geq(minN).name(String.format("nMin_%d", j));
//            nMax.leq(maxN).name(String.format("nMax_%d", j));
//            lpSolver.addExpression(wMin);
//            lpSolver.addExpression(wMax);
//            lpSolver.addExpression(nMin);
//            lpSolver.addExpression(nMax);
//        }
//        // constraint: every slot must be filled exactly once
//        for (int i = 0; i < N; i++) {
//            LpExpression expr = new LpExpression();
//            for (int j = 0; j < M; j++) {
//                expr.add(vars[i][j]);
//            }
//            expr.eq(1.0).name("SumNum" + i);
//            lpSolver.addExpression(expr);
//        }
//        if (N <= 256 && M <= 16) {
//            this.solverTime = 30;
//        } else if (N <= 2048 && N >= 1024 && M <= 32) {
//            this.solverTime = 120;
//        } else {
//            this.solverTime = 180;
//        }
//    }

    public void setVariable(int M, int N, int[] X, double Weight[], double lamda1, double lamda2,
                            int[] drainNodeIndexes) {
        // create the variables (and set up the objective coefficients)
        // X: N * M matrix,
        // Weight: N * 1 vector
        this.mu = 1 - lamda1;
        this.N = N;
        this.M = M;
        int resM = M - drainNodeIndexes.length;
        Set<Integer> drainNodeIndexSet = Arrays.stream(drainNodeIndexes).boxed().collect(Collectors.toSet());
        LpVariable[][] vars = new LpVariable[N][M];
        double avgWeight = Arrays.stream(Weight).sum() / resM;
        int minN = (int) Math.floor(N / (resM * 1.0));
        int maxN = (int) Math.ceil(N / (resM * 1.0));
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                int moveScale = (X[i] == j) ? 0 : 1;
                vars[i][j] = new LpVariable()
                    .lb(0.0)
                    .ub(1.0)
                    .isInteger((char) 1)
                    .name("x_" + i + "_" + j)
                    .objective((Weight[i] + 1) * moveScale);
                lpSolver.addVariable(vars[i][j]);
            }
        }
        // constraint: every subject must be assigned exactly once
        for (int j = 0; j < M; j++) {
            LpExpression wMin = new LpExpression();
            LpExpression wMax = new LpExpression();
            LpExpression nMin = new LpExpression();
            LpExpression nMax = new LpExpression();
            for (int i = 0; i < N; i++) {
                wMin.add(Weight[i], vars[i][j]);
                wMax.add(Weight[i], vars[i][j]);
                nMin.add(vars[i][j]);
                nMax.add(vars[i][j]);
            }
            if (drainNodeIndexSet.contains(j)) {
                nMin.geq(0).name(String.format("nMin_%d", j));
                nMax.leq(0).name(String.format("nMax_%d", j));
                lpSolver.addExpression(nMin);
                lpSolver.addExpression(nMax);
            } else {
                wMin.geq(lamda1 * avgWeight).name(String.format("wMin_%d", j));
                wMax.leq(lamda2 * avgWeight).name(String.format("wMax_%d", j));
                nMin.geq(minN).name(String.format("nMin_%d", j));
                nMax.leq(maxN).name(String.format("nMax_%d", j));
                lpSolver.addExpression(wMin);
                lpSolver.addExpression(wMax);
                lpSolver.addExpression(nMin);
                lpSolver.addExpression(nMax);
            }
        }
        // constraint: every slot must be filled exactly once
        for (int i = 0; i < N; i++) {
            LpExpression expr = new LpExpression();
            for (int j = 0; j < M; j++) {
                expr.add(vars[i][j]);
            }
            expr.eq(1.0).name("SumNum" + i);
            lpSolver.addExpression(expr);
        }
        if (N <= 256 && M <= 16) {
            this.solverTime = 30;
        } else if (N <= 2048 && N >= 1024 && M <= 32) {
            this.solverTime = 120;
        } else {
            this.solverTime = 60;
        }
    }

    public Solution lpSolveWithRetry() {
        int status = 0;
        for (int i = 0; i < maxRetryTime; i++) {
            lpSolve();
            switch (lpSolver.getStatus()) {
            case BEFORE_BRANCH_AND_CUT:
                solverTime *= 2;
                status = 0;
                break;
            case STOP_ON_TIME:
                solverTime *= 2;
                status = 0;
                break;
            case INFEASIBLE:
                status = -1;
                break;
            case OPTIMAL:
            case STOP_ON_GAP:
                status = 1;
                break;
            default:
                status = -1;
                break;
            }
            if (status != 0) {
                return buildLpSolution(lpSolver, status);
            }
        }
        return buildLpSolution(lpSolver, status);
    }

    public void lpSolve() {
        lpSolver.setMaximumTime(solverTime);
        lpSolver.setGapPercent(minGapPercent);
        lpSolver.solve();
    }

    public Solution buildLpSolution(LpSolver model, int status) {
        if (status == 1) {
            List<Double> solution = model.getSolution();
            int results[][] = new int[N][M];
            int targetPlace[] = new int[N];
            int index = 0;
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    double value = solution.get(index);
                    if (Math.abs(1 - value) < 1e-3) {
                        results[i][j] = 1;
                        targetPlace[i] = j;
                    }
                    index++;
                }
            }
            return new Solution(true, results, targetPlace, mu, "LinearProgram");
        } else {
            return new Solution(false, mu, "LinearProgram");
        }
    }

    public void close() {
        lpSolver.close();
    }
}
