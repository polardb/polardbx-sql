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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SequentialPlaceModel {
    int M;

    int N;

    int resM;

    int solverTime = 60;

    int maxRetryTime = 3;

    double minGapPercent = 0.1;

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

    public void setVariable(int M, int N, int[] X, double Weight[]) {
        // create the variables (and set up the objective coefficients)
        // X: M * M matrix,
        // Weight: N * 1 vector

        this.N = N;
        this.M = M;
        this.resM = M;
        int avgPartNum = N / M;
        LpVariable[][] vars = new LpVariable[M][M];
        for (int j = 0; j < M; j++) {
            for (int i = 0; i < M; i++) {
                double weight = 0;
                for (int k = 0; k < avgPartNum; k++) {
                    int partIndex = i * avgPartNum + k;
                    int moveScale = (X[partIndex] == j) ? 0 : 1;
                    weight += (Weight[partIndex] + 1) * moveScale;
                }
                vars[i][j] = new LpVariable()
                    .lb(0.0)
                    .ub(1.0)
                    .isInteger((char) 1)
                    .name("x_" + i + "_" + j)
                    .objective((weight + 1));
                lpSolver.addVariable(vars[i][j]);
            }
        }
        // constraint: every slot must be filled exactly once
        for (int i = 0; i < M; i++) {
            LpExpression expr1 = new LpExpression();
            for (int j = 0; j < M; j++) {
                expr1.add(vars[i][j]);
            }
            expr1.eq(1.0).name("SumNum" + i);
            lpSolver.addExpression(expr1);
        }
        for (int j = 0; j < M; j++) {
            LpExpression expr2 = new LpExpression();
            for (int i = 0; i < M; i++) {
                expr2.add(vars[i][j]);
            }
            expr2.eq(1.0).name("SumNum" + j);
            lpSolver.addExpression(expr2);
        }
        if (N <= 256 && M <= 16) {
            this.solverTime = 30;
        } else if (N <= 2048 && N >= 1024 && M <= 32) {
            this.solverTime = 120;
        } else {
            this.solverTime = 60;
        }
    }

    public void setVariable(int M, int N, int[] X, double Weight[], int[] drainNodeIndexes) {
        // create the variables (and set up the objective coefficients)
        // X: N * M matrix,
        // Weight: N * 1 vector
        this.N = N;
        this.M = M;
        this.resM = M - drainNodeIndexes.length;
        int avgPartNum = N / resM;
        Set<Integer> drainNodeIndexSet = Arrays.stream(drainNodeIndexes).boxed().collect(Collectors.toSet());
        LpVariable[][] vars = new LpVariable[resM][M];
        for (int j = 0; j < M; j++) {
            for (int i = 0; i < resM; i++) {
                double weight = 0;
                for (int k = 0; k < avgPartNum; k++) {
                    int partIndex = i * avgPartNum + k;
                    int moveScale = (X[partIndex] == j) ? 0 : 1;
                    weight += (Weight[partIndex] + 1) * moveScale;
                }
                vars[i][j] = new LpVariable()
                    .lb(0.0)
                    .ub(1.0)
                    .isInteger((char) 1)
                    .name("x_" + i + "_" + j)
                    .objective((weight + 1));
                lpSolver.addVariable(vars[i][j]);
            }
        }
        // constraint: every slot must be filled exactly once
        for (int i = 0; i < resM; i++) {
            LpExpression expr1 = new LpExpression();
            for (int j = 0; j < M; j++) {
                expr1.add(vars[i][j]);
            }
            expr1.eq(1.0).name("SumNum" + i);
            lpSolver.addExpression(expr1);
        }
        for (int j = 0; j < M; j++) {
            LpExpression expr2 = new LpExpression();
            if (!drainNodeIndexSet.contains(j)) {
                for (int i = 0; i < resM; i++) {
                    expr2.add(vars[i][j]);
                }
                expr2.eq(1.0).name("SumPartNum" + j);
                lpSolver.addExpression(expr2);
            }
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
        double mu = 0;
        if (status == 1) {
            int avgPartNum = N / resM;
            List<Double> solution = model.getSolution();
            int results[][] = new int[N][M];
            int targetPlace[] = new int[N];
            int index = 0;
            for (int j = 0; j < M; j++) {
                for (int i = 0; i < resM; i++) {
                    double value = solution.get(index);
                    if (Math.abs(1 - value) < 1e-3) {
                        for (int k = i * avgPartNum; k < (i + 1) * avgPartNum; k++) {
                            results[k][j] = 1;
                            targetPlace[k] = j;
                        }
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

    public static int[] solve(int M, int N, int[] X, double[] Weight) {
        SequentialPlaceModel sequentialPlaceModel = new SequentialPlaceModel();
        sequentialPlaceModel.setVariable(M, N, X, Weight);
        Solution solution = sequentialPlaceModel.lpSolveWithRetry();
        return solution.targetPlace;
    }

    public static int[] solve(int M, int N, int[] X, double[] Weight, int[] drainNodeIndex) {
        SequentialPlaceModel sequentialPlaceModel = new SequentialPlaceModel();
        sequentialPlaceModel.setVariable(M, N, X, Weight, drainNodeIndex);
        Solution solution = sequentialPlaceModel.lpSolveWithRetry();
        return solution.targetPlace;
    }
}
