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

import java.util.List;

public class MatchModel {
    public LpSolver lpSolver = new LpSolver();

    int N;

    public void buildLpModel(int N, double[][] matchCost) {
        // create the variables (and set up the objective coefficients)
        // X: N * M matrix,
        // Weight: N * 1 vector
        this.N = N;
        LpVariable[][] vars = new LpVariable[N][N];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                vars[i][j] = new LpVariable()
                    .lb(0.0)
                    .ub(1.0)
                    .isInteger((char) 1)
                    .name("x_" + i + "_" + j)
                    .objective(matchCost[i][j]);
                lpSolver.addVariable(vars[i][j]);
            }
        }

        // constraint: every slot must be filled exactly once
        for (int i = 0; i < N; i++) {
            LpExpression expr1 = new LpExpression();
            LpExpression expr2 = new LpExpression();
            for (int j = 0; j < N; j++) {
                expr1.add(vars[i][j]);
                expr2.add(vars[j][i]);
            }
            expr1.eq(1.0).name("SumNumOnPart" + i);
            expr2.eq(1.0).name("SumNumOnDn" + i);
            lpSolver.addExpression(expr1);
            lpSolver.addExpression(expr2);
        }
    }

    public int solve() {
        return lpSolver.solve();
    }

    public int[] buildSolution() {
        List<Double> solution = lpSolver.getSolution();
        int[] targetPlace = new int[N];
        int index = 0;
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                double value = solution.get(index);
                if (Math.abs(1 - value) < 1e-3) {
                    targetPlace[i] = j;
                }
                index++;
            }
        }
        return targetPlace;
    }

    public void close() {
        lpSolver.close();
    }

    public static int[] solve(int N, double[][] matchCost) {
        MatchModel matchModel = new MatchModel();
        matchModel.buildLpModel(N, matchCost);
        matchModel.solve();
        int[] solution = matchModel.buildSolution();
        matchModel.close();
        return solution;
    }
}
