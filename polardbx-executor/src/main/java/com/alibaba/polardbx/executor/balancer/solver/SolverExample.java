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

import java.io.InputStream;
import java.util.Scanner;

public class SolverExample {
    public int M;

    public int N;
    public int[] originalPlace;
    public double[] partitionSize;

    public SolverExample(int M, int N, int[] originalPlace, double[] partitionSize) {
        this.M = M;
        this.N = N;
        this.originalPlace = originalPlace;
        this.partitionSize = partitionSize;
    }

    public static SolverExample generate102432SolverExample() {
        return generateSolverExampleFromFileName("1024_32.txt");
    }

    public static SolverExample generate102416SolverExample() {
        return generateSolverExampleFromFileName("1024_16.txt");
    }

    public static SolverExample generateSolverExampleFromFileName(String fileName) {
        String resourceDir = "solver/" + fileName;
        try (InputStream file = SolverExample.class.getClassLoader().getResourceAsStream(resourceDir);
            Scanner scanner = new Scanner(file)) {
            int N = scanner.nextInt();
            int M = scanner.nextInt();
            int[] originalPlace = new int[N];
            double[] weight = new double[N];
            for (int i = 0; i < N; i++) {
                originalPlace[i] = scanner.nextInt();
            }
            for (int i = 0; i < N; i++) {
                weight[i] = scanner.nextInt();
            }
            return new SolverExample(M, N, originalPlace, weight);
        } catch (Exception e) {
            return new SolverExample(0, 0, new int[0], new double[0]);
        }
    }
}
