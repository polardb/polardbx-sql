package com.alibaba.polardbx.executor.balancer;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import static com.alibaba.polardbx.executor.balancer.TestBalanceSolver.testGreedySolver;
import static com.alibaba.polardbx.executor.balancer.TestBalanceSolver.testGreedySolverDrainNode;
import static com.alibaba.polardbx.executor.balancer.TestBalanceSolver.testMixedSolver;
import static com.alibaba.polardbx.executor.balancer.TestBalanceSolver.testSolverFromFile;

/**
 * @author jinkun.taojinkun
 * @since 2022/08
 */
public class TestBalanceSolverOnlineCase {
    public static void testSolverFromTextFile(String sampleName, String solverType) throws FileNotFoundException {
        String resourceDir = String.format("com/alibaba/polardbx/executor/balancer/%s.sample.txt", sampleName);
        String fileDir = TestBalanceSolver.class.getClassLoader().getResource(resourceDir).getPath();
        double moveRate = 1.0;
        double overRate = 1.0;
        try (BufferedReader br = new BufferedReader(new FileReader(fileDir))) {
            StringBuilder content = new StringBuilder();
            String line;

            while ((line = br.readLine()) != null) {
                content.append(line);
            }

            String data = content.toString().replace("\n", " ");

            // 提取M的值
            int mIndexStart = data.indexOf("M=") + 2;
            int mIndexEnd = data.indexOf(", N=");
            int M = Integer.parseInt(data.substring(mIndexStart, mIndexEnd));

            // 提取N的值
            int nIndexStart = data.indexOf("N=") + 2;
            int nIndexEnd = data.indexOf(", originalPlace=[");
            int N = Integer.parseInt(data.substring(nIndexStart, nIndexEnd));

            // 提取originalPlace的值
            int originalPlaceIndexStart = data.indexOf("originalPlace=[") + 15;
            int originalPlaceIndexEnd = data.indexOf("]", originalPlaceIndexStart);
            String originalPlaceString = data.substring(originalPlaceIndexStart, originalPlaceIndexEnd);
            int[] originalPlace = new int[N];
            String[] originalPlaceStrings = originalPlaceString.split(",");
            for (int i = 0; i < N; i++) {
                originalPlace[i] = Integer.parseInt(originalPlaceStrings[i].trim());
            }

            // 提取partitionSize的值，假设targetPlace为partitionSize，就用上述方法提取partitionSize：
            int partitionSizeIndexStart = data.indexOf("partitionSize=[") + 15;
            int partitionSizeIndexEnd = data.indexOf("]", partitionSizeIndexStart);
            String partitionSizeString = data.substring(partitionSizeIndexStart, partitionSizeIndexEnd);
            double[] weight = new double[N];
            String[] weightStrings = partitionSizeString.split(",");
            for (int i = 0; i < N; i++) {
                weight[i] = Double.parseDouble(weightStrings[i].trim());
            }
            if (solverType.equals("greedy")) {
                testGreedySolver(M, N, originalPlace, weight, moveRate, overRate);
            } else if (solverType.equals("mixed")) {
                testMixedSolver(M, N, originalPlace, weight, moveRate, overRate);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMixedSolver128_15_mixed_ok_nft_tg25() throws FileNotFoundException {
        String sampleName = "rebalance_128_15_mixed_ok_nft_tg25";
        testSolverFromTextFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolver128_8_mixed_ok_nft2_tg25() throws FileNotFoundException {
        String sampleName = "rebalance_128_8_mixed_ok_nft2_tg25";
        testSolverFromTextFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolver256_15_mixed_ok_nft3_tg16() throws FileNotFoundException {
        String sampleName = "rebalance_256_15_mixed_ok_nft3_tg16";
        testSolverFromTextFile(sampleName, "mixed");
    }

    @Test
    public void testMixedSolver256_12_mixed_ok_nft4_tg16() throws FileNotFoundException {
        String sampleName = "rebalance_256_12_mixed_ok_nft4_tg16";
        testSolverFromTextFile(sampleName, "mixed");
    }
}