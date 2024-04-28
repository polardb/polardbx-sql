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

import org.apache.calcite.util.Pair;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SplitModel {
    List<SolverUtils.PartitionSet> partitionSets;

    List<SolverUtils.PartitionSet> targetPartitionSets;

    double mu;

    int M;

    int N;

    double[] Weight;

    int[] X;

    PartitionSplitInfo partitionSplitInfo;

    double maxMu = 0.2;

    void fromGreedyModel(GreedyModel greedyModel) {
        this.partitionSets = Arrays.asList(greedyModel.partitionSets.clone());
        this.targetPartitionSets = Arrays.asList(greedyModel.targetPartitionSets.clone());
        this.M = greedyModel.M;
        this.N = greedyModel.N;
        this.Weight = greedyModel.Weight;
        this.X = greedyModel.X;
        this.mu = greedyModel.mu;
    }

    public void setPartitionSplitPlan(PartitionSplitInfo partitionSplitInfo) {
        this.partitionSplitInfo = partitionSplitInfo;
    }

    public static Solution buildSplitSolution(Boolean flag) {
        return new Solution(false);
    }

    public static Solution buildSplitSolution(int n, PartitionSplitInfo partitionSplitInfo) {
        Solution solution = new Solution(true);
        solution.setWithSplitPartition(true);
        return solution;
    }

    public Solution splitSolve() {
        Boolean result = buildSplitPartitionInfo(partitionSplitInfo, mu);
        if (result) {
            return buildSplitSolution(N, partitionSplitInfo);
        } else {
            return buildSplitSolution(false);
        }
    }

    public Boolean buildSplitPartitionInfo(
        PartitionSplitInfo partitionSplitInfo, double mu) {
        targetPartitionSets = targetPartitionSets.stream()
            .sorted(Comparator.comparingDouble(SolverUtils.PartitionSet::getSumWeight).reversed()).collect(
                Collectors.toList());
        double avgTotalWeight = Arrays.stream(Weight).sum() / M;
        double thresholdFactor = 1 + mu * 0.5;
        // 1. find split partitions.
        List<SolverUtils.ToSplitPartition> splitPartitions = new ArrayList<>();
        SolverUtils.Fetcher fetcher = object -> ((Pair<Integer, Double>) object).getValue();
        for (int i = 0; i < M; i++) {
            SolverUtils.PartitionSet partitionSet = targetPartitionSets.get(i);
            if (partitionSet.sumWeight > avgTotalWeight * thresholdFactor) {
                int index = SolverUtils.getFirstLargerThan(partitionSet.weightSet, avgTotalWeight * mu, fetcher);
                double overWeight = partitionSet.sumWeight - avgTotalWeight;
                SolverUtils.ToSplitPartition
                    partition = new SolverUtils.ToSplitPartition(partitionSet.weightSet.get(index), overWeight);
                splitPartitions.add(partition);
            }
        }
        splitPartitions.sort(Comparator.comparingDouble(SolverUtils.ToSplitPartition::getSplitSize));

        List<Double> lessThanGap = targetPartitionSets.stream().map(o -> avgTotalWeight - o.sumWeight).
            filter(o -> o > 0).collect(Collectors.toList());
        List<Double> totalResidue =
            splitPartitions.stream().map(SolverUtils.ToSplitPartition::getSplitSize).collect(Collectors.toList());
        totalResidue.sort(Comparator.comparingDouble(o -> o));
        double sumResidue = totalResidue.stream().reduce(Double::sum).orElse((double) 0);
        double finalGap = SolverUtils.waterFilling(lessThanGap, (double) 0, sumResidue).getKey();
        List<SolverUtils.ToSplitPartition> toSplitPartitions =
            caculateSplitSize(splitPartitions, lessThanGap, finalGap);
        partitionSplitInfo.generateSplitPoints(toSplitPartitions);
        Boolean splitPlanNumValid = partitionSplitInfo.checkSplitNumValid(toSplitPartitions);
        if (!splitPlanNumValid) {
            int splitCount = (int) lessThanGap.stream().filter(o -> o > finalGap).count();
            splitPartitions = splitAllLargePartitions(mu, splitCount);
            partitionSplitInfo.generateSplitPoints(splitPartitions);
        }
        Boolean planValid = checkSplitActionValidByEvaluate(partitionSplitInfo);
        if (!planValid) {
            partitionSplitInfo.reportErrorPartitions(splitPartitions);
        }
        return planValid;
    }

    private Boolean checkSplitActionValidByEvaluate(PartitionSplitInfo partitionSplitInfo) {
        int afterSplitM = M;
        int afterSplitN = N;
        List<Integer> afterSplitX = new ArrayList<>(Arrays.asList(ArrayUtils.toObject(X)));
        List<Double> afterSplitWeight = new ArrayList<>(Arrays.asList(ArrayUtils.toObject(Weight)));
        for (Integer splitIndex : partitionSplitInfo.splitPointsMap.keySet()) {
            List<Double> splitSizes = partitionSplitInfo.splitPointsMap.get(splitIndex).getValue();
            afterSplitWeight.set(splitIndex, splitSizes.get(0));
            int j = 1;
            for (j = 1; j < splitSizes.size(); j++) {
                afterSplitWeight.add(splitSizes.get(j) - splitSizes.get(j - 1));
            }
            afterSplitWeight.add(
                partitionSplitInfo.partitionGroupStatMap.get(splitIndex).getDataRows() - splitSizes.get(j - 1));
            afterSplitX.addAll(Collections.nCopies(splitSizes.size(), X[splitIndex]));
            afterSplitN += splitSizes.size();
        }
        GreedyModel greedyModel = new GreedyModel();
        greedyModel.setVariable(afterSplitM, afterSplitN, afterSplitX.stream().mapToInt(o -> o).toArray(),
            afterSplitWeight.stream().mapToDouble(o -> o).toArray());
        greedyModel.preSolve();
        return greedyModel.mu <= maxMu && greedyModel.mu <= mu;
    }

    //to refractor now!
    private List<SolverUtils.ToSplitPartition> caculateSplitSize(List<SolverUtils.ToSplitPartition> toSplitPartitions,
                                                                 List<Double> lessThanGap, Double targetGap) {
        List<Double> copyLessThanGaps = new ArrayList<>(lessThanGap);
        List<SolverUtils.ToSplitPartition> copyToSplitPartitions = new ArrayList<>(toSplitPartitions);
        copyLessThanGaps.sort(Comparator.comparingDouble(o -> (double) o).reversed());
        copyToSplitPartitions.sort(Comparator.comparingDouble(SolverUtils.ToSplitPartition::getSplitSize).reversed());
        int j = 0;
        int i = 0;
        while (i < toSplitPartitions.size() && j < copyLessThanGaps.size()) {
            SolverUtils.ToSplitPartition toSplitPartition = toSplitPartitions.get(i);
            double res = toSplitPartition.getSplitSize() - copyLessThanGaps.get(j) + targetGap;
            if (res > 0) {
                toSplitPartition.addSplitSize(lessThanGap.get(j) - targetGap);
                j++;
            } else {
                double splitSize = toSplitPartition.getSplitSize();
                toSplitPartition.comsueResidue();
                copyLessThanGaps.set(i, copyLessThanGaps.get(j) - splitSize);
                i++;
            }
        }
        toSplitPartitions =
            toSplitPartitions.stream().filter(o -> o.getSplitSizes().size() >= 2).collect(Collectors.toList());
        return toSplitPartitions;
    }

    private List<SolverUtils.ToSplitPartition> splitAllLargePartitions(double mu, int splitCount) {
        double avgTotalWeight = Arrays.stream(Weight).sum() / M;
        double thresholdFactor = 1 + mu * 0.8;
        List<SolverUtils.ToSplitPartition> toSplitPartitions = new ArrayList<>();
        for (int i = 0; i < M; i++) {
            SolverUtils.PartitionSet partitionSet = targetPartitionSets.get(i);
            SolverUtils.Fetcher fetcher = object -> ((Pair<Integer, Double>) object).getValue();
            if (partitionSet.sumWeight >= avgTotalWeight * thresholdFactor) {
                int index = SolverUtils.getFirstLargerThan(partitionSet.weightSet, avgTotalWeight * mu, fetcher);
                for (int j = index; j >= 0; j--) {
                    SolverUtils.ToSplitPartition toSplitPartition =
                        new SolverUtils.ToSplitPartition(partitionSet.weightSet.get(j));
                    toSplitPartitions.add(toSplitPartition);
                }
            }
        }
        for (int i = 0; i < toSplitPartitions.size(); i++) {
            toSplitPartitions.get(i).splitIntoNParts(splitCount);
        }
        return toSplitPartitions;
    }
}
