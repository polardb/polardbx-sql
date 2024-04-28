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

import com.alibaba.polardbx.executor.balancer.solver.SolverUtils.PartitionCluster;
import com.alibaba.polardbx.executor.balancer.solver.SolverUtils.PartitionSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GreedyModel {
    public Logger logger = Logger.getLogger(String.valueOf(getClass()));

    public int M;

    public int N;

    public int[] X;

    public int[] targetX;

    public double[] Weight;

    public double sum;

    public double moveCost;

    public int maxRetryTime = 2;

    public List<Pair<Integer, Integer>> indexedX;

    public List<Pair<Integer, Double>> indexedWeight;

    public double mu;

    public double finalMu;

    public double partitionSizeMatchThreshold;

    public static final int OPTIMAL = 0;
    public static final int INFEASIBLE = 1;

    SolverUtils.PartitionSet[] partitionSets;

    int[] drainNodeIndexes;

    public Boolean drainNodeFlag = false;

    SolverUtils.PartitionSet[] targetPartitionSets;

    GreedyModel() {
    }

    public void setVariable(int M, int N, int[] X, double[] Weight) {
        setVariable(M, N, X, Weight, new int[0]);
    }

    public void setVariable(int M, int N, int[] X, double[] Weight, int[] drainNodeIndexes) {
        this.M = M;
        this.N = N;
        this.X = X;
        this.Weight = Weight;
        this.partitionSets = new PartitionSet[M];
        this.drainNodeIndexes = drainNodeIndexes;
        for (int i = 0; i < M; i++) {
            this.partitionSets[i] = new PartitionSet();
        }
        this.indexedX = new ArrayList<>();
        this.indexedWeight = new ArrayList<>();
        for (int i = 0; i < X.length; i++) {
            int x = X[i];
            indexedX.add(Pair.of(i, X[i]));
            this.partitionSets[x].update(Weight[i], i);
            indexedWeight.add(Pair.of(i, Weight[i]));
            sum += Weight[i];
        }

        //indexedWeight: from high to low, because queue of weight
        indexedWeight.sort(Comparator.comparingDouble(Pair::getValue));
        Collections.reverse(indexedWeight);
    }

    public void preSolve() {
        //targetPartitionSetCount not strictly from less to high
        this.drainNodeIndexes = drainNodeIndexes;
        this.drainNodeFlag = true;
        double leftPartNum = (double) (this.X.length);
        int resM = M - drainNodeIndexes.length;
        double maxPartitionNum = Math.ceil(leftPartNum / resM);

        targetPartitionSets = new PartitionSet[M];
        List<PartitionSet> partitionSetList = new ArrayList<>();
        for (int i = 0; i < resM; i++) {
            partitionSetList.add(new PartitionSet());
        }
        List<PartitionSet> finalPartitionSetList = partitionSetList;
        PriorityQueue<Integer> partitionSetOrderBySumWeight =
            new PriorityQueue<>(Comparator.comparingDouble(o -> finalPartitionSetList.get(o).sumWeight));
        PriorityQueue<Integer> partitionSetOrderByPartNum =
            new PriorityQueue<>(Comparator.comparingInt(o -> finalPartitionSetList.get((Integer) o).n).reversed());

        for (int i = 0; i < resM; i++) {
            partitionSetOrderBySumWeight.add(i);
            partitionSetOrderByPartNum.add(i);
        }

        int leftNode = resM;
        for (int i = 0; i < X.length; i++) {
            if (leftNode <= 0) {
                break;
            }
            Integer largestPartNumIndex = partitionSetOrderByPartNum.peek();
            Integer smallestWeightIndex = partitionSetOrderBySumWeight.peek();
            PartitionSet partitionSet = finalPartitionSetList.get(largestPartNumIndex);
            if (partitionSet.n >= maxPartitionNum) {
                leftPartNum -= partitionSet.n;
                leftNode--;
                maxPartitionNum = Math.ceil(leftPartNum / leftNode);
                partitionSetOrderByPartNum.remove(largestPartNumIndex);
                partitionSetOrderBySumWeight.remove(largestPartNumIndex);
                i--;
            } else {
                partitionSet = finalPartitionSetList.get(smallestWeightIndex);
                partitionSet.update(indexedWeight.get(i).getValue(), indexedWeight.get(i).getKey());
                partitionSetOrderByPartNum.remove(smallestWeightIndex);
                partitionSetOrderBySumWeight.remove(smallestWeightIndex);
                partitionSetOrderByPartNum.add(smallestWeightIndex);
                partitionSetOrderBySumWeight.add(smallestWeightIndex);
            }
        }
        partitionSetList =
            partitionSetList.stream().sorted(Comparator.comparingDouble(o -> o.sumWeight)).collect(Collectors.toList());
        for (int i = 0; i < resM; i++) {
            targetPartitionSets[i] = partitionSetList.get(i);
        }
        for (int i = resM; i < M; i++) {
            targetPartitionSets[i] = new PartitionSet();
        }
        double minSize = targetPartitionSets[0].sumWeight;
        double maxSize = targetPartitionSets[resM - 1].sumWeight;
        double avgSize = sum / resM;
        mu = Math.max(1 - minSize / avgSize, maxSize / avgSize - 1);
    }

    private List<PartitionCluster> buildPartitionClusters() {
        //ASSUME indexedWeight from high to low.
        //SUCH THAT: clusterId from low to high
        double intervalFactor = (1 + partitionSizeMatchThreshold) / (1 - partitionSizeMatchThreshold);
        List<PartitionCluster> partitionClusters = new ArrayList<>();
        int i = X.length - 1;
        int clusterCount = 0;
        while (i >= 0) {
            double upper = indexedWeight.get(i).getValue() * intervalFactor;
            partitionClusters.add(new PartitionCluster());
            clusterCount++;
            while (i >= 0 && indexedWeight.get(i).getValue() <= upper) {
                partitionClusters.get(clusterCount - 1)
                    .update(indexedWeight.get(i).getValue(), indexedWeight.get(i).getKey());
                i--;
            }
        }
        return partitionClusters;
    }

    private double computeMatchCost(Map<Integer, Integer> partitionClusterMap, double[] weight,
                                    PartitionSet originalPartitionSet, PartitionSet targetPartitionSet) {
        //ASSUME: originalPartitionSet.weightSet high => low
        //ASSUME: targetPartitionSet.weighSet high => low
        //SUCH THAT: originalClusterId low => high
        List<Pair<Integer, Double>> originalWeightSet = originalPartitionSet.weightSet;
        List<Pair<Integer, Double>> targetWeightSet = targetPartitionSet.weightSet;
        List<Integer> originalClusterIds =
            originalWeightSet.stream().map(o -> partitionClusterMap.get(o.getKey())).collect(Collectors.toList());
        List<Integer> targetClusterIds =
            targetWeightSet.stream().map(o -> partitionClusterMap.get(o.getKey())).collect(Collectors.toList());
        int j = 0;
        int i = 0;
        double matchCost = 0;
        for (i = 0; i < targetWeightSet.size(); i++) {
            while (j < originalWeightSet.size() && originalClusterIds.get(j) > targetClusterIds.get(i)) {
                matchCost += weight[originalWeightSet.get(j).getKey()];
                j++;
            }
            if (j == originalWeightSet.size()) {
                break;
            }
            if (!originalClusterIds.get(j).equals(targetClusterIds.get(i))) {
                matchCost += weight[targetWeightSet.get(i).getKey()];
            } else {
                j++;
            }
        }
        while (i < targetWeightSet.size()) {
            matchCost += weight[targetWeightSet.get(i).getKey()];
            i++;
        }
        while (j < originalWeightSet.size()) {
            matchCost += weight[originalWeightSet.get(j).getKey()];
            j++;
        }

        return matchCost;
    }

    //Refractor.

    private Pair<List<Integer>, List<Integer>> caculateMovePartitions(Map<Integer, Integer> partitionClusterMap,
                                                                      PartitionSet originalPartitionSet,
                                                                      PartitionSet targetPartitionSet) {
        //ASSUME: originalPartitionSet.weightSet high => low
        //ASSUME: targetPartitionSet.weighSet high => low
        //SUCH THAT: originalClusterId low => high
        List<Pair<Integer, Double>> originalWeightSet = originalPartitionSet.weightSet;
        List<Pair<Integer, Double>> targetWeightSet = targetPartitionSet.weightSet;
        List<Integer> moveIn = new ArrayList<>();
        List<Integer> moveOut = new ArrayList<>();
        List<Integer> originalClusterId =
            originalWeightSet.stream().map(o -> partitionClusterMap.get(o.getKey())).collect(Collectors.toList());
        List<Integer> targetClusterId =
            targetWeightSet.stream().map(o -> partitionClusterMap.get(o.getKey())).collect(Collectors.toList());
        int j = 0;
        int i = 0;
        // two queue, match the element by size.
        for (i = 0; i < targetWeightSet.size(); i++) {
            while (j < originalWeightSet.size() && originalClusterId.get(j) > targetClusterId.get(i)) {
                moveOut.add(originalWeightSet.get(j).getKey());
                j++;
            }
            if (j == originalWeightSet.size()) {
                break;
            }
            if (!originalClusterId.get(j).equals(targetClusterId.get(i))) {
                moveIn.add(targetWeightSet.get(i).getKey());
            } else {
                j++;
            }
        }
        while (i < targetWeightSet.size()) {
            moveIn.add(targetWeightSet.get(i).getKey());
            i++;
        }
        while (j < originalWeightSet.size()) {
            moveOut.add(originalWeightSet.get(j).getKey());
            j++;
        }
        return Pair.of(moveIn, moveOut);
    }

    private Map<Integer, Integer> findWeightMatch(Map<Integer, Integer> partitionClusterMap,
                                                  List<Integer> moveInPartitions, List<Integer> moveOutPartitions) {
        moveInPartitions.sort(Comparator.comparingInt(partitionClusterMap::get));
        moveOutPartitions.sort(Comparator.comparingInt(partitionClusterMap::get));
        Map<Integer, Integer> weightMatch = new HashMap<>();
        for (int i = 0; i < moveInPartitions.size(); i++) {
            weightMatch.put(moveInPartitions.get(i), moveOutPartitions.get(i));
        }
        return weightMatch;
    }

    public List<List<Integer>> getMoveInPartitions() {
        // 1. cluster partitions with close weight.
        List<PartitionCluster> partitionClusters = buildPartitionClusters();
        Map<Integer, Integer> partitionClusterMap = new HashMap<>();
        for (int i = 0; i < partitionClusters.size(); i++) {
            int finalI = i;
            partitionClusters.get(i).weightSet.keySet().forEach(o -> partitionClusterMap.put(o, finalI));
        }
        // 2. sort partitionSets.weightSet with weight.
        // weightSet: from high to low.
        for (int i = 0; i < M; i++) {
            partitionSets[i].weightSet.sort(Comparator.comparingDouble(Pair::getValue));
            Collections.reverse(partitionSets[i].weightSet);
        }
        // 3. match targetPartitionSets with existing partitionSets, get match cost.
        int resM = M - drainNodeIndexes.length;
        double[][] matchCost = new double[resM][resM];
        Set<Integer> drainNodeIndexSet = Arrays.stream(drainNodeIndexes).boxed().collect(Collectors.toSet());
        int index = 0;
        Map<Integer, Integer> resNodeMap = new HashMap<>();
        for (int i = 0; i < M; i++) {
            if (!drainNodeIndexSet.contains(i)) {
                resNodeMap.put(index, i);
                index++;
            }
        }
        for (int i = 0; i < drainNodeIndexes.length; i++) {
            resNodeMap.put(index, drainNodeIndexes[i]);
            index++;
        }
        for (int i = 0; i < resM; i++) {
            for (int j = 0; j < resM; j++) {
                int nodeIndex = resNodeMap.get(i);
                matchCost[i][j] =
                    computeMatchCost(partitionClusterMap, Weight, partitionSets[nodeIndex], targetPartitionSets[j]);
            }
        }
        // 4. solver matches.
        int matches[] = MatchModel.solve(resM, matchCost);

        int fullMatches[] = new int[M];
        for (int i = 0; i < resM; i++) {
            fullMatches[resNodeMap.get(i)] = matches[i];
        }
        for (int i = resM; i < M; i++) {
            fullMatches[resNodeMap.get(i)] = i;
        }

        // 5. get moveIn and moveOut partitions
        List<Integer> moveInPartitions = new ArrayList<>();
        List<Integer> moveOutPartitions = new ArrayList<>();
        List<List<Integer>> moveInPartitionsList = new ArrayList<>();
        for (int i = 0; i < M; i++) {
            Pair<List<Integer>, List<Integer>> movePartitions =
                caculateMovePartitions(partitionClusterMap, partitionSets[i], targetPartitionSets[fullMatches[i]]);
            moveInPartitionsList.add(movePartitions.getKey());
            moveInPartitions.addAll(movePartitions.getKey());
            moveOutPartitions.addAll(movePartitions.getValue());
        }

        // 6. substitue move plan by match partitions with cluster map.
        Map<Integer, Integer> weightMatch = findWeightMatch(partitionClusterMap, moveInPartitions, moveOutPartitions);
        for (int i = 0; i < M; i++) {
            List<Integer> moveInPartition = moveInPartitionsList.get(i);
            for (int j = 0; j < moveInPartition.size(); j++) {
                Integer swapPartition = weightMatch.get(moveInPartition.get(j));
                moveInPartition.set(j, swapPartition);
            }
        }
        // 7. return move_in partition list
        return moveInPartitionsList;
    }

    public Solution buildGreedySolution(GreedyModel model, int status) {
        if (status == 0) {
            int[] targetPlace = model.getSolution();
            return new Solution(true, targetPlace, finalMu, "GreedySolver");
        } else {
            return new Solution(false, finalMu, "GreedySolver");
        }
    }

    public Solution greedySolveWithRetry() {
        int status = 0;
        for (int i = 0; i < maxRetryTime; i++) {
            this.solve();
            switch (this.getStatus()) {
            case INFEASIBLE:
                status = -1;
                break;
            case OPTIMAL:
                status = 0;
                break;
            default:
                break;
            }
            if (status == 0) {
                return buildGreedySolution(this, status);
            }
        }
        return buildGreedySolution(this, -1);
    }

    public void solve() {
        if (mu >= 0.1) {
            partitionSizeMatchThreshold = 0.1;
        } else if (mu >= 0.05) {
            partitionSizeMatchThreshold = mu;
        } else {
            partitionSizeMatchThreshold = 0.05;
        }
        List<List<Integer>> moveInPartitions = getMoveInPartitions();
        targetX = new int[X.length];
        moveCost = 0;
        for (int i = 0; i < X.length; i++) {
            targetX[i] = X[i];
        }
        for (int i = 0; i < M; i++) {
            List<Integer> moveInPartition = moveInPartitions.get(i);
            for (int j = 0; j < moveInPartition.size(); j++) {
                int x = moveInPartition.get(j);
                moveCost += Weight[x];
                targetX[x] = i;
            }
        }
        double[] sumPartitionSize = new double[M];
        for (int i = 0; i < X.length; i++) {
            sumPartitionSize[targetX[i]] += Weight[i];
        }
        double minSize = Arrays.stream(sumPartitionSize).min().getAsDouble();
        double maxSize = Arrays.stream(sumPartitionSize).max().getAsDouble();
        double avgSize = sum / M;
        finalMu = (avgSize < 1e-5) ? 1 : Math.max(1 - minSize / avgSize, maxSize / avgSize - 1);
    }

    public int getStatus() {
        return 0;
    }

    public int[] getSolution() {
        return targetX;
    }

    public static int[] solve(int M, int N, int[] X, double[] Weight) {
        GreedyModel greedyModel = new GreedyModel();
        greedyModel.setVariable(M, N, X, Weight);
        greedyModel.preSolve();
        greedyModel.solve();
        return greedyModel.getSolution();
    }

    public static int[] solve(int M, int N, int[] X, double[] Weight, int[] drainNodeIndexes) {
        GreedyModel greedyModel = new GreedyModel();
        greedyModel.setVariable(M, N, X, Weight, drainNodeIndexes);
        greedyModel.preSolve();
        greedyModel.solve();
        return greedyModel.getSolution();
    }
}
