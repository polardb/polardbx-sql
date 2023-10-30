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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolverUtils {
    public static class PartitionSet {
        public int n;
        public double sumWeight;
        public List<Pair<Integer, Double>> weightSet;

        PartitionSet() {
            this.weightSet = new ArrayList<>();
            this.n = 0;
            this.sumWeight = 0;
        }

        public void update(double weight, int index) {
            this.n++;
            this.sumWeight += weight;
            this.weightSet.add(Pair.of(index, weight));
        }

        public double getSumWeight() {
            return sumWeight;
        }
    }

    public interface Fetcher {
        double fetchValue(Object object);
    }

    public static class ToSplitPartition {
        Integer index;
        Double sumSize;
        Double keepSize;

        public Integer getIndex() {
            return index;
        }

        public Double getSumSize() {
            return sumSize;
        }

        public Double getKeepSize() {
            return keepSize;
        }

        public Double getSplitSize() {
            return splitSize;
        }

        public void addSplitSize(double size) {
            splitSize -= size;
            splitSizes.add(size);
        }

        public void comsueResidue() {
            splitSizes.add(splitSize);
            splitSize = (double) 0;
        }

        public void splitIntoNParts(int n) {
            double size = sumSize / n;
            splitSizes = Collections.nCopies(n, size);
        }

        Double splitSize;

        public List<Double> getSplitSizes() {
            return splitSizes;
        }

        List<Double> splitSizes;
        List<Integer> splitIndexes;

        ToSplitPartition(Pair<Integer, Double> partition) {
            this.index = partition.getKey();
            this.sumSize = partition.getValue();
            this.splitSize = partition.getValue();
        }

        ToSplitPartition(Pair<Integer, Double> partition, double splitSize) {
            this.index = partition.getKey();
            this.sumSize = partition.getValue();
            this.splitSize = splitSize;
            this.keepSize = this.sumSize - splitSize;
            this.splitIndexes = Stream.of(this.index).collect(Collectors.toList());
            this.splitSizes = Stream.of(this.keepSize).collect(Collectors.toList());
        }
    }

    static int getFirstLargerThan(List<?> lst, double value, Fetcher fetcher) {
        //ASSUME: lst value high => low.
        int low = 0;
        int high = lst.size() - 1;

        while (low < high - 1) {
            int mid = (low + high) / 2;
            if (fetcher.fetchValue(lst.get(mid)) >= value) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        int index = -1;
        if (fetcher.fetchValue(lst.get(high)) >= value) {
            index = high;
        } else if (fetcher.fetchValue(lst.get(low)) >= value) {
            index = low;
        }
        return index;
    }

    public static class PartitionCluster {
        public int n;
        public Map<Integer, Double> weightSet;

        PartitionCluster() {
            this.weightSet = new HashMap<>();
            this.n = 0;
        }

        public void update(double weight, int index) {
            this.n++;
            this.weightSet.put(index, weight);
        }
    }

    public static Pair<Double, Double> waterFilling(List<Double> gaps, Double stopGap, double residue) {
        // water filling algorithm
        // filling gaps in list by water with quantity of residue
        // ASSUME: gaps high => low
        gaps.sort(Comparator.comparingDouble(o -> (double) o).reversed());
        int i = 0;
        double finalGap = 0;
        while (i < gaps.size()) {
            double gap = gaps.get(i);
            double nextGap = stopGap;
            if (i < gaps.size() - 1 && gaps.get(i + 1) > stopGap) {
                nextGap = gaps.get(i + 1);
            }
            if (gap <= stopGap) {
                finalGap = gap;
                break;
            }
            double fillingWaterQuantity = (gap - nextGap) * (i + 1);
            if (fillingWaterQuantity <= residue) {
                residue -= fillingWaterQuantity;
                i++;
            } else {
                finalGap = gap - residue / (i + 1);
                residue = 0;
                break;
            }
        }
        return Pair.of(finalGap, residue);
    }

    static Pair<Integer, Double> findLargerPartSumThan(List<Double> lst, Double sum) {
        // ASSUME: NO ORDER REQUIRED.
        Double partSum = (double) 0;
        int i;
        for (i = 0; i < lst.size(); i++) {
            partSum += lst.get(i);
            if (partSum >= sum) {
                break;
            }
        }
        return Pair.of(i, partSum);
    }
}
