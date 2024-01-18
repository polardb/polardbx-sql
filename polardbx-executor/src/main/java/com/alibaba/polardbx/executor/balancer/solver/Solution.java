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

import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Map;

public class Solution {
    public Boolean withValidSolve = false;

    public void setWithSplitPartition(Boolean withSplitPartition) {
        this.withSplitPartition = withSplitPartition;
    }

    public Boolean withSplitPartition = false;
    public int[][] optimi;

    public int[] targetPlace;

    public String strategy;

    public double mu;

    Solution(Boolean withValidSolve, int[][] optimi, int[] targetPlace, double mu, String strategy) {
        this.withValidSolve = withValidSolve;
        this.optimi = optimi;
        this.targetPlace = targetPlace;
        this.mu = mu;
        this.strategy = strategy;
    }

    Solution(Boolean withValidSolve, int[] targetPlace, double mu, String strategy) {
        this.withValidSolve = withValidSolve;
        this.targetPlace = targetPlace;
        this.mu = mu;
        this.strategy = strategy;
    }

    Solution(Boolean withValidSolve) {
        this.withValidSolve = withValidSolve;
    }

    Solution(Boolean withValidSolve, double mu, String strategy) {
        this.withValidSolve = withValidSolve;
        this.mu = mu;
        this.strategy = strategy;
    }

}
