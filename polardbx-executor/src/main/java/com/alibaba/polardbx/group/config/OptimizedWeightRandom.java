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

package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OptimizedWeightRandom {

    private static final Logger logger = LoggerFactory.getLogger(OptimizedWeightRandom.class);

    public Object[] weightsArray;
    private Random random = new Random(System.currentTimeMillis());
    List<Pair<Object, Integer>> weights;

    public OptimizedWeightRandom(List<Pair<Object, Integer>> weights) {

        this.weights = weights;

        int weightSum = 0;

        for (Pair<Object, Integer> weight : weights) {
            weightSum += weight.getValue();
        }

        List<Object> weightsArray = new ArrayList();

        for (Pair<Object, Integer> weight : weights) {
            for (int i = 0; i < (weight.getValue() / (double) weightSum) * 100; i++) {
                weightsArray.add(weight.getKey());
            }
        }

        this.weightsArray = (Object[]) weightsArray.toArray();

    }

    public Object next() {
        return weightsArray[random.nextInt(weightsArray.length - 1)];
    }

    public List<Pair<Object, Integer>> getWeights() {
        return weights;
    }

}
