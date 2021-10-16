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

package com.alibaba.polardbx.optimizer.planmanager.parametric;

import com.alibaba.polardbx.common.exception.NotSupportException;

public enum SimilarityAlgo {
    EUCLIDEAN, COSINE;

    public double distance(double[] array1, double[] array2) {
        switch (this) {
        case EUCLIDEAN:
            return euclideanDistance(array1, array2);
        case COSINE:
            return cosineDistance(array1, array2);
        default:
            throw new NotSupportException("not supported SimilarityAlgo" + this.name());
        }
    }

    public static double cosineDistance(double[] selectivityA, double[] selectivityB) {
        int size = selectivityA.length;
        double simVal = 0;

        double num = 0;
        double den = 1;
        double powa_sum = 0;
        double powb_sum = 0;
        for (int i = 0; i < size; i++) {
            double a = selectivityA[i];
            double b = selectivityB[i];

            num = num + a * b;
            powa_sum = powa_sum + Math.pow(a, 2);
            powb_sum = powb_sum + Math.pow(b, 2);
        }
        double sqrta = Math.sqrt(powa_sum);
        double sqrtb = Math.sqrt(powb_sum);
        den = sqrta * sqrtb;

        simVal = num / den;

        return simVal;
    }

    public double euclideanDistance(double[] selectivityA, double[] selectivityB) {
        double distance = 0;
        if (selectivityA.length == selectivityB.length) {
            for (int i = 0; i < selectivityA.length; i++) {
                double temp = Math.pow((selectivityA[i] - selectivityB[i]), 2);
                distance += temp;
            }
            distance = Math.sqrt(distance);
        }
        return distance;
    }

}
