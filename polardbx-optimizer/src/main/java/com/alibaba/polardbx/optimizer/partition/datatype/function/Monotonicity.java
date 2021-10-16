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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

/**
 * Represent the monotonicity info of the partition function.
 */
public enum Monotonicity {
    /**
     * none of the below holds
     */
    NON_MONOTONIC,
    /**
     * F() is unary and (x < y) => (F(x) <= F(y))
     */
    MONOTONIC_INCREASING,
    /**
     * But only for valid/real x and y
     */
    MONOTONIC_INCREASING_NOT_NULL,
    /**
     * F() is unary and (x < y) => (F(x) <  F(y))
     */
    MONOTONIC_STRICT_INCREASING,
    /**
     * But only for valid/real x and y
     */
    MONOTONIC_STRICT_INCREASING_NOT_NULL
}
