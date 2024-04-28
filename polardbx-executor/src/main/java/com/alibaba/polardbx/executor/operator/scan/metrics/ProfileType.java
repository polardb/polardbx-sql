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

package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * Type of metrics.
 */
public enum ProfileType {
    /**
     * An incrementing and decrementing counter metric.
     */
    COUNTER,

    /**
     * A timer metric which aggregates timing durations and provides duration statistics.
     */
    TIMER,

    /**
     * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
     * moving average throughput.
     */
    METER,

    /**
     * A metric which calculates the distribution of a value.
     */
    HISTOGRAM,

    /**
     * A gauge metric is an instantaneous reading of a particular value.
     */
    GAUGE
}
