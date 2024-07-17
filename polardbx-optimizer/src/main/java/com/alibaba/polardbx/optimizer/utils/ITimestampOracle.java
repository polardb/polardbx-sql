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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;

/**
 * Timestamp Oracle Interface
 */
public interface ITimestampOracle extends Lifecycle {

    int BITS_PHYSICAL_TIME = 42;
    int BITS_LOGICAL_TIME = 22;
    int LOGICAL_TIME_MASK = (1 << BITS_LOGICAL_TIME) - 1;

    void setTimeout(long timeout);

    long nextTimestamp();

    static long getLogicalTime(final long logicalClock) {
        return (logicalClock & LOGICAL_TIME_MASK);
    }

    static StringBuilder format(StringBuilder build, final long logicalClock) {
        return build.append(logicalClock >>> BITS_LOGICAL_TIME).append(':').append(logicalClock & LOGICAL_TIME_MASK);
    }

    static String toString(final long logicalClock) {
        return format(new StringBuilder(20), logicalClock).toString();
    }

    static long getTimeMillis(long tso) {
        return (tso & (-1L << BITS_LOGICAL_TIME)) >>> BITS_LOGICAL_TIME;
    }
}
