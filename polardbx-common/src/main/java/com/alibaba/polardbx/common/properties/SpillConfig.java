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

package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import io.airlift.slice.DataSize;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_AVAILABLE_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MAX_QUERY_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_SPILL_FD_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MAX_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_SPILL_THREADS;
import static com.alibaba.polardbx.common.properties.DynamicConfig.parseValue;
import static io.airlift.slice.DataSize.Unit.GIGABYTE;

public class SpillConfig {

    private static final Logger log = LoggerFactory.getLogger(SpillConfig.class);

    private int maxSpillThreads = ThreadCpuStatUtil.NUM_CORES;
    private int maxSpillFdThreshold = 10000;
    private DataSize maxSpillSpaceThreshold = new DataSize(200, GIGABYTE);
    private DataSize maxQuerySpillSpaceThreshold = new DataSize(100, GIGABYTE);
    private double maxAvaliableSpaceThreshold = 0.9;

    public void loadValue(org.slf4j.Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case MPP_MAX_SPILL_THREADS:
                maxSpillThreads = parseValue(value, Integer.class, ThreadCpuStatUtil.NUM_CORES);
                break;
            case MPP_MAX_SPILL_FD_THRESHOLD:
                maxSpillFdThreshold = parseValue(value, Integer.class, 1000);
                break;
            case MPP_AVAILABLE_SPILL_SPACE_THRESHOLD:
                maxAvaliableSpaceThreshold = parseValue(value, Double.class, 0.9);
                break;
            case MAX_SPILL_SPACE_THRESHOLD:
                maxSpillSpaceThreshold = new DataSize(parseValue(value, Long.class, 200L), GIGABYTE);
                break;
            case MAX_QUERY_SPILL_SPACE_THRESHOLD:
                maxQuerySpillSpaceThreshold = new DataSize(parseValue(value, Long.class, 100L), GIGABYTE);
                break;
            }
        }
    }

    public double getAvaliableSpillSpaceThreshold() {
        return maxAvaliableSpaceThreshold;
    }

    public DataSize getMaxQuerySpillSpaceThreshold() {
        return maxQuerySpillSpaceThreshold;
    }

    public DataSize getMaxSpillSpaceThreshold() {
        return maxSpillSpaceThreshold;
    }

    public int getMaxSpillFdThreshold() {
        return maxSpillFdThreshold;
    }

    public int getMaxSpillThreads() {
        return maxSpillThreads;
    }

}


