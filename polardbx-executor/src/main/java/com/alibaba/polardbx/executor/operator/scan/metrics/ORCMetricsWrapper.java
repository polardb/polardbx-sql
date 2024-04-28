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

import com.codahale.metrics.Counter;
import org.apache.orc.customized.ORCProfile;

/**
 * Wrapper of Runtime Metrics and adapt to ORC Profile.
 */
public class ORCMetricsWrapper implements ORCProfile {
    private final String name;
    private final Counter counter;
    private final RuntimeMetrics runtimeMetrics;

    public ORCMetricsWrapper(String name, String parentName, ProfileUnit unit, RuntimeMetrics runtimeMetrics) {
        this.name = name;
        this.runtimeMetrics = runtimeMetrics;
        this.counter = runtimeMetrics.addCounter(name, parentName, unit);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void update(long delta) {
        counter.inc(delta);
    }
}
