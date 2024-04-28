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

import java.util.List;

/**
 * A runtime metrics object is a group of counter/timer organized as tree structure.
 * 1. Manage the name and value of counters/timers
 * 2. Life cycle management of counters/timers: create / remove / arrange
 * 3. Derive counter/timer value from tree structure.
 */
public interface RuntimeMetrics {
    static RuntimeMetrics create(String name) {
        return new RuntimeMetricsImpl(name);
    }

    /**
     * Get unique name of this runtime profile.
     */
    String name();

    /**
     * Get the parent of this runtime profile in tree structure.
     */
    RuntimeMetrics parent();

    /**
     * Get children of this runtime profile in tree structure.
     */
    List<RuntimeMetrics> children();

    /**
     * Add a child profile.
     */
    void addChild(RuntimeMetrics child);

    /**
     * The counter is owned by the RuntimeProfile object.
     * If the counter already exists, the existing counter object is returned.
     *
     * @param name counter name.
     * @param parentName If parent_name is a non-empty string, the counter is added as a child of parent name.
     * @return New counter or If the counter already exists, the existing counter object is returned.
     */
    Counter addCounter(String name, String parentName, ProfileUnit unit);

    /**
     * Add Derived counter as metrics tree node, aggregating metrics
     * from children instead of collecting metrics directly.
     *
     * @param name counter name.
     * @param parentName the name of parent counter.
     * @param unit profile unit.
     * @param accumulatorType type of accumulation.
     */
    void addDerivedCounter(String name, String parentName, ProfileUnit unit, ProfileAccumulatorType accumulatorType);

    /**
     * Get the counter with given name.
     *
     * @return null if not exist.
     */
    Counter getCounter(String name);

    /**
     * Remove the counter or timer with given name.
     *
     * @return TRUE if element removed, or FALSE if element not found.
     */
    boolean remove(String name);

    /**
     * Generate a metrics report from the given parent node.
     *
     * @param parent parent node.
     */
    String report(String parent);

    /**
     * Generate a metrics report from the root node.
     */
    default String reportAll() {
        return report(null);
    }

    void merge(RuntimeMetrics metrics);
}
