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

package com.alibaba.polardbx.optimizer.sharding.label;

import java.util.EnumSet;

/**
 * Type of the bottom relNode of the subtree which belongs to a label
 */
public enum LabelType {
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.TableScan}
     */
    TABLE_SCAN,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Join}
     */
    JOIN,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Values}
     */
    VALUES,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Union}
     */
    UNION,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Project}
     * and the project is not a permutation
     */
    PROJECT,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Aggregate}
     */
    AGGREGATE,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Filter}
     * and the filter contains at least one {@link org.apache.calcite.rex.RexSubQuery} condition
     */
    FILTER_SUBQUERY,
    /**
     * Label with bottom relNode of {@link org.apache.calcite.rel.core.Project}
     * and the filter contains at least one {@link org.apache.calcite.rex.RexSubQuery} condition
     */
    PROJECT_SUBQUERY,
    /**
     * Label for RexSubquery
     */
    REX_SUBQUERY,

    CORRELATE;
    ///**
    // * Wrapper for correlate subquery
    // */
    //CORRELATE_SUBQUERY,
    ///**
    // * Wrapper for non-correlate subquery
    // */
    //NON_CORRELATE_SUBQUERY;

    public static EnumSet<LabelType> COLUMN_JOINT_LABEL = EnumSet.of(JOIN, CORRELATE);
    public static EnumSet<LabelType> LEAF_LABEL = EnumSet.of(TABLE_SCAN, VALUES);
    public static EnumSet<LabelType> SNAPSHOT_LABEL = EnumSet.of(AGGREGATE, PROJECT, FILTER_SUBQUERY, REX_SUBQUERY);
    public static EnumSet<LabelType> SUBQUERY_WRAPPER_LABEL = EnumSet.of(FILTER_SUBQUERY, PROJECT_SUBQUERY, CORRELATE);

    public boolean isColumnJointLabel() {
        return COLUMN_JOINT_LABEL.contains(this);
    }

    public boolean isLeafLabel() {
        return LEAF_LABEL.contains(this);
    }

    public boolean isSnapshotLabel() {
        return SNAPSHOT_LABEL.contains(this);
    }

    public boolean isSubqueryWrapper() {
        return SUBQUERY_WRAPPER_LABEL.contains(this);
    }
}
