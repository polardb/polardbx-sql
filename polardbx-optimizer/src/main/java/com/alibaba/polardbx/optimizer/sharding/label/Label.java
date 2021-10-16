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

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * @author chenmo.cm
 */
public interface Label extends Cloneable, LabelOptNode {

    List<Label> getInputs();

    <T extends Label> T getInput(int i);

    Label copy(List<Label> inputs);

    /**
     * If project contains any expression column, store the expression in
     * {@link AbstractLabelOptNode#columnConditionMap} with its column index. <br/>
     * Firstly, get expression from {@code project} argument if exists any, then put
     * them into {@link AbstractLabelOptNode#columnConditionMap} with their column index.
     * <br/>
     * Secondly, apply column permutation on current Label, update
     * {@link AbstractLabelOptNode#columnMapping}, {@link AbstractLabelOptNode#currentBaseRowType}
     * and reset {@link AbstractLabelOptNode#rowType} cache.
     *
     * @param project project whose input is the root node of the subtree which
     * belongs to this Label
     * @param context extractor context
     * @return original Label with columnMapping and rowType updated
     */
    Label project(Project project, ExtractorContext context);

    /**
     * Apply column permutation on current Label, update
     * {@link AbstractLabelOptNode#columnMapping}, {@link AbstractLabelOptNode#currentBaseRowType}
     * and reset {@link AbstractLabelOptNode#rowType} cache.
     *
     * @param window window whose input is the root node of the subtree which
     * belongs to this Label
     * @param context extractor context
     * @return original Label with columnMapping and rowType updated
     */
    Label window(Window window, ExtractorContext context);

    Label expand(LogicalExpand logicalExpand, ExtractorContext context);

    /**
     * Firstly, if {@link Aggregate#getAggCallList()} is not empty, put
     * all AggregateCall into {@link AbstractLabelOptNode#columnConditionMap} with their column index.
     * <br/>
     * Secondly, apply column permutation on current Label, update
     * {@link AbstractLabelOptNode#columnMapping}, {@link AbstractLabelOptNode#currentBaseRowType}
     * and reset {@link AbstractLabelOptNode#rowType} cache.
     *
     * @param agg aggregate whose input is the root node of the subtree which
     * belongs to this Label
     * @param context extractor context
     * @return original Label with columnMapping and rowType updated
     */
    Label aggregate(Aggregate agg, ExtractorContext context);

    Label aggregate(HashGroupJoin agg, ExtractorContext context);

    /**
     * Add predicate to {@link AbstractLabelOptNode#predicates}
     *
     * @param predicate predicate without subquery
     * @param filter from which the {@code predicate} is extracted. null for inferred predicate
     * @param context extractor context
     * @return origin Label with {@link AbstractLabelOptNode#predicates} updated
     */
    Label filter(RexNode predicate, Filter filter, ExtractorContext context);

    /**
     * Accepts a visit from a shuttle.
     *
     * @param shuttle Shuttle
     * @return A copy of this label incorporating changes made by the shuttle to
     * this label's children
     */
    Label accept(LabelShuttle shuttle);
}
