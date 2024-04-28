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

package com.alibaba.polardbx.optimizer.config.meta;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdExplainVisibility;
import org.apache.calcite.rel.metadata.RelMdExpressionLineage;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMdNodeTypes;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;

public class
DrdsRelMetadataProvider extends ChainedRelMetadataProvider {
    public static final DrdsRelMetadataProvider INSTANCE = new DrdsRelMetadataProvider();
    public static final JaninoRelMetadataProvider DEFAULT =
        JaninoRelMetadataProvider.of(DrdsRelMetadataProvider.INSTANCE);

    protected DrdsRelMetadataProvider() {
        super(
            ImmutableList.of(
                DrdsRelMdCost.SOURCE,
                DrdsRelMdColumnOrigins.SOURCE,
                DrdsRelMdColumnOriginNames.SOURCE,
                DrdsRelMdDmlColumnNames.SOURCE,
                DrdsRelMdOriginalRowType.SOURCE,
                DrdsRelMdCoveringIndex.SOURCE,
                RelMdExpressionLineage.SOURCE,
                DrdsRelMdTableReferences.SOURCE,
                RelMdNodeTypes.SOURCE,
                DrdsRelMdRowCount.SOURCE,
                DrdsRelMdMaxRowCount.SOURCE,
                RelMdMinRowCount.SOURCE,
                RelMdUniqueKeys.SOURCE,
                DrdsRelMdColumnUniqueness.SOURCE,
                DrdsRelMdCompositePk.SOURCE,
                DrdsRelMdPopulationSize.SOURCE,
                RelMdSize.SOURCE,
                DrdsRelMdDistribution.SOURCE,
                RelMdMemory.SOURCE,
                DrdsRelMdDistinctRowCount.SOURCE,
                DrdsRelMdSelectivity.SOURCE,
                RelMdExplainVisibility.SOURCE,
                DrdsRelMdPredicates.SOURCE,
                DrdsRelMdAllPredicates.SOURCE,
                DrdsRelMdFunctionalDependency.SOURCE,
                RelMdCollation.SOURCE,
                DrdsRelMdLowerBoundCost.SOURCE));
    }
}
