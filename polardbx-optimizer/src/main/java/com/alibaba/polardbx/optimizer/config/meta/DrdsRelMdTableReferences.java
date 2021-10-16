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

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdTableReferences;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Set;

public class DrdsRelMdTableReferences extends RelMdTableReferences {
    /**
     * make sure you have overridden the SOURCE
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.TABLE_REFERENCES.method, new DrdsRelMdTableReferences());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdColumnOrigins.class);

    public Set<RexTableInputRef.RelTableRef> getTableReferences(LogicalView rel, RelMetadataQuery mq,
                                                                boolean logicalViewLevel) {
        return rel.getTableReferences(mq, logicalViewLevel);
    }

    public Set<RexTableInputRef.RelTableRef> getTableReferences(LogicalTableScan rel, RelMetadataQuery mq,
                                                                boolean logicalViewLevel) {
        return Sets.newHashSet(RexTableInputRef.RelTableRef.of(rel.getTable(), 0));
    }

    public Set<RexTableInputRef.RelTableRef> getTableReferences(RelSubset rel, RelMetadataQuery mq,
                                                                boolean logicalViewLevel) {
        if (logicalViewLevel) {
            return mq.getLogicalViewReferences(rel.getOriginal());
        } else {
            return mq.getTableReferences(rel.getOriginal());
        }
    }

    public Set<RexTableInputRef.RelTableRef> getTableReferences(TableLookup rel, RelMetadataQuery mq,
                                                                boolean logicalViewLevel) {
        if (logicalViewLevel) {
            return mq.getLogicalViewReferences(rel.getProject());
        } else {
            return mq.getTableReferences(rel.getProject());
        }
    }

    public Set<RexTableInputRef.RelTableRef> getTableReferences(ViewPlan rel, RelMetadataQuery mq,
                                                                boolean logicalViewLevel) {
        return Sets.newHashSet(RexTableInputRef.RelTableRef.of(rel.getTable(), 0));
    }
}
