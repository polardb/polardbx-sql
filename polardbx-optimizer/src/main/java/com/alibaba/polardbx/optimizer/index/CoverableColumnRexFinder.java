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

package com.alibaba.polardbx.optimizer.index;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.Set;

/**
 * @author dylan
 */
public class CoverableColumnRexFinder extends RexVisitorImpl<Void> {

    private RelMetadataQuery mq;

    private RelNode rel;

    // {schema -> table -> columns}
    private CoverableColumnSet coverableColumnSet;

    private boolean inLogicalView;

    public CoverableColumnRexFinder(RelMetadataQuery mq, RelNode rel,
                                    CoverableColumnSet coverableColumnSet, boolean inLogicalView) {
        super(true);
        this.mq = mq;
        this.rel = rel;
        this.coverableColumnSet = coverableColumnSet;
        this.inLogicalView = inLogicalView;
    }

    public CoverableColumnSet getCoverableColumnSet() {
        return this.coverableColumnSet;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
        if (inLogicalView) {
            Set<RelColumnOrigin> columnOrigins = mq.getColumnOrigins(rel, inputRef.getIndex());
            this.coverableColumnSet.addCoverableColumn(columnOrigins);
        }
        return null;
    }

    @Override
    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == -2 || dynamicParam.getIndex() == -3) {
            CoverableColumnRelFinder coverableColumnRelFinder =
                new CoverableColumnRelFinder(mq, coverableColumnSet, inLogicalView);
            coverableColumnRelFinder.go(dynamicParam.getRel());
        }
        return null;
    }
}
