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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

/**
 * @author dylan
 */
public class IndexableColumnRexFinder extends RexVisitorImpl<Void> {

    private RelMetadataQuery mq;

    private RelNode rel;

    // {schema -> table -> columns}
    private IndexableColumnSet indexableColumnSet;

    public IndexableColumnRexFinder(RelMetadataQuery mq, RelNode rel,
                                    IndexableColumnSet indexableColumnSet) {
        super(true);
        this.mq = mq;
        this.rel = rel;
        this.indexableColumnSet = indexableColumnSet;
    }

    public IndexableColumnSet getIndexableColumnSet() {
        return this.indexableColumnSet;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
        RelColumnOrigin columnOrigin;
        if (rel instanceof Join) {
            int leftCount = ((Join) rel).getLeft().getRowType().getFieldCount();
            if (inputRef.getIndex() < leftCount) {
                columnOrigin = mq.getColumnOrigin(((Join) rel).getLeft(), inputRef.getIndex());
            } else {
                columnOrigin = mq.getColumnOrigin(((Join) rel).getRight(), inputRef.getIndex() - leftCount);
            }
        } else {
            columnOrigin = mq.getColumnOrigin(rel, inputRef.getIndex());
        }
        this.indexableColumnSet.addIndexableColumn(columnOrigin);
        return null;
    }

    @Override
    public Void visitCall(RexCall call) {
        if (call.getOperator().getKind().belongsTo(SqlKind.INDEXABLE)) {
            return super.visitCall(call);
        } else {
            return null;
        }
    }

    @Override
    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == -2 || dynamicParam.getIndex() == -3) {
            IndexableColumnRelFinder indexableColumnRelFinder =
                new IndexableColumnRelFinder(mq, indexableColumnSet);
            indexableColumnRelFinder.go(dynamicParam.getRel());
        }
        return null;
    }
}
