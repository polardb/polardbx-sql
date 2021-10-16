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

package com.alibaba.polardbx.optimizer.partition.pruning;

import org.apache.calcite.rex.RexNode;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenghui.lch
 */
public class PredConstExprReferenceInfo {
    protected Integer constExprId;
    protected String constExprDigest;
    protected RexNode constExpr;
    protected AtomicInteger referencedCount;

    protected PredConstExprReferenceInfo(Integer constExprId, RexNode constExpr) {
        this.constExprId = constExprId;
        this.constExpr = constExpr;
        this.constExprDigest = constExpr.toString();
        this.referencedCount = new AtomicInteger(1);
    }

    public RexNode getConstExpr() {
        return constExpr;
    }

    public AtomicInteger getReferencedCount() {
        return referencedCount;
    }

    public Integer getConstExprId() {
        return constExprId;
    }

    public String getConstExprDigest() {
        return constExprDigest;
    }
}
