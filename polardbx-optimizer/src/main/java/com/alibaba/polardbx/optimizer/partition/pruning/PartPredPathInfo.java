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

/**
 * A complete partition predicate path , such as p1_cond,p2_cond,p3_cond, ...
 *
 * @author chenghui.lch
 */
public class PartPredPathInfo {

    protected Integer partKeyStart;
    protected Integer partKeyEnd;
    /**
     * The prefix predicate item of predicate linked list
     * when the prefix predicate path  is p1_cond,p2_cond,p3_cond, ...
     * then the prefix predicate linked list is p3_cond -> p2_cond -> p1_cond
     */
    protected PartPredPathItem prefixPathItem;
    protected ComparisonKind cmpKind;
    /**
     * label that if current vectorial interval contains predicates for each partition columns
     */
    protected boolean containFullPartColPreds = true;
    
    
    public PartPredPathInfo(PartPredPathItem prefixPathItem, Integer partKeyStart, Integer partKeyEnd,
                            ComparisonKind cmpKind,
                            boolean containFullPartColPreds) {
        this.prefixPathItem = prefixPathItem;
        this.partKeyStart = partKeyStart;
        this.partKeyEnd = partKeyEnd;
        this.cmpKind = cmpKind;
        this.containFullPartColPreds = containFullPartColPreds;
    }

    public Integer getPartKeyEnd() {
        return partKeyEnd;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }

    public PartPredPathItem getPrefixPathItem() {
        return prefixPathItem;
    }

    public String getDigest() {
        return prefixPathItem == null ? "" : prefixPathItem.getDigest();
    }

    public boolean isContainFullPartColPreds() {
        return containFullPartColPreds;
    }

    @Override
    public String toString() {
        return getDigest();
    }
}
