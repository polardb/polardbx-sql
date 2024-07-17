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

import java.util.List;

/**
 * A PartClauseItem represent a partition predicate expr that has been converted to a uniform formula
 *
 * @author chenghui.lch
 */
public class PartClauseItem {

    protected PartPruneStepType type = PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY;

    /**
     * if the type of PartClauseItem is NOTPARTPRUNE_COMBINE_INTERSECT or PARTPRUNE_COMBINE_UNION,
     * it always be null.
     */
    protected List<PartClauseItem> itemList = null;

    /**
     * if the type of PartClauseItem is NOT PARTPRUNE_OP_MATCHED_PART_KEY,
     * it always be null
     */
    protected PartClauseInfo clauseInfo = null;

    /**
     * FIXME: maybe predExpr is unused now
     */
    protected RexNode predExpr;
    protected boolean alwaysTrue = false;
    protected boolean alwaysFalse = false;
    protected String digest = null;

    public static PartClauseItem buildPartClauseItem(PartPruneStepType type,
                                                     PartClauseInfo clauseInfo,
                                                     List<PartClauseItem> itemList,
                                                     RexNode predExpr) {
        PartClauseItem partClauseItem = new PartClauseItem(type, clauseInfo, itemList, predExpr, false, false);
        return partClauseItem;
    }

    public static PartClauseItem buildAlwaysTrueItem() {
        PartClauseItem alwaysTrueItem =
            new PartClauseItem(PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY, null, null, null, true, false);
        return alwaysTrueItem;
    }

    public static PartClauseItem buildAlwaysFalseItem() {
        PartClauseItem alwaysTrueItem =
            new PartClauseItem(PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY, null, null, null, false, true);
        return alwaysTrueItem;
    }

    public static PartClauseItem buildAnyValueItem() {
        PartClauseItem alwaysTrueItem =
            new PartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, null, null, null, false, false);
        return alwaysTrueItem;
    }

    private PartClauseItem(PartPruneStepType type,
                           PartClauseInfo clauseInfo,
                           List<PartClauseItem> itemList,
                           RexNode predExpr,
                           boolean alwaysTrue,
                           boolean alwaysFalse) {
        this.type = type;
        this.clauseInfo = clauseInfo;
        this.itemList = itemList;
        this.predExpr = predExpr;
        this.alwaysTrue = alwaysTrue;
        this.alwaysFalse = alwaysFalse;
        this.digest = buildDigestInner();
    }

    public PartPruneStepType getType() {
        return type;
    }

    public void setType(PartPruneStepType type) {
        this.type = type;
    }

    public PartClauseInfo getClauseInfo() {
        return clauseInfo;
    }

    public List<PartClauseItem> getItemList() {
        return itemList;
    }

    protected String buildDigestInner() {
        StringBuilder sb = new StringBuilder("");
        if (itemList != null && !itemList.isEmpty()) {
            sb.append(type.getSymbol());
            sb.append("(");
            for (int i = 0; i < itemList.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(itemList.get(i).getDigest());
            }
            sb.append(")");
        } else {
            if (clauseInfo != null) {
                sb.append(clauseInfo.getDigest());
            } else {
                sb.append("NoPartClause");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return digest;
    }

    public boolean isAlwaysTrue() {
        return alwaysTrue;
    }

    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public String getDigest() {
        return digest;
    }

    public void setItemList(List<PartClauseItem> itemList) {
        this.itemList = itemList;
    }
}
