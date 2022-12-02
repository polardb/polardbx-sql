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

import org.apache.calcite.rex.RexDynamicParam;

import java.util.ArrayList;
import java.util.List;

/**
 * The clause info of a partition predicate expression
 *
 * @author chenghui.lch
 */
public class SubQueryInPartClauseInfo extends PartClauseInfo {
    /**
     *  (p1,p2) in (dynamicScalarSubQuery) will be converted to
     *      foreach val of dynamicScalarSubQuery
     *          1. set the values for tmpDynamicParams1, tmpDynamicParams2 by val of dynamicScalarSubQuery;
     *          2. perform pruning by p1=tmpDynamicParams1 and p2=tmpDynamicParams2
     * , which the tmpDynamicParams1,tmpDynamicParams2,tmpDynamicParams3 is the list of the following partColDynamicParams
     */
    protected List<RexDynamicParam> partColDynamicParams = new ArrayList<>();
    protected List<PartClauseItem> eqExprClauseItems = new ArrayList<>();
    protected RexDynamicParam subQueryDynamicParam;

    public SubQueryInPartClauseInfo() {
        this.isSubQueryInExpr = true;
    }

    public List<RexDynamicParam> getPartColDynamicParams() {
        return partColDynamicParams;
    }

    public void setPartColDynamicParams(List<RexDynamicParam> partColDynamicParams) {
        this.partColDynamicParams = partColDynamicParams;
    }

    public List<PartClauseItem> getEqExprClauseItems() {
        return eqExprClauseItems;
    }

    public RexDynamicParam getSubQueryDynamicParam() {
        return subQueryDynamicParam;
    }

    public void setSubQueryDynamicParam(RexDynamicParam subQueryDynamicParam) {
        this.subQueryDynamicParam = subQueryDynamicParam;
    }
}
