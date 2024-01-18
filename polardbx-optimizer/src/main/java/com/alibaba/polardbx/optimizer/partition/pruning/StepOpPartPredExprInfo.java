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

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class StepOpPartPredExprInfo {

    /**
     * The order of the list is the same as the part col defined in partitionBy,
     * Must Be Read-Only !!!
     */
    protected List<PartClauseInfo> partPredExprList = new ArrayList<>();
    protected ComparisonKind cmpKind;

    public List<PartClauseInfo> getPartPredExprList() {
        return partPredExprList;
    }

    public void setPartPredExprList(
        List<PartClauseInfo> partPredExprList) {
        this.partPredExprList = partPredExprList;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }

    public void setCmpKind(ComparisonKind cmpKind) {
        this.cmpKind = cmpKind;
    }
}
