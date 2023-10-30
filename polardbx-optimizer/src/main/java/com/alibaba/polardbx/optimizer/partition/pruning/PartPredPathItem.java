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
 * @author chenghui.lch
 */
public class PartPredPathItem {
    protected PartPredPathItem prefixPath;
    protected PartClauseIntervalInfo partClauseIntervalInfo;
    protected int partKeyIdx;

    public PartPredPathItem(PartPredPathItem prefixPath,
                            PartClauseIntervalInfo partClauseIntervalInfo,
                            int partKeyIndex) {
        this.prefixPath = prefixPath;
        this.partClauseIntervalInfo = partClauseIntervalInfo;
        this.partKeyIdx = partKeyIndex;
    }

    public void toPartClauseInfoArray(PartClauseIntervalInfo[] output) {
        if (prefixPath != null) {
            prefixPath.toPartClauseInfoArray(output);
        }
        output[partKeyIdx] = partClauseIntervalInfo;
    }

    public String getDigest() {
        return getDigest(false);
    }

    public String getDigest(boolean ignoreOp) {
        StringBuilder itemDigestSb = new StringBuilder("");
        if (prefixPath != null) {
            itemDigestSb.append(prefixPath.getDigest(ignoreOp));
            itemDigestSb.append(",");
        }
        itemDigestSb.append(partClauseIntervalInfo.getPartClause().getDigest(ignoreOp));
        return itemDigestSb.toString();
    }

    public PartClauseIntervalInfo getPartClauseIntervalInfo() {
        return partClauseIntervalInfo;
    }

    @Override
    public String toString() {
        return getDigest();
    }
}
