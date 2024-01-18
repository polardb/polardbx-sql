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

package com.alibaba.polardbx.optimizer.partition.common;

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartClauseInfo;

import java.util.List;

/**
 * @author chenbhui.lch
 */
public class GenTupleRouteFuncParams {
    protected PartitionInfo partInfo;
    protected PartKeyLevel level;
    protected List<PartClauseInfo> partClauseInfoLis;

    public GenTupleRouteFuncParams() {
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public PartKeyLevel getLevel() {
        return level;
    }

    public void setLevel(PartKeyLevel level) {
        this.level = level;
    }

    public List<PartClauseInfo> getPartClauseInfoLis() {
        return partClauseInfoLis;
    }

    public void setPartClauseInfoLis(
        List<PartClauseInfo> partClauseInfoLis) {
        this.partClauseInfoLis = partClauseInfoLis;
    }
}
