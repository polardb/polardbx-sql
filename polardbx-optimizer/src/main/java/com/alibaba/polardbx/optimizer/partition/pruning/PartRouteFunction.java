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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.BitSet;

/**
 * @author chenghui.lch
 */
public abstract class PartRouteFunction {

    protected int partCount;
    protected int subPartCount;
    protected PartitionInfo partInfo;
    protected PartitionRouter router = null;
    protected PartKeyLevel matchLevel;
    protected ComparisonKind cmpKind;

    public PartRouteFunction() {
    }

    public abstract PartRouteFunction copy();

    public abstract BitSet routePartitions(ExecutionContext ec, PartPruneStepPruningContext pruningCtx);

    public int getPartCount() {
        return partCount;
    }

    public void setPartCount(int partCount) {
        this.partCount = partCount;
    }

    public int getSubPartCount() {
        return subPartCount;
    }

    public void setSubPartCount(int subPartCount) {
        this.subPartCount = subPartCount;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public PartitionRouter getRouter() {
        return router;
    }

    public void setRouter(PartitionRouter router) {
        this.router = router;
    }

    public PartKeyLevel getMatchLevel() {
        return matchLevel;
    }

    public void setMatchLevel(PartKeyLevel matchLevel) {
        this.matchLevel = matchLevel;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }

    public void setCmpKind(ComparisonKind cmpKind) {
        this.cmpKind = cmpKind;
    }
}
