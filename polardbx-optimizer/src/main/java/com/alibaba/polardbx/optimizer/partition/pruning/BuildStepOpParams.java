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

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;

/**
 * @author chenghui.lch
 */
public class BuildStepOpParams {

    protected PartPruneStepBuildingContext currFullContext;
    protected PartitionInfo partInfo;
    protected PartPredPathInfo partPredPathInfo;
    protected PartRouteFunction predRouteFunc;
    protected PartKeyLevel partKeyMatchLevel;
    protected boolean isConflict;
    protected boolean forceFullScan;
    protected boolean fullScanSubPartsCrossAllParts;
    protected boolean isScanFirstPartOnly;
    protected boolean enableRangeMerge;

    public BuildStepOpParams() {
    }

    public PartPruneStepBuildingContext getCurrFullContext() {
        return currFullContext;
    }

    public void setCurrFullContext(PartPruneStepBuildingContext currFullContext) {
        this.currFullContext = currFullContext;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public PartPredPathInfo getPartPredPathInfo() {
        return partPredPathInfo;
    }

    public void setPartPredPathInfo(PartPredPathInfo partPredPathInfo) {
        this.partPredPathInfo = partPredPathInfo;
    }

    public PartRouteFunction getPredRouteFunc() {
        return predRouteFunc;
    }

    public void setPredRouteFunc(PartRouteFunction predRouteFunc) {
        this.predRouteFunc = predRouteFunc;
    }

    public PartKeyLevel getPartKeyMatchLevel() {
        return partKeyMatchLevel;
    }

    public void setPartKeyMatchLevel(PartKeyLevel partKeyMatchLevel) {
        this.partKeyMatchLevel = partKeyMatchLevel;
    }

    public boolean isConflict() {
        return isConflict;
    }

    public void setConflict(boolean conflict) {
        isConflict = conflict;
    }

    public boolean isForceFullScan() {
        return forceFullScan;
    }

    public void setForceFullScan(boolean forceFullScan) {
        this.forceFullScan = forceFullScan;
    }

    public boolean isFullScanSubPartsCrossAllParts() {
        return fullScanSubPartsCrossAllParts;
    }

    public void setFullScanSubPartsCrossAllParts(boolean fullScanSubPartsCrossAllParts) {
        this.fullScanSubPartsCrossAllParts = fullScanSubPartsCrossAllParts;
    }

    public boolean isScanFirstPartOnly() {
        return isScanFirstPartOnly;
    }

    public void setScanFirstPartOnly(boolean scanFirstPartOnly) {
        isScanFirstPartOnly = scanFirstPartOnly;
    }

    public boolean isEnableRangeMerge() {
        return enableRangeMerge;
    }

    public void setEnableRangeMerge(boolean enableRangeMerge) {
        this.enableRangeMerge = enableRangeMerge;
    }
}
