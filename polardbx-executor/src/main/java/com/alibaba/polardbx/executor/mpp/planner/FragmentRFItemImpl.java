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

package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.Synchronizer;
import com.alibaba.polardbx.executor.operator.SynchronizerRFMerger;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FragmentRFItemImpl implements FragmentRFItem {
    private FragmentRFManager manager;

    private final String buildColumnName;
    private final String probeColumnName;

    private final boolean useXXHashInBuild;
    private final boolean useXXHashInFilter;
    private final FragmentRFManager.RFType rfType;

    // The block channel of build side
    private int buildSideChannel;
    private int sourceFilterChannel;
    private int sourceRefInFile;

    private volatile RFBloomFilter[] bloomFilters;
    private volatile Synchronizer[] synchronizerList;
    private List<ColumnarScanExec> sourceList;

    public FragmentRFItemImpl(FragmentRFManager manager, String buildColumnName, String probeColumnName,
                              boolean useXXHashInBuild, boolean useXXHashInFilter,
                              FragmentRFManager.RFType rfType) {
        this.manager = manager;

        this.buildColumnName = buildColumnName;
        this.probeColumnName = probeColumnName;
        this.useXXHashInBuild = useXXHashInBuild;
        this.useXXHashInFilter = useXXHashInFilter;
        this.rfType = rfType;

        this.sourceList = new ArrayList<>();

        this.buildSideChannel = -1;
        this.sourceFilterChannel = -1;
    }

    @Override
    public FragmentRFManager.RFType getRFType() {
        return rfType;
    }

    @Override
    public FragmentRFManager getManager() {
        return manager;
    }

    @Override
    public boolean useXXHashInBuild() {
        return useXXHashInBuild;
    }

    @Override
    public boolean useXXHashInFilter() {
        return useXXHashInFilter;
    }

    @Override
    public String getBuildColumnName() {
        return buildColumnName;
    }

    @Override
    public String getProbeColumnName() {
        return probeColumnName;
    }

    @Override
    public void setBuildSideChannel(int buildSideChannel) {
        this.buildSideChannel = buildSideChannel;
    }

    @Override
    public void setSourceFilterChannel(int sourceFilterChannel) {
        this.sourceFilterChannel = sourceFilterChannel;
    }

    @Override
    public void setSourceRefInFile(int sourceRefInFile) {
        this.sourceRefInFile = sourceRefInFile;
    }

    @Override
    public int getBuildSideChannel() {
        return this.buildSideChannel;
    }

    @Override
    public int getSourceFilterChannel() {
        return this.sourceFilterChannel;
    }

    @Override
    public int getSourceRefInFile() {
        return sourceRefInFile;
    }

    @Override
    public void registerBuilder(int ordinal, int buildSideParallelism, Synchronizer... synchronizerList) {
        SynchronizerRFMerger merger = new SynchronizerRFMerger(
            this.getManager(), this, buildSideParallelism, ordinal);

        for (Synchronizer synchronizer : synchronizerList) {
            synchronizer.putSynchronizerRFMerger(ordinal, merger);
        }

        this.synchronizerList = synchronizerList;
    }

    @Override
    public void registerSource(ColumnarScanExec columnarScanExec) {
        sourceList.add(columnarScanExec);
        columnarScanExec.setFragmentRFManager(this.getManager());
    }

    @Override
    public List<ColumnarScanExec> getRegisteredSource() {
        return sourceList;
    }

    @Override
    public void assignRF(RFBloomFilter[] bloomFilters) {
        Preconditions.checkArgument(this.bloomFilters == null);
        this.bloomFilters = bloomFilters;
    }

    @Override
    public RFBloomFilter[] getRF() {
        return bloomFilters;
    }

    @Override
    public String toString() {
        return "FragmentRFItemImpl{" +
            "  buildColumnName='" + buildColumnName + '\'' +
            ", probeColumnName='" + probeColumnName + '\'' +
            ", useXXHashInBuild=" + useXXHashInBuild +
            ", useXXHashInFilter=" + useXXHashInFilter +
            ", rfType=" + rfType +
            ", buildSideChannel=" + buildSideChannel +
            ", sourceFilterChannel=" + sourceFilterChannel +
            ", sourceRefInFile=" + sourceRefInFile +
            ", synchronizerList=" + Arrays.toString(synchronizerList) +
            ", sourceList=" + sourceList +
            '}';
    }
}
