package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.Synchronizer;

import java.util.List;

public interface FragmentRFItem {
    FragmentRFManager.RFType getRFType();

    FragmentRFManager getManager();

    boolean useXXHashInBuild();

    boolean useXXHashInFilter();

    String getBuildColumnName();

    String getProbeColumnName();

    void setBuildSideChannel(int buildSideChannel);

    void setSourceFilterChannel(int sourceFilterChannel);

    void setSourceRefInFile(int sourceRefInFile);

    int getBuildSideChannel();

    int getSourceFilterChannel();

    int getSourceRefInFile();

    /**
     * Register this item to several Synchronizer Objects so that they can share the Runtime Filter Merger.
     *
     * @param ordinal the ordinal of this item in the build side channels.
     * @param buildSideParallelism the total DOP in build side for shared Runtime Filter Merger.
     * @param synchronizerList the Synchronizer Objects that sharing the Runtime Filter Merger.
     */
    void registerBuilder(int ordinal, int buildSideParallelism, Synchronizer... synchronizerList);

    void registerSource(ColumnarScanExec columnarScanExec);

    List<ColumnarScanExec> getRegisteredSource();

    void assignRF(RFBloomFilter[] bloomFilters);

    RFBloomFilter[] getRF();
}
