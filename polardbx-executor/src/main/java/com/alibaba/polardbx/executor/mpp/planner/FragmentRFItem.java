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
