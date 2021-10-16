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

package com.alibaba.polardbx.optimizer.core.rel.util;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.util.List;

/**
 * Runtime filter的动态参数信息。
 *
 * @author bairui.lrj
 */
public class RuntimeFilterDynamicParamInfo implements DynamicParamInfo {
    public static final int ARG_IDX_RUNTIME_FILTER_ID = 1;
    public static final int ARG_IDX_BLOOM_FILTER_INFO = 2;

    private final InfoType infoType;
    private final int runtimeFilterId;

    public RuntimeFilterDynamicParamInfo(InfoType infoType, int runtimeFilterId) {
        this.infoType = infoType;
        this.runtimeFilterId = runtimeFilterId;
    }

    public static List<RuntimeFilterDynamicParamInfo> fromRuntimeFilterId(int runtimeFilterId) {
        return Lists.newArrayList(
            new RuntimeFilterDynamicParamInfo(InfoType.BLOOM_FILTER_DATA, runtimeFilterId),
            new RuntimeFilterDynamicParamInfo(InfoType.BLOOM_FILTER_DATA_LENGTH, runtimeFilterId),
            new RuntimeFilterDynamicParamInfo(InfoType.BLOOM_FILTER_NUM_FUNC, runtimeFilterId));
    }

    public InfoType getInfoType() {
        return infoType;
    }

    public int getRuntimeFilterId() {
        return runtimeFilterId;
    }

    public ParameterContext toParameterContext() {
        ParameterMethod method;
        switch (infoType) {
        case BLOOM_FILTER_DATA:
            method = ParameterMethod.setBloomFilterData;
            break;
        case BLOOM_FILTER_DATA_LENGTH:
            method = ParameterMethod.setBloomFilterDataLength;
            break;
        case BLOOM_FILTER_NUM_FUNC:
            method = ParameterMethod.setBloomFilterFuncNum;
            break;
        default:
            throw GeneralUtil.nestedException("Unsupported runtime filter info type: " + infoType);
        }

        return new ParameterContext(method, new Object[] {null, runtimeFilterId, null});
    }

    /**
     * 目前runtime filter是通过bloom filter实现的，因此每个runtime filter下推至mysql的时候都需要传递三个参数:
     * bloom filter的数据, bloom filter 数据长度，bloom filter函数个数
     * 具体定义可以参见mysql中实现的bloomfilter的udf的参数
     */
    public enum InfoType {
        // bloom filter 二进制数据
        BLOOM_FILTER_DATA,
        // bloom filter数据长度
        BLOOM_FILTER_DATA_LENGTH,
        // bloom filter函数个数
        BLOOM_FILTER_NUM_FUNC
    }
}
