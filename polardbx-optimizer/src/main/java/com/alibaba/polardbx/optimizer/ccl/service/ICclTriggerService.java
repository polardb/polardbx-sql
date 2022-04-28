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

package com.alibaba.polardbx.optimizer.ccl.service;

import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;

/**
 * @author busu
 * date: 2021/4/6 10:32 上午
 */
public interface ICclTriggerService {

    /**
     * 提供SQL运行指标的样本
     *
     * @param cclSqlMetric SQL查询运行时指标
     * @return 是否提供成功
     */
    boolean offerSample(CclSqlMetric cclSqlMetric, boolean hasBound);

    /**
     * 处理收集到的样本
     */
    void processSamples();

    /**
     * 清空样本，可以被垃圾回收
     */
    void clearSamples();

    /**
     * @return 是否处于工作状态
     */
    boolean isWorking();

    void startWorking();

}
