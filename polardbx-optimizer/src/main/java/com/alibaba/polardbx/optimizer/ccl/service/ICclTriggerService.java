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
