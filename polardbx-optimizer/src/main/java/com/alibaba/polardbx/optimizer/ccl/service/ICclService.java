package com.alibaba.polardbx.optimizer.ccl.service;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;

import java.util.List;

/**
 * @author busu
 * date: 2020/11/1 10:20 上午
 */
public interface ICclService {

    String CCL_WAIT_TIMEOUT_MESSAGE_FORMAT =
        "Exceeding the max concurrency %d per node of ccl rule %s after waiting for %d ms";

    boolean begin(final ExecutionContext executionContext);

    void end(final ExecutionContext executionContext);

    void invalidateCclRule(CclRuleInfo cclRuleInfo);

    void clearCache();

    /**
     * @return [ notMatchConnCacheHitCount, notMatchPlanNoParamCacheHitCount, notMatchPlanParamCacheHitCount, notMatchPlanOnlyPwdHitCount ]
     */
    List<Long> getCacheStats();

}
