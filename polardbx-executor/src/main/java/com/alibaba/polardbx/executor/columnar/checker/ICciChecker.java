package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Collection;

/**
 * @author yaozhili
 */
public interface ICciChecker {
    void check(ExecutionContext baseEc, Runnable recoverChangedConfigs) throws Throwable;

    /**
     * @param reports [OUT] check reports returned
     * @return true if anything is ok (reports may be empty),
     * or false if inconsistency detected (inconsistency details are in reports)
     */
    boolean getCheckReports(Collection<String> reports);
}
