package com.alibaba.polardbx.common.trx;

/**
 * @author yaozhili
 */
public interface ISyncPointExecutor {
    /**
     * @return true if success
     */
    boolean execute();
}
