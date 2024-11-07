package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import java.util.concurrent.Callable;

/**
 * The common base of intra task of ttl
 */
public abstract class TtlIntraTaskRunner implements Callable<Object> {

    public TtlIntraTaskRunner() {
    }

    public abstract void runTask();

    public abstract String getDnId();

    public abstract void notifyStopTask();

    public abstract void forceStopTask();

    @Override
    public Object call() throws Exception {
        runTask();
        return true;
    }

}
