package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.Pair;

import java.util.List;
import java.util.concurrent.Future;

public interface TtlWorkerTaskSubmitter {
    List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks();
}
