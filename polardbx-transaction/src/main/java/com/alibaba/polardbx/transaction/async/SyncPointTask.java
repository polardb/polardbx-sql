package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.trx.ISyncPointExecutor;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class SyncPointTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SyncPointTask.class);

    @Override
    public void run() {
        if (!ExecUtils.hasLeadership(DEFAULT_DB_NAME)) {
            return;
        }

        ISyncPointExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getSyncPointExecutor();
        boolean success = executor.execute();
        if (!success) {
            logger.warn("Trigger sync point trx failed.");
        }
    }
}
