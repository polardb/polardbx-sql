package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.io.PrintWriter;
import java.io.StringWriter;

public class OptimizerAlertLoggerStackBaseImpl extends OptimizerAlertLoggerBaseImpl {
    public OptimizerAlertLoggerStackBaseImpl() {
        super();
    }

    @Override
    public boolean logDetail(ExecutionContext ec, Object object) {
        if (lock.tryLock()) {
            try {
                long lastTime = lastAccessTime.get();
                long currentTime = System.currentTimeMillis();
                if (currentTime >= lastTime + DynamicConfig.getInstance().getOptimizerAlertLogInterval()) {
                    lastAccessTime.set(currentTime);
                    if (ec == null) {
                        logger.info(optimizerAlertType.name());
                    } else {
                        String stack = "";
                        if (object instanceof Throwable) {
                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            ((Throwable) object).printStackTrace(pw);
                            stack = sw.toString();
                        }
                        logger.info(String.format("alert_type{ %s }: schema{ %s } trace_id { %s } stack %s",
                            optimizerAlertType.name(),
                            ec.getSchemaName(),
                            ec.getTraceId(),
                            stack));
                    }
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }
}
