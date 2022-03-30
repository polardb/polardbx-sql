package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlPlanManager;
import com.alibaba.polardbx.executor.partitionmanagement.rebalance.RebalanceDdlPlanManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DdlPlanScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineScheduler.class);

    private static final DdlPlanScheduler INSTANCE = new DdlPlanScheduler();

    public static DdlPlanScheduler getINSTANCE() {
        return INSTANCE;
    }

    /**
     * periodically scan & trigger JOBs
     */
    private final ScheduledThreadPoolExecutor scannerThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("DDL-PLAN-Scanner-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    private DdlPlanScheduler() {
        scannerThread.scheduleWithFixedDelay(
            AsyncTask.build(new DdlPlanScanner()),
            0L,
            1L,
            TimeUnit.MINUTES
        );
    }

    private static class DdlPlanScanner implements Runnable {

        DdlPlanManager ddlPlanManager = new DdlPlanManager();

        @Override
        public void run() {
            try {
                if (!ExecUtils.hasLeadership(null)) {
                    return;
                }
                List<DdlPlanRecord> ddlPlanRecordList = ddlPlanManager.getExecutableDdlPlan(DdlType.REBALANCE);
                if (CollectionUtils.isEmpty(ddlPlanRecordList)) {
                    return;
                }
                synchronized (DdlPlanScheduler.class) {
                    for (DdlPlanRecord record: ddlPlanRecordList){
                        try {
                            RebalanceDdlPlanManager rebalanceDdlPlanManager = new RebalanceDdlPlanManager();
                            rebalanceDdlPlanManager.process(record);
                        }catch (Exception e){
                            LOGGER.error("process ddl plan error, planId:" + record.getPlanId(), e);
                        }
                    }
                }
            } catch (Throwable t) {
                //never throw
                LOGGER.error("DDL PLAN SCHEDULER ERROR", t);
            }
        }
    }

}