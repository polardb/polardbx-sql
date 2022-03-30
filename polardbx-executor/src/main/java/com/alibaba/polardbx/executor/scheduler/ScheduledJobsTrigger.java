package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.scheduler.SchedulerType;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.Optional;

public interface ScheduledJobsTrigger {

    static ScheduledJobsTrigger restoreTrigger(ScheduledJobsRecord record,
                                               ScheduledJobsAccessor scheduledJobsAccessor,
                                               FiredScheduledJobsAccessor firedScheduledJobsScanner){
        if(record==null || StringUtils.isEmpty(record.getScheduleType())){
            return null;
        }
        if(StringUtils.equalsIgnoreCase(record.getScheduleType(), SchedulerType.QUARTZ_CRON.name())){
            return new DefaultQuartzCronTrigger(record, scheduledJobsAccessor, firedScheduledJobsScanner);
        }

        return null;
    }

    /**
     * 获取下次定时任务的调度时间，这个函数是有状态的，因为它的值会根据上一次调度的时间生成
     * @return
     */
    Optional<ZonedDateTime> getNextFireTime();

    /**
     * 调度定时任务，会修改getNextFireTime
     * @return
     */
    boolean fire();

    /**
     * 立刻调度一次定时任务，不影响getNextFireTime
     * @return
     */
    boolean fireOnceNow();

}
