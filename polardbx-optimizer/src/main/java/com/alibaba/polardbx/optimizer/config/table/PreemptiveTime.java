package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.LongConfigParam;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

public class PreemptiveTime {
    private final long interval;
    private final long init;
    private final TimeUnit timeUnit;

//    private PreemptiveTime(){
//        this.interval = 0;
//        this.init = 0;
//        this.timeUnit = TimeUnit.MILLISECONDS;
//    }

    private PreemptiveTime(long interval, long init){
        this.interval = interval;
        this.init = init;
        this.timeUnit = TimeUnit.MILLISECONDS;
    }
    // WARNING: DON'T CHANGE THIS `PRIVATE` TO `PUBLIC`,
    // AND DON'T ADD ANY NEW CONSTRUCTOR.
    // YOU SHOULD INITIALIZE OBJECT OF PreemptiveTime ALL IN THIS CLASS
    // IF YOU HAVE ANY DOUBT ABOUT THIS CLASS, CONTACT yijin/taojinkun.
    @JSONCreator
    private PreemptiveTime(long interval, long init, TimeUnit timeUnit) {
        this.interval = interval;
        this.init = init;
        this.timeUnit = timeUnit;
    }

    public long getInit() {
        return init;
    }


    public long getInterval() {
        return interval;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public static PreemptiveTime newDefaultPreemptiveTime() {
        return new PreemptiveTime(15000L, 15000L);
    }

    public static void checkIntervalAndTimeUnit(PreemptiveTime preemptiveTime) {
        TimeUnit timeUnit = preemptiveTime.timeUnit;
        Long interval = preemptiveTime.interval;
        Assert.assertTrue(timeUnit != null && interval != null,
            "preemptive time should have timeUnit and interval Value");
        if (timeUnit.equals(TimeUnit.MILLISECONDS)) {
            Assert.assertTrue(interval >= 500,
                "preemptive time should be more than 0.5 seconds");
        } else {
            Assert.assertTrue(1 != 1,
                "preemptive time should have a legal time unit, MILLISECOND");
        }
    }

    public static PreemptiveTime getPreemptiveTimeFromExecutionContext(ExecutionContext executionContext,
                                                                       LongConfigParam initTime,
                                                                       LongConfigParam intervalTime) {
        return getPreemptiveTimeFromExecutionContext(executionContext.getParamManager(), initTime, intervalTime);
    }

    public static PreemptiveTime getPreemptiveTimeFromExecutionContext(ParamManager paramManager,
                                                                       LongConfigParam initTime,
                                                                       LongConfigParam intervalTime) {
        long init = paramManager.getLong(initTime);
        long interval = paramManager.getLong(intervalTime);
        PreemptiveTime preemptiveTime = new PreemptiveTime(init, interval);
        return preemptiveTime;
    }



    public static void checkIntervalAndTimeUnitNotNull(PreemptiveTime preemptiveTime) {
        Preconditions.checkArgument(preemptiveTime != null);
        Preconditions.checkArgument(preemptiveTime.getInit() != 0);
        Preconditions.checkArgument(preemptiveTime.getInterval() != 0);
        Preconditions.checkArgument(preemptiveTime.getTimeUnit() != null);
    }



}
