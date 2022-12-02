/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.ParamReplaceVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author busu
 * date: 2020/10/31 11:37 下午
 */
public class CclUtils {

    /**
     * unit: millisecond
     */
    public static final int EPSILON = 100;
    private static final Random rand = new Random();

    private final static Function<CclRuleInfo<RescheduleTask>, Boolean> rescheduleCallback =
        (CclRuleInfo<RescheduleTask> cclRuleInfo) -> {
            boolean deliverResult = deliverRunningCount(cclRuleInfo);
            if (!deliverResult) {
                CclUtils.exitRunning(cclRuleInfo);
            }
            CclUtils.exitStaying(cclRuleInfo);
            return Boolean.TRUE;
        };

    public static boolean atomicallyIncrementCount(AtomicInteger count, int valueBound) {
        int value = count.get();
        //condition is true when rollback == true
        while (value < valueBound) {
            if (count.compareAndSet(value, value + 1)) {
                return true;
            }
            value = count.get();
        }
        return false;
    }

    public static boolean enterRunning(CclRuleInfo cclRuleInfo) {
        return CclUtils
            .atomicallyIncrementCount(cclRuleInfo.getRunningCount(), cclRuleInfo.getCclRuleRecord().parallelism);
    }

    public static void exitRunning(CclRuleInfo cclRuleInfo) {
        cclRuleInfo.getRunningCount().decrementAndGet();
    }

    public static boolean enterStaying(CclRuleInfo cclRuleInfo) {
        return CclUtils
            .atomicallyIncrementCount(cclRuleInfo.getStayCount(), cclRuleInfo.getMaxStayCount());
    }

    public static void exitStaying(CclRuleInfo cclRuleInfo) {
        cclRuleInfo.getStayCount().decrementAndGet();
    }

    public static <T> boolean deliverRunningCount(CclRuleInfo<T> cclRuleInfo) {
        Queue<T> queue = cclRuleInfo.getWaitQueue();
        T obj = queue.poll();
        boolean wakenUpResult = false;
        while (obj != null) {
            if (cclRuleInfo.isReschedule()) {
                wakenUpResult = wakenUp((RescheduleTask) obj);
            } else {
                wakenUpResult = wakenUp((CclContext) obj);
            }
            if (wakenUpResult) {
                return true;
            }
            obj = queue.poll();
        }
        return false;
    }

    public static boolean wakenUp(CclContext cclContext) {
        if (cclContext != null && cclContext.isValid()) {
            boolean wakeUpResult = cclContext.setReady();
            if (wakeUpResult) {
                LockSupport.unpark(cclContext.getThread());
            }
            return wakeUpResult;
        }
        return false;
    }

    public static boolean wakenUp(RescheduleTask rescheduleTask) {
        boolean wakeUpResult = rescheduleTask.getReschedulable().reschedule(rescheduleCallback);
        return wakeUpResult;
    }

    public static <T> void tryPollWaitQueue(CclRuleInfo<T> cclRuleInfo) {
        Queue<T> queue = cclRuleInfo.getWaitQueue();
        while (true) {
            boolean enterRunning = CclUtils.enterRunning(cclRuleInfo);
            if (enterRunning) {
                T obj = queue.poll();
                boolean wakeUpResult = false;
                if (obj != null) {
                    //waken others
                    if (cclRuleInfo.isReschedule()) {
                        wakeUpResult = CclUtils.wakenUp((RescheduleTask) obj);
                    } else {
                        wakeUpResult = CclUtils.wakenUp((CclContext) obj);
                    }
                }
                if (!wakeUpResult) {
                    CclUtils.exitRunning(cclRuleInfo);
                }
                if (obj != null) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    public static int generateNoise() {
        return rand.nextInt(EPSILON);
    }

    public static Map<Integer, Object> getPosParamValueMap(SqlParameterized sqlParameterized) {
        StringBuilder sb = new StringBuilder();
        String replaceValue = UUID.randomUUID().toString().replace("-", "");
        ParamReplaceVisitor paramReplaceVisitor = new ParamReplaceVisitor(sb, replaceValue);
        sqlParameterized.getStmt().accept(paramReplaceVisitor);
        String replacedSql = sb.toString();
        Map<Integer, Object> resultMap = Maps.newHashMap();
        SqlParameterized replacedSqlParameterized = SqlParameterizeUtils.parameterize(replacedSql, false);
        List<Object> replacedParameters = replacedSqlParameterized.getParameters();
        if (CollectionUtils.isNotEmpty(replacedParameters)) {
            for (int i = 0; i < replacedParameters.size(); ++i) {
                Object paramValue = replacedParameters.get(i);
                if (!StringUtils.equals(replaceValue, paramValue.toString())) {
                    resultMap.put(i + 1, paramValue);
                }
            }
        }
        return resultMap;
    }

}
